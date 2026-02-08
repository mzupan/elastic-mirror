#!/usr/bin/env bash
set -euo pipefail

# ES Version Compatibility Test
# Tests the elastic-mirror plugin against different ES versions.
# For each version: update build files → Docker build → deploy → 2-min load test → compare

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
RESULTS_FILE="${PROJECT_DIR}/test/version-results.txt"
DURATION_SECONDS=120
OPS_PER_SECOND=5

# Versions to test - latest patch of each 8.x minor
VERSIONS=(
  "8.0.1"
  "8.1.3"
  "8.2.3"
  "8.3.3"
  "8.4.3"
  "8.5.3"
  "8.6.2"
  "8.7.1"
  "8.8.2"
  "8.9.2"
  "8.10.4"
  "8.11.4"
  "8.12.2"
  "8.13.4"
  "8.14.3"
  "8.15.5"
  "8.16.6"
  "8.17.10"
  "8.18.8"
  "8.19.11"
)

# Override with args if provided
if [ $# -gt 0 ]; then
  VERSIONS=("$@")
fi

CYAN='\033[0;36m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
pass() { echo -e "${GREEN}[$(date +%H:%M:%S)] ✓${NC} $*"; }
fail() { echo -e "${RED}[$(date +%H:%M:%S)] ✗${NC} $*"; }
warn() { echo -e "${YELLOW}[$(date +%H:%M:%S)] !${NC} $*"; }

update_version() {
  local ver="$1"
  log "Updating build files to ES ${ver}..."

  # build.gradle
  sed -i '' "s/def esVersion = '.*'/def esVersion = '${ver}'/" "${PROJECT_DIR}/build.gradle"

  # plugin-descriptor.properties
  sed -i '' "s/elasticsearch.version=.*/elasticsearch.version=${ver}/" "${PROJECT_DIR}/src/main/resources/plugin-descriptor.properties"

  # Dockerfile base image
  sed -i '' "s|elasticsearch:7\.17\.[0-9]*|elasticsearch:${ver}|g; s|elasticsearch:8\.[0-9]*\.[0-9]*|elasticsearch:${ver}|g" "${PROJECT_DIR}/Dockerfile"

  # Dockerfile COPY line for zip (non-greedy: [^ ]* to avoid eating /tmp/plugin.zip)
  local plugin_version
  plugin_version=$(grep "^version = " "${PROJECT_DIR}/build.gradle" | sed "s/version = '//;s/'//")
  sed -i '' "s|elastic-mirror-[^ ]*\.zip|elastic-mirror-${plugin_version}.zip|g" "${PROJECT_DIR}/Dockerfile"
}

build_image() {
  local ver="$1"
  log "Building Docker image for ES ${ver}..."

  if docker build -t es-replication-plugin:latest "${PROJECT_DIR}" 2>&1; then
    pass "Docker build succeeded for ES ${ver}"
    return 0
  else
    fail "Docker build FAILED for ES ${ver}"
    return 1
  fi
}

deploy_and_wait() {
  local ver="$1"
  log "Deploying ES ${ver} pods..."

  # Kill existing port forwards
  pkill -f "kubectl port-forward.*es-active" 2>/dev/null || true
  pkill -f "kubectl port-forward.*es-passive" 2>/dev/null || true
  sleep 1

  # Delete PVCs to get clean data
  kubectl delete pvc -n es-replication data-es-active-0 data-es-passive-0 --ignore-not-found --wait=false 2>/dev/null || true

  # Restart pods (force pull new image)
  kubectl rollout restart statefulset es-active -n es-replication 2>/dev/null
  kubectl rollout restart statefulset es-passive -n es-replication 2>/dev/null

  # Wait for rollout to complete (old pods terminate, new pods start)
  log "Waiting for rollout to complete..."
  kubectl rollout status statefulset es-active -n es-replication --timeout=180s 2>/dev/null || true
  kubectl rollout status statefulset es-passive -n es-replication --timeout=180s 2>/dev/null || true

  # Wait for pods to be ready
  log "Waiting for pods to be ready..."
  local waited=0
  local max_wait=180
  while [ $waited -lt $max_wait ]; do
    local active_ready passive_ready
    active_ready=$(kubectl get pod es-active-0 -n es-replication -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false")
    passive_ready=$(kubectl get pod es-passive-0 -n es-replication -o jsonpath='{.status.containerStatuses[0].ready}' 2>/dev/null || echo "false")

    if [ "$active_ready" = "true" ] && [ "$passive_ready" = "true" ]; then
      pass "Both pods ready"
      break
    fi

    # Check for CrashLoopBackOff
    local active_status passive_status
    active_status=$(kubectl get pod es-active-0 -n es-replication -o jsonpath='{.status.containerStatuses[0].state}' 2>/dev/null || echo "")
    if echo "$active_status" | grep -q "CrashLoopBackOff\|Error\|waiting"; then
      local reason
      reason=$(kubectl get pod es-active-0 -n es-replication -o jsonpath='{.status.containerStatuses[0].state.waiting.reason}' 2>/dev/null || echo "unknown")
      if [ "$reason" = "CrashLoopBackOff" ] || [ "$reason" = "Error" ]; then
        fail "Pod crashed for ES ${ver} (${reason})"
        kubectl logs es-active-0 -n es-replication --tail=20 2>/dev/null || true
        return 1
      fi
    fi

    sleep 5
    waited=$((waited + 5))
  done

  if [ $waited -ge $max_wait ]; then
    fail "Pods did not become ready in ${max_wait}s for ES ${ver}"
    return 1
  fi

  # Start port forwards
  kubectl port-forward -n es-replication svc/es-active 9200:9200 &>/dev/null &
  kubectl port-forward -n es-replication svc/es-passive 9201:9200 &>/dev/null &
  sleep 3

  # Verify connectivity
  if ! curl -s -o /dev/null -w '%{http_code}' http://localhost:9200 | grep -q 200; then
    fail "Cannot reach active cluster for ES ${ver}"
    return 1
  fi
  if ! curl -s -o /dev/null -w '%{http_code}' http://localhost:9201 | grep -q 200; then
    fail "Cannot reach passive cluster for ES ${ver}"
    return 1
  fi

  # Verify plugin is loaded
  local plugins
  plugins=$(curl -s http://localhost:9200/_cat/plugins 2>/dev/null)
  if echo "$plugins" | grep -q "elastic-mirror"; then
    pass "Plugin loaded on ES ${ver}"
  else
    fail "Plugin NOT loaded on ES ${ver}"
    log "Installed plugins: ${plugins}"
    return 1
  fi

  pass "ES ${ver} deployed and plugin loaded"
  return 0
}

run_test() {
  local ver="$1"
  log "Running ${DURATION_SECONDS}s load test on ES ${ver}..."

  # Delete checkpoint index
  curl -s -X DELETE "http://localhost:9201/.replication_checkpoint" > /dev/null 2>&1 || true

  # Purge SQS
  local queue_url="https://sqs.us-west-2.amazonaws.com/637423241855/es-replication"
  aws sqs purge-queue --queue-url "$queue_url" --region us-west-2 2>/dev/null || true
  sleep 2

  # Start replication
  local start_producer start_consumer
  start_producer=$(curl -s -X POST http://localhost:9200/_replication/start)
  start_consumer=$(curl -s -X POST http://localhost:9201/_replication/start)

  if ! echo "$start_producer" | grep -q '"acknowledged":true'; then
    fail "Failed to start producer on ES ${ver}: ${start_producer}"
    return 1
  fi
  if ! echo "$start_consumer" | grep -q '"acknowledged":true'; then
    fail "Failed to start consumer on ES ${ver}: ${start_consumer}"
    return 1
  fi

  # Run load test
  ACTIVE_URL=http://localhost:9200 \
  PASSIVE_URL=http://localhost:9201 \
  DURATION_SECONDS=$DURATION_SECONDS \
  OPS_PER_SECOND=$OPS_PER_SECOND \
  bash "${SCRIPT_DIR}/load-generator.sh" 2>&1

  return $?
}

check_sync() {
  local ver="$1"
  log "Checking sync for ES ${ver}..."

  sleep 15  # extra settle time

  local total_active=0
  local total_passive=0
  local all_match=true

  for i in $(seq -w 1 10); do
    local idx="test-index-${i}"
    local active_count passive_count
    active_count=$(curl -s "http://localhost:9200/${idx}/_count" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo 0)
    passive_count=$(curl -s "http://localhost:9201/${idx}/_count" 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('count',0))" 2>/dev/null || echo 0)

    total_active=$((total_active + active_count))
    total_passive=$((total_passive + passive_count))

    if [ "$active_count" != "$passive_count" ]; then
      all_match=false
    fi
  done

  if [ "$all_match" = true ] && [ "$total_active" -gt 0 ]; then
    pass "ES ${ver}: IN SYNC (${total_active} docs)"
    return 0
  else
    local diff=$((total_active - total_passive))
    fail "ES ${ver}: MISMATCH (active=${total_active} passive=${total_passive} diff=${diff})"
    return 1
  fi
}

cleanup_test() {
  # Stop replication
  curl -s -X POST http://localhost:9200/_replication/stop > /dev/null 2>&1 || true
  curl -s -X POST http://localhost:9201/_replication/stop > /dev/null 2>&1 || true

  # Delete test indices
  curl -s -X DELETE "http://localhost:9200/test-index-*" > /dev/null 2>&1 || true
  curl -s -X DELETE "http://localhost:9201/test-index-*" > /dev/null 2>&1 || true
  curl -s -X DELETE "http://localhost:9201/.replication_checkpoint" > /dev/null 2>&1 || true

  # Purge SQS
  aws sqs purge-queue --queue-url "https://sqs.us-west-2.amazonaws.com/637423241855/es-replication" --region us-west-2 2>/dev/null || true
}

# Main
echo "============================================"
echo "  Elastic Mirror Version Compatibility Test"
echo "============================================"
echo "Versions: ${VERSIONS[*]}"
echo "Test duration: ${DURATION_SECONDS}s per version"
echo ""

> "$RESULTS_FILE"

for ver in "${VERSIONS[@]}"; do
  echo ""
  log "========== Testing ES ${ver} =========="

  # Update version in build files
  update_version "$ver"

  # Build
  build_output=$(mktemp)
  if ! build_image "$ver" > "$build_output" 2>&1; then
    cat "$build_output" | tail -30
    echo "${ver} BUILD_FAIL" >> "$RESULTS_FILE"
    rm -f "$build_output"
    continue
  fi
  rm -f "$build_output"

  # Deploy
  if ! deploy_and_wait "$ver"; then
    echo "${ver} DEPLOY_FAIL" >> "$RESULTS_FILE"
    continue
  fi

  # Test
  if ! run_test "$ver"; then
    echo "${ver} TEST_FAIL" >> "$RESULTS_FILE"
    cleanup_test
    continue
  fi

  # Check sync
  if check_sync "$ver"; then
    echo "${ver} PASS" >> "$RESULTS_FILE"
  else
    echo "${ver} SYNC_FAIL" >> "$RESULTS_FILE"
  fi

  cleanup_test
done

# Restore original version
update_version "8.14.3"

echo ""
echo "============================================"
echo "  RESULTS"
echo "============================================"
cat "$RESULTS_FILE"
echo ""
