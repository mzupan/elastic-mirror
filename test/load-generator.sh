#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# ES Replication Load Generator
# Generates creates, updates, and deletes across 10 indices
# for a configurable duration, then compares both clusters.
# ============================================================

ACTIVE_URL="${ACTIVE_URL:-http://localhost:9200}"
PASSIVE_URL="${PASSIVE_URL:-http://localhost:9201}"
DURATION_SECONDS="${DURATION_SECONDS:-3600}"  # default 1 hour
OPS_PER_SECOND="${OPS_PER_SECOND:-5}"
NUM_INDICES=10
DOCS_PER_INDEX=100  # initial seed docs per index

# Index names
INDICES=()
for i in $(seq 1 $NUM_INDICES); do
    INDICES+=("test-index-$(printf '%02d' $i)")
done

# Counters
TOTAL_CREATES=0
TOTAL_UPDATES=0
TOTAL_DELETES=0
TOTAL_ERRORS=0
START_TIME=$(date +%s)

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

log() { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
ok()  { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓${NC} $*"; }
err() { echo -e "${RED}[$(date '+%H:%M:%S')] ✗${NC} $*"; }
warn(){ echo -e "${YELLOW}[$(date '+%H:%M:%S')] !${NC} $*"; }

# ---- Phase 1: Check connectivity ----
check_cluster() {
    local url=$1 name=$2
    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" "$url/_cluster/health" 2>/dev/null || echo "000")
    if [ "$status" = "200" ]; then
        ok "$name cluster reachable at $url"
        return 0
    else
        err "$name cluster NOT reachable at $url (HTTP $status)"
        return 1
    fi
}

# ---- Phase 2: Create index mappings on both clusters ----
create_indices() {
    local url=$1 name=$2
    log "Creating ${#INDICES[@]} indices on $name..."
    for idx in "${INDICES[@]}"; do
        local resp
        resp=$(curl -s -X PUT "$url/$idx" -H 'Content-Type: application/json' -d '{
            "settings": { "number_of_shards": 1, "number_of_replicas": 0 },
            "mappings": {
                "properties": {
                    "message": { "type": "text" },
                    "counter": { "type": "integer" },
                    "status": { "type": "keyword" },
                    "tags": { "type": "keyword" },
                    "created_at": { "type": "date" },
                    "updated_at": { "type": "date" }
                }
            }
        }' 2>/dev/null)
        if echo "$resp" | grep -q '"acknowledged":true'; then
            : # silent success
        elif echo "$resp" | grep -q 'resource_already_exists'; then
            : # already exists, fine
        else
            warn "Index $idx on $name: $resp"
        fi
    done
    ok "Indices ready on $name"
}

# ---- Phase 3: Seed initial documents ----
seed_docs() {
    log "Seeding $DOCS_PER_INDEX docs per index on active cluster..."
    for idx in "${INDICES[@]}"; do
        # Use bulk API for speed
        local bulk_body=""
        for doc_id in $(seq 1 $DOCS_PER_INDEX); do
            bulk_body+='{"index":{"_index":"'"$idx"'","_id":"doc-'"$doc_id"'"}}'$'\n'
            bulk_body+='{"message":"Initial seed document '"$doc_id"'","counter":0,"status":"active","tags":["seed","test"],"created_at":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'","updated_at":"'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"}'$'\n'
        done
        local resp
        resp=$(curl -s -X POST "$ACTIVE_URL/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary "$bulk_body" 2>/dev/null)
        local errs
        errs=$(echo "$resp" | grep -o '"errors":true' || true)
        if [ -n "$errs" ]; then
            warn "Bulk seed for $idx had errors"
        fi
        TOTAL_CREATES=$((TOTAL_CREATES + DOCS_PER_INDEX))
    done
    ok "Seeded $((NUM_INDICES * DOCS_PER_INDEX)) documents across $NUM_INDICES indices"
}

# ---- Phase 4: Generate mixed workload ----
random_index() {
    echo "${INDICES[$((RANDOM % NUM_INDICES))]}"
}

random_doc_id() {
    # doc IDs from 1 to DOCS_PER_INDEX * 2 (some will be new, some existing)
    echo "doc-$((RANDOM % (DOCS_PER_INDEX * 2) + 1))"
}

random_word() {
    local words=("alpha" "bravo" "charlie" "delta" "echo" "foxtrot" "golf" "hotel"
                 "india" "juliet" "kilo" "lima" "mike" "november" "oscar" "papa"
                 "quebec" "romeo" "sierra" "tango" "uniform" "victor" "whiskey" "xray"
                 "yankee" "zulu" "search" "index" "cluster" "shard" "replica" "node")
    echo "${words[$((RANDOM % ${#words[@]}))]}"
}

do_create() {
    local idx=$(random_index)
    local doc_id="doc-$((DOCS_PER_INDEX + RANDOM % 10000 + 1))"
    local word1=$(random_word) word2=$(random_word)
    local resp
    resp=$(curl -s -X PUT "$ACTIVE_URL/$idx/_doc/$doc_id" -H 'Content-Type: application/json' -d '{
        "message": "'"$word1 $word2 generated document"'",
        "counter": 1,
        "status": "active",
        "tags": ["'"$word1"'", "generated"],
        "created_at": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'",
        "updated_at": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"
    }' 2>/dev/null)
    if echo "$resp" | grep -q '"result"'; then
        TOTAL_CREATES=$((TOTAL_CREATES + 1))
    else
        TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
    fi
}

do_update() {
    local idx=$(random_index)
    local doc_id=$(random_doc_id)
    local word=$(random_word)
    local counter=$((RANDOM % 1000))
    local resp
    resp=$(curl -s -X POST "$ACTIVE_URL/$idx/_update/$doc_id" -H 'Content-Type: application/json' -d '{
        "doc": {
            "message": "Updated: '"$word"' at '"$(date -u +%H:%M:%S)"'",
            "counter": '"$counter"',
            "status": "updated",
            "updated_at": "'"$(date -u +%Y-%m-%dT%H:%M:%SZ)"'"
        },
        "doc_as_upsert": true
    }' 2>/dev/null)
    if echo "$resp" | grep -q '"result"'; then
        TOTAL_UPDATES=$((TOTAL_UPDATES + 1))
    else
        TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
    fi
}

do_delete() {
    local idx=$(random_index)
    local doc_id=$(random_doc_id)
    local resp
    resp=$(curl -s -X DELETE "$ACTIVE_URL/$idx/_doc/$doc_id" 2>/dev/null)
    if echo "$resp" | grep -q '"result"'; then
        TOTAL_DELETES=$((TOTAL_DELETES + 1))
    else
        TOTAL_ERRORS=$((TOTAL_ERRORS + 1))
    fi
}

run_workload() {
    local end_time=$((START_TIME + DURATION_SECONDS))
    local ops=0
    local last_report=$START_TIME

    log "Starting mixed workload for $((DURATION_SECONDS / 60)) minutes..."
    log "Target: ~$OPS_PER_SECOND ops/sec (60% create, 30% update, 10% delete)"
    echo ""

    while [ "$(date +%s)" -lt "$end_time" ]; do
        # Pick operation: 60% create, 30% update, 10% delete
        local roll=$((RANDOM % 100))
        if [ $roll -lt 60 ]; then
            do_create
        elif [ $roll -lt 90 ]; then
            do_update
        else
            do_delete
        fi

        ops=$((ops + 1))

        # Rate limiting
        if [ $((ops % OPS_PER_SECOND)) -eq 0 ]; then
            sleep 1
        fi

        # Progress report every 60 seconds
        local now=$(date +%s)
        if [ $((now - last_report)) -ge 60 ]; then
            local elapsed=$((now - START_TIME))
            local remaining=$((end_time - now))
            log "Progress: ${elapsed}s elapsed, ${remaining}s remaining | " \
                "creates=$TOTAL_CREATES updates=$TOTAL_UPDATES deletes=$TOTAL_DELETES errors=$TOTAL_ERRORS"
            last_report=$now
        fi
    done

    echo ""
    ok "Workload complete!"
    log "Final: creates=$TOTAL_CREATES updates=$TOTAL_UPDATES deletes=$TOTAL_DELETES errors=$TOTAL_ERRORS"
}

# ---- Phase 5: Compare clusters ----
compare_clusters() {
    echo ""
    log "============================================"
    log "  CLUSTER COMPARISON"
    log "============================================"
    echo ""

    # Wait for replication to catch up
    log "Waiting 30 seconds for replication to catch up..."
    sleep 30

    # Refresh both clusters
    curl -s -X POST "$ACTIVE_URL/_refresh" >/dev/null 2>&1
    curl -s -X POST "$PASSIVE_URL/_refresh" >/dev/null 2>&1
    sleep 2

    local all_match=true
    local total_active=0
    local total_passive=0

    printf "${CYAN}%-20s %10s %10s %10s${NC}\n" "INDEX" "ACTIVE" "PASSIVE" "STATUS"
    printf "%-20s %10s %10s %10s\n" "--------------------" "----------" "----------" "----------"

    for idx in "${INDICES[@]}"; do
        local active_count passive_count
        active_count=$(curl -s "$ACTIVE_URL/$idx/_count" 2>/dev/null | grep -o '"count":[0-9]*' | grep -o '[0-9]*' || echo "0")
        passive_count=$(curl -s "$PASSIVE_URL/$idx/_count" 2>/dev/null | grep -o '"count":[0-9]*' | grep -o '[0-9]*' || echo "0")

        total_active=$((total_active + active_count))
        total_passive=$((total_passive + passive_count))

        if [ "$active_count" = "$passive_count" ]; then
            printf "%-20s %10s %10s ${GREEN}%10s${NC}\n" "$idx" "$active_count" "$passive_count" "MATCH"
        else
            printf "%-20s %10s %10s ${RED}%10s${NC}\n" "$idx" "$active_count" "$passive_count" "MISMATCH"
            all_match=false
        fi
    done

    printf "%-20s %10s %10s %10s\n" "--------------------" "----------" "----------" "----------"
    if [ "$total_active" = "$total_passive" ]; then
        printf "%-20s %10s %10s ${GREEN}%10s${NC}\n" "TOTAL" "$total_active" "$total_passive" "MATCH"
    else
        printf "%-20s %10s %10s ${RED}%10s${NC}\n" "TOTAL" "$total_active" "$total_passive" "MISMATCH"
        all_match=false
    fi

    echo ""

    # Spot-check: compare 10 random documents
    log "Spot-checking 10 random documents..."
    local spot_match=0
    local spot_total=0
    for i in $(seq 1 10); do
        local idx=$(random_index)
        local doc_id=$(random_doc_id)
        local active_doc passive_doc
        active_doc=$(curl -s "$ACTIVE_URL/$idx/_doc/$doc_id" 2>/dev/null)
        passive_doc=$(curl -s "$PASSIVE_URL/$idx/_doc/$doc_id" 2>/dev/null)

        local active_found=$(echo "$active_doc" | grep -o '"found":true' || echo "")
        local passive_found=$(echo "$passive_doc" | grep -o '"found":true' || echo "")

        spot_total=$((spot_total + 1))
        if [ "$active_found" = "$passive_found" ]; then
            spot_match=$((spot_match + 1))
            # If both found, compare source
            if [ -n "$active_found" ]; then
                local active_src=$(echo "$active_doc" | grep -o '"_source":{[^}]*}' || echo "")
                local passive_src=$(echo "$passive_doc" | grep -o '"_source":{[^}]*}' || echo "")
                if [ "$active_src" != "$passive_src" ]; then
                    warn "Content mismatch: $idx/$doc_id"
                    spot_match=$((spot_match - 1))
                fi
            fi
        else
            warn "Existence mismatch: $idx/$doc_id (active=$active_found passive=$passive_found)"
        fi
    done

    echo ""
    if [ $spot_match -eq $spot_total ]; then
        ok "Spot check: $spot_match/$spot_total documents match"
    else
        warn "Spot check: $spot_match/$spot_total documents match"
    fi

    echo ""
    log "============================================"
    if $all_match && [ $spot_match -eq $spot_total ]; then
        ok "RESULT: CLUSTERS ARE IN SYNC"
    else
        err "RESULT: CLUSTERS HAVE DIFFERENCES"
        warn "This may be due to replication lag. Try waiting longer and re-running comparison."
    fi
    log "============================================"

    # Replication status from both clusters
    echo ""
    log "Active cluster replication status:"
    curl -s "$ACTIVE_URL/_replication/status" 2>/dev/null | python3 -m json.tool 2>/dev/null || curl -s "$ACTIVE_URL/_replication/status"
    echo ""
    log "Passive cluster replication status:"
    curl -s "$PASSIVE_URL/_replication/status" 2>/dev/null | python3 -m json.tool 2>/dev/null || curl -s "$PASSIVE_URL/_replication/status"
}

# ---- Main ----
main() {
    echo ""
    log "============================================"
    log "  ES Replication Load Test"
    log "============================================"
    log "Active:  $ACTIVE_URL"
    log "Passive: $PASSIVE_URL"
    log "Duration: $((DURATION_SECONDS / 60)) minutes"
    log "Indices: $NUM_INDICES"
    log "Ops/sec: ~$OPS_PER_SECOND"
    echo ""

    # Check connectivity
    check_cluster "$ACTIVE_URL" "Active" || exit 1
    check_cluster "$PASSIVE_URL" "Passive" || exit 1

    # Create indices on both clusters (mappings need to exist on passive too)
    create_indices "$ACTIVE_URL" "Active"
    create_indices "$PASSIVE_URL" "Passive"

    # Seed initial data
    seed_docs

    # Run workload
    run_workload

    # Compare
    compare_clusters
}

main "$@"
