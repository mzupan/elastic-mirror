# Elastic Mirror

### Cross-Cluster Replication for Elasticsearch 7.17.x

An open-source plugin that provides active/passive cross-cluster replication for Elasticsearch 7.17.x without requiring a Platinum license. Uses S3 and SQS as the transport layer.

---

## Key Features

- Change data capture via IndexingOperationListener (captures all writes including updates and deletes)
- Batched, compressed transport over S3 with SQS notifications
- External versioning for idempotent replay -- safe to process duplicate or out-of-order messages
- Multi-threaded consumer with configurable worker pool
- Per-shard checkpoint tracking for replay progress
- Configurable index include/exclude patterns
- Support for MinIO and ElasticMQ as on-prem alternatives to AWS
- Manual failover with role reversal

## Architecture

```
ES Cluster A (Active)  -->  S3 + SQS  -->  ES Cluster B (Passive)
     producer                                    consumer
```

**Producer side:** An `IndexingOperationListener` captures every index, update, and delete operation. Events are batched in memory by count, size, or time window, compressed, and uploaded to S3 as NDJSON files. An SQS message is sent for each batch with the S3 key and sequence metadata.

**Consumer side:** A background poller reads SQS messages and dispatches them to a worker pool. Each worker downloads the batch from S3, parses the events, and replays them through the Bulk API using external versioning. Checkpoints are tracked per-shard in a `.replication_checkpoint` index.

## Compatibility

Tested against the full Elasticsearch 7.17.x release line (7.17.0 through 7.17.28). The plugin compiles and runs correctly on every 7.17.x patch version. Runtime load tests with multi-index mixed workloads (creates, updates, deletes) have confirmed perfect cluster sync on 7.17.0, 7.17.25, and 7.17.28.

Elasticsearch 8.x is not supported. The 7.x to 8.x upgrade introduced breaking changes to the plugin API that require a significant rewrite.

## Requirements

- Elasticsearch 7.17.x (any patch version)
- Java 11
- AWS S3 bucket (or MinIO)
- AWS SQS queue (or ElasticMQ)
- AWS credentials with appropriate S3 and SQS permissions

## Getting Started

### Build

```bash
./gradlew build
```

The plugin zip will be at `build/distributions/elastic-mirror-1.0.0.zip`.

### Install

```bash
# On both clusters
bin/elasticsearch-plugin install file:///path/to/elastic-mirror-1.0.0.zip
```

### Docker

A multi-stage Dockerfile is included that builds the plugin and installs it into the official Elasticsearch image:

```bash
docker build -t es-replication:latest .
```

### Elastic Cloud on Kubernetes (ECK)

If you are running ECK and do not want to maintain a custom Docker image, you can use an init container to install the plugin at pod startup. Host the plugin zip on S3 or an internal HTTP server and add an init container to your Elasticsearch resource spec:

```yaml
podTemplate:
  spec:
    initContainers:
      - name: install-replication-plugin
        command:
          - sh
          - -c
          - bin/elasticsearch-plugin install --batch https://your-host/elastic-mirror-1.0.0.zip
```

## Configuration

Add to `elasticsearch.yml` on each cluster.

### Active Cluster (Producer)

```yaml
replication.role: producer

replication.transport.s3.bucket: my-replication-bucket
replication.transport.s3.region: us-east-1
replication.transport.s3.base_path: replication/

replication.transport.sqs.queue_url: https://sqs.us-east-1.amazonaws.com/123456789/es-replication
replication.transport.sqs.region: us-east-1

# Batching (optional, these are defaults)
replication.batch.size: 1000
replication.batch.age_ms: 5000
replication.batch.max_bytes: 5242880

# Index filtering (optional)
replication.indices.include: ""
replication.indices.exclude: ""

# Compression (optional, default: true)
replication.transport.compress: true
```

### Passive Cluster (Consumer)

```yaml
replication.role: consumer

replication.transport.s3.bucket: my-replication-bucket
replication.transport.s3.region: us-east-1
replication.transport.s3.base_path: replication/

replication.transport.sqs.queue_url: https://sqs.us-east-1.amazonaws.com/123456789/es-replication
replication.transport.sqs.region: us-east-1

# Replay settings (optional)
replication.replay.bulk_size: 500
replication.replay.poll_batch: 10
replication.replay.poll_wait_seconds: 10
replication.replay.worker_threads: 4
```

### On-Prem (MinIO + ElasticMQ)

```yaml
replication.transport.s3.endpoint: http://minio.internal:9000
replication.transport.s3.path_style_access: true
replication.transport.sqs.endpoint: http://elasticmq.internal:9324
```

### AWS Credentials

The plugin uses the AWS SDK default credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. System properties
3. IAM instance profile or ECS task role (recommended for AWS deployments)

For explicit credentials, add to the Elasticsearch keystore:

```bash
bin/elasticsearch-keystore add replication.transport.aws.access_key
bin/elasticsearch-keystore add replication.transport.aws.secret_key
```

## Usage

### Start Replication

```bash
# On the active cluster
curl -X POST http://active-cluster:9200/_replication/start

# On the passive cluster
curl -X POST http://passive-cluster:9200/_replication/start
```

### Check Status

```bash
curl http://active-cluster:9200/_replication/status
curl http://passive-cluster:9200/_replication/status
```

### Stop Replication

```bash
curl -X POST http://active-cluster:9200/_replication/stop
curl -X POST http://passive-cluster:9200/_replication/stop
```

## What Gets Replicated

- New document inserts
- Full document updates (partial `_update` operations are captured post-merge)
- Document deletes
- All indices by default (configurable include/exclude patterns)

Index settings, mappings, templates, cluster settings, ILM/SLM policies, and ingest pipelines are not replicated. Create these on both clusters manually.

## Failover

When the active cluster goes down:

1. Stop the consumer on the passive cluster
2. Verify data is current by checking the last checkpoint
3. Point application traffic to the passive cluster

To fail back when the original active recovers, swap the roles in `elasticsearch.yml`, restart both nodes, and start replication in the reverse direction. If writes happened on the promoted passive during the outage, a fresh snapshot/restore from the promoted passive to the original active is the safest approach.

## Initial Sync

For clusters with existing data, use snapshot/restore before starting CDC replication:

```bash
# On the active cluster
curl -X PUT "http://active:9200/_snapshot/my_backup/initial" \
  -H 'Content-Type: application/json' -d '{
    "indices": "*,-.*",
    "ignore_unavailable": true
  }'

# On the passive cluster
curl -X POST "http://passive:9200/_snapshot/my_backup/initial/_restore" \
  -H 'Content-Type: application/json' -d '{
    "indices": "*,-.*"
  }'
```

Then start CDC replication for ongoing changes.

## S3 Key Format

```
{base_path}/{index}/{shard_id}/{primary_term}_{from_seq}_{to_seq}.ndjson.gz
```

## Limitations

- Eventual consistency -- there will be replication lag, typically seconds
- No mapping sync -- index mappings and settings must be created on both clusters
- No conflict resolution -- active/passive only, writes should go to one cluster
- No translog access -- uses IndexingOperationListener, not translog replay
- Single direction -- one producer, one consumer per S3/SQS pair

## About

This project was built with [Claude Code](https://claude.ai) by someone with a strong background in operations and infrastructure work. The plugin has been through extensive failure scenario testing, including SQS message ordering edge cases, parallel consumer race conditions, checkpoint consistency under load, and prolonged multi-index workloads. The design decisions here -- particularly around always-replay with external versioning instead of checkpoint-based skipping -- came directly from observing real data loss scenarios during testing and fixing them.

## License

Apache 2.0
