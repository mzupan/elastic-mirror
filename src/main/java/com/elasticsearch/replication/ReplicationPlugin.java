package com.elasticsearch.replication;

import com.elasticsearch.replication.api.ReplicationStatusAction;
import com.elasticsearch.replication.api.StartReplicationAction;
import com.elasticsearch.replication.api.StopReplicationAction;
import com.elasticsearch.replication.cdc.ChangeCapture;
import com.elasticsearch.replication.cdc.DeleteRoutingCapture;
import com.elasticsearch.replication.cdc.OperationBatcher;
import com.elasticsearch.replication.replay.BulkReplayer;
import com.elasticsearch.replication.replay.CheckpointManager;
import com.elasticsearch.replication.replay.ReplayService;
import com.elasticsearch.replication.settings.ReplicationSettings;
import com.elasticsearch.replication.transport.BatchShipper;
import com.elasticsearch.replication.transport.S3Transport;
import com.elasticsearch.replication.transport.SQSNotifier;
import com.elasticsearch.replication.transport.TransportConfig;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.apache.http.HttpHost;

import org.elasticsearch.action.support.ActionFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.elasticsearch.features.NodeFeature;

/**
 * Main entry point for the Elasticsearch replication plugin.
 *
 * Supports two roles:
 * - "producer": Captures index/delete operations and ships them to S3/SQS
 * - "consumer": Polls SQS, downloads from S3, replays to local cluster
 *
 * Configuration is done via elasticsearch.yml or cluster settings API.
 */
public class ReplicationPlugin extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(ReplicationPlugin.class);

    private final Settings settings;
    private volatile String role;

    // Producer components
    private volatile ChangeCapture changeCapture;
    private volatile OperationBatcher batcher;
    private volatile BatchShipper shipper;

    // Consumer components
    private volatile ReplayService replayService;
    private volatile RestClient replayClient;

    // Shared
    private volatile TransportConfig transportConfig;
    private volatile boolean running = false;

    public ReplicationPlugin(Settings settings) {
        this.settings = settings;
        this.role = ReplicationSettings.ROLE.get(settings);
        logger.info("ReplicationPlugin initialized with role: {}", role);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return ReplicationSettings.getSettings();
    }

    @Override
    public Collection<RestHandler> getRestHandlers(Settings settings,
                                              NamedWriteableRegistry namedWriteableRegistry,
                                              RestController restController,
                                              ClusterSettings clusterSettings,
                                              IndexScopedSettings indexScopedSettings,
                                              SettingsFilter settingsFilter,
                                              IndexNameExpressionResolver indexNameExpressionResolver,
                                              Supplier<DiscoveryNodes> nodesInCluster,
                                              Predicate<NodeFeature> clusterSupportsFeature) {
        return Arrays.asList(
            new StartReplicationAction(this),
            new StopReplicationAction(this),
            new ReplicationStatusAction(this)
        );
    }

    @Override
    public Collection<ActionFilter> getActionFilters() {
        if ("producer".equals(role)) {
            return Collections.singletonList(new DeleteRoutingCapture());
        }
        return Collections.emptyList();
    }

    /**
     * Register the IndexingOperationListener on every index.
     * Only fires events when role is "producer" and replication is running.
     */
    @Override
    public void onIndexModule(IndexModule indexModule) {
        // Create a lazy-initialized listener that delegates to changeCapture
        // when it's active, and is a no-op otherwise
        indexModule.addIndexOperationListener(new LazyChangeCapture());
    }

    /**
     * Start replication based on configured role.
     */
    public synchronized void startReplication() {
        if (running) {
            throw new IllegalStateException("Replication is already running");
        }

        this.role = ReplicationSettings.ROLE.get(settings);
        this.transportConfig = new TransportConfig(settings);
        transportConfig.validate();

        if ("producer".equals(role)) {
            startProducer();
        } else if ("consumer".equals(role)) {
            startConsumer();
        } else {
            throw new IllegalStateException("Cannot start replication with role: " + role +
                                            ". Set replication.role to 'producer' or 'consumer'");
        }

        running = true;
        logger.info("Replication started as {}", role);
    }

    /**
     * Stop replication (either role).
     */
    public synchronized void stopReplication() {
        if (!running) {
            logger.info("Replication is not running");
            return;
        }

        if ("producer".equals(role)) {
            stopProducer();
        } else if ("consumer".equals(role)) {
            stopConsumer();
        }

        running = false;
        logger.info("Replication stopped");
    }

    private void startProducer() {
        S3Transport s3Transport = new S3Transport(transportConfig);
        SQSNotifier sqsNotifier = new SQSNotifier(transportConfig);
        shipper = new BatchShipper(s3Transport, sqsNotifier);

        int batchSize = ReplicationSettings.BATCH_SIZE.get(settings);
        long batchAgeMs = ReplicationSettings.BATCH_AGE_MS.get(settings);
        long batchBytes = ReplicationSettings.BATCH_BYTES.get(settings);

        batcher = new OperationBatcher(batchSize, batchAgeMs, batchBytes, shipper::ship);
        batcher.start();

        // Parse include/exclude indices
        Set<String> included = parseCommaSeparated(ReplicationSettings.INCLUDE_INDICES.get(settings));
        Set<String> excluded = parseCommaSeparated(ReplicationSettings.EXCLUDE_INDICES.get(settings));

        changeCapture = new ChangeCapture(batcher, included, excluded);
        logger.info("Producer started: batch_size={}, age_ms={}, bytes={}, include={}, exclude={}",
                     batchSize, batchAgeMs, batchBytes, included, excluded);
    }

    private void stopProducer() {
        changeCapture = null; // stop capturing
        if (batcher != null) {
            batcher.stop(); // flushes remaining events
        }
        if (shipper != null) {
            shipper.close();
        }
    }

    private void startConsumer() {
        S3Transport s3Transport = new S3Transport(transportConfig);
        SQSNotifier sqsNotifier = new SQSNotifier(transportConfig);

        // Build low-level REST client pointing to localhost (this cluster)
        replayClient = RestClient.builder(new HttpHost("localhost", 9200, "http")).build();

        int bulkSize = ReplicationSettings.REPLAY_BULK_SIZE.get(settings);
        BulkReplayer bulkReplayer = new BulkReplayer(replayClient, bulkSize);
        CheckpointManager checkpointManager = new CheckpointManager(replayClient);

        int pollBatch = ReplicationSettings.REPLAY_POLL_BATCH.get(settings);
        int pollWait = ReplicationSettings.REPLAY_POLL_WAIT_SEC.get(settings);
        int workerThreads = ReplicationSettings.REPLAY_WORKER_THREADS.get(settings);

        replayService = new ReplayService(sqsNotifier, s3Transport, bulkReplayer,
                                           checkpointManager, pollBatch, pollWait, workerThreads);
        replayService.start();
        logger.info("Consumer started: bulk_size={}, poll_batch={}, poll_wait={}s, workers={}",
                     bulkSize, pollBatch, pollWait, workerThreads);
    }

    private void stopConsumer() {
        if (replayService != null) {
            replayService.stop();
        }
        if (replayClient != null) {
            try {
                replayClient.close();
            } catch (IOException e) {
                logger.warn("Error closing replay client", e);
            }
        }
    }

    /**
     * Get current replication status and metrics.
     */
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("role", role);
        status.put("running", running);

        if ("producer".equals(role) && running) {
            Map<String, Object> producer = new HashMap<>();
            if (changeCapture != null) {
                producer.put("captured_ops", changeCapture.getCapturedOps());
                producer.put("skipped_ops", changeCapture.getSkippedOps());
                producer.put("failed_ops", changeCapture.getFailedOps());
            }
            if (batcher != null) {
                producer.put("queue_size", batcher.getQueueSize());
                producer.put("queue_bytes", batcher.getQueueBytes());
            }
            if (shipper != null) {
                producer.put("batches_shipped", shipper.getTotalBatchesShipped());
                producer.put("events_shipped", shipper.getTotalEventsShipped());
                producer.put("bytes_shipped", shipper.getTotalBytesShipped());
                producer.put("ship_failures", shipper.getTotalShipFailures());
            }
            status.put("producer", producer);
        }

        if ("consumer".equals(role) && running) {
            Map<String, Object> consumer = new HashMap<>();
            if (replayService != null) {
                consumer.put("batches_processed", replayService.getTotalBatchesProcessed());
                consumer.put("batches_failed", replayService.getTotalBatchesFailed());
                consumer.put("last_poll_ms", replayService.getLastPollTimeMs());
                consumer.put("worker_threads", replayService.getWorkerThreads());
                consumer.put("active_workers", replayService.getActiveWorkers());
            }
            status.put("consumer", consumer);
        }

        return status;
    }

    public String getRole() {
        return role;
    }

    @Override
    public void close() throws IOException {
        stopReplication();
    }

    private static Set<String> parseCommaSeparated(String value) {
        if (value == null || value.trim().isEmpty()) {
            return new HashSet<>();
        }
        Set<String> result = new HashSet<>();
        for (String part : value.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                result.add(trimmed);
            }
        }
        return result;
    }

    /**
     * Lazy proxy for ChangeCapture that is a no-op when replication
     * is not running or role is not producer.
     */
    private class LazyChangeCapture implements org.elasticsearch.index.shard.IndexingOperationListener {
        @Override
        public void postIndex(org.elasticsearch.index.shard.ShardId shardId,
                              org.elasticsearch.index.engine.Engine.Index operation,
                              org.elasticsearch.index.engine.Engine.IndexResult result) {
            ChangeCapture cc = changeCapture;
            if (cc != null && running) {
                cc.postIndex(shardId, operation, result);
            }
        }

        @Override
        public void postDelete(org.elasticsearch.index.shard.ShardId shardId,
                               org.elasticsearch.index.engine.Engine.Delete operation,
                               org.elasticsearch.index.engine.Engine.DeleteResult result) {
            ChangeCapture cc = changeCapture;
            if (cc != null && running) {
                cc.postDelete(shardId, operation, result);
            }
        }
    }
}
