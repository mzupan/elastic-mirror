package com.elasticsearch.replication.transport;

import com.elasticsearch.replication.cdc.ChangeEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Orchestrates the batch shipping pipeline:
 * 1. Receives a batch of mixed events from the OperationBatcher
 * 2. Groups events by index+shard for organized S3 storage
 * 3. Uploads each group to S3
 * 4. Sends SQS notification for each upload
 *
 * This is the batchConsumer passed to OperationBatcher.
 */
public class BatchShipper {

    private static final Logger logger = LogManager.getLogger(BatchShipper.class);

    private final S3Transport s3Transport;
    private final SQSNotifier sqsNotifier;

    // Metrics
    private final AtomicLong totalBatchesShipped = new AtomicLong(0);
    private final AtomicLong totalEventsShipped = new AtomicLong(0);
    private final AtomicLong totalBytesShipped = new AtomicLong(0);
    private final AtomicLong totalShipFailures = new AtomicLong(0);

    public BatchShipper(S3Transport s3Transport, SQSNotifier sqsNotifier) {
        this.s3Transport = s3Transport;
        this.sqsNotifier = sqsNotifier;
    }

    /**
     * Ship a batch of events. Called by OperationBatcher's flush.
     * Groups by index+shard, uploads each group, notifies via SQS.
     */
    public void ship(List<ChangeEvent> events) {
        // Group by index + shard for organized S3 storage
        Map<String, List<ChangeEvent>> groups = new HashMap<>();
        for (ChangeEvent event : events) {
            String groupKey = event.getIndex() + "/" + event.getShardId();
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(event);
        }

        for (Map.Entry<String, List<ChangeEvent>> entry : groups.entrySet()) {
            List<ChangeEvent> group = entry.getValue();
            try {
                // Upload to S3
                S3Transport.BatchUploadResult result = s3Transport.uploadBatch(group);

                // Notify SQS
                sqsNotifier.notifyBatchUploaded(result);

                totalBatchesShipped.incrementAndGet();
                totalEventsShipped.addAndGet(group.size());
                totalBytesShipped.addAndGet(result.getPayloadBytes());

                logger.debug("Shipped batch {}: {} events for {}",
                             result.getS3Key(), group.size(), entry.getKey());
            } catch (Exception e) {
                totalShipFailures.incrementAndGet();
                logger.error("Failed to ship batch for {}: {} events lost",
                             entry.getKey(), group.size(), e);
                // TODO: Phase 5 — local disk spill buffer for retry
            }
        }
    }

    // Metrics
    public long getTotalBatchesShipped() { return totalBatchesShipped.get(); }
    public long getTotalEventsShipped() { return totalEventsShipped.get(); }
    public long getTotalBytesShipped() { return totalBytesShipped.get(); }
    public long getTotalShipFailures() { return totalShipFailures.get(); }

    public void close() {
        s3Transport.close();
        sqsNotifier.close();
    }
}
