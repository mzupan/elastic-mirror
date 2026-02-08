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
 * 3. Uploads each group to S3 (with retry on transient failures)
 * 4. Sends SQS notification for each upload
 *
 * This is the batchConsumer passed to OperationBatcher.
 */
public class BatchShipper {

    private static final Logger logger = LogManager.getLogger(BatchShipper.class);
    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 500;

    private final S3Transport s3Transport;
    private final SQSNotifier sqsNotifier;

    // Metrics
    private final AtomicLong totalBatchesShipped = new AtomicLong(0);
    private final AtomicLong totalEventsShipped = new AtomicLong(0);
    private final AtomicLong totalBytesShipped = new AtomicLong(0);
    private final AtomicLong totalShipFailures = new AtomicLong(0);
    private final AtomicLong totalRetries = new AtomicLong(0);

    public BatchShipper(S3Transport s3Transport, SQSNotifier sqsNotifier) {
        this.s3Transport = s3Transport;
        this.sqsNotifier = sqsNotifier;
    }

    /**
     * Ship a batch of events. Called by OperationBatcher's flush.
     * Groups by index+shard, uploads each group, notifies via SQS.
     * Retries transient failures with exponential backoff.
     */
    /**
     * Ship a batch of events. Groups by index+shard, uploads each group to S3,
     * notifies via SQS. If any groups fail after retries, throws RuntimeException
     * so the caller can re-queue failed events.
     */
    public void ship(List<ChangeEvent> events) {
        // Group by index + shard for organized S3 storage
        Map<String, List<ChangeEvent>> groups = new HashMap<>();
        for (ChangeEvent event : events) {
            String groupKey = event.getIndex() + "/" + event.getShardId();
            groups.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(event);
        }

        int failedCount = 0;
        Exception lastError = null;
        for (Map.Entry<String, List<ChangeEvent>> entry : groups.entrySet()) {
            if (!shipWithRetry(entry.getKey(), entry.getValue())) {
                failedCount += entry.getValue().size();
            }
        }

        if (failedCount > 0) {
            throw new RuntimeException(failedCount + " events failed to ship after " + MAX_RETRIES + " retries");
        }
    }

    /**
     * Attempt to ship a single group with exponential backoff.
     * Returns true on success, false if all retries exhausted.
     */
    private boolean shipWithRetry(String groupKey, List<ChangeEvent> group) {
        Exception lastException = null;

        for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
            try {
                if (attempt > 0) {
                    long backoffMs = INITIAL_BACKOFF_MS * (1L << (attempt - 1));
                    logger.warn("Retry {}/{} for {} after {}ms backoff",
                                attempt, MAX_RETRIES, groupKey, backoffMs);
                    Thread.sleep(backoffMs);
                    totalRetries.incrementAndGet();
                }

                // Upload to S3
                S3Transport.BatchUploadResult result = s3Transport.uploadBatch(group);

                // Notify SQS
                sqsNotifier.notifyBatchUploaded(result);

                totalBatchesShipped.incrementAndGet();
                totalEventsShipped.addAndGet(group.size());
                totalBytesShipped.addAndGet(result.getPayloadBytes());

                logger.debug("Shipped batch {}: {} events for {}",
                             result.getS3Key(), group.size(), groupKey);
                return true;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                totalShipFailures.incrementAndGet();
                logger.error("Interrupted while retrying batch for {}: {} events pending retry",
                             groupKey, group.size());
                return false;
            } catch (Exception e) {
                lastException = e;
            }
        }

        // All retries exhausted — caller will re-queue
        totalShipFailures.incrementAndGet();
        logger.error("Failed to ship batch for {} after {} retries: {} events pending retry",
                     groupKey, MAX_RETRIES, group.size(), lastException);
        return false;
    }

    // Metrics
    public long getTotalBatchesShipped() { return totalBatchesShipped.get(); }
    public long getTotalEventsShipped() { return totalEventsShipped.get(); }
    public long getTotalBytesShipped() { return totalBytesShipped.get(); }
    public long getTotalShipFailures() { return totalShipFailures.get(); }
    public long getTotalRetries() { return totalRetries.get(); }

    public void close() {
        s3Transport.close();
        sqsNotifier.close();
    }
}
