package com.elasticsearch.replication.replay;

import com.elasticsearch.replication.cdc.ChangeEvent;
import com.elasticsearch.replication.transport.S3Transport;
import com.elasticsearch.replication.transport.SQSNotifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background service running on the passive cluster that:
 * 1. Polls SQS for batch notifications
 * 2. Downloads batch files from S3
 * 3. Parses and replays events via BulkReplayer
 * 4. Updates checkpoints on success
 * 5. Deletes SQS messages on success
 *
 * Uses a single poll thread + a worker pool for parallel message processing.
 * External versioning and per-shard checkpoints make out-of-order replay safe.
 */
public class ReplayService {

    private static final Logger logger = LogManager.getLogger(ReplayService.class);

    private final SQSNotifier sqsNotifier;
    private final S3Transport s3Transport;
    private final BulkReplayer bulkReplayer;
    private final CheckpointManager checkpointManager;

    private final int pollBatchSize;
    private final int pollWaitSeconds;
    private final int workerThreads;

    private final ScheduledExecutorService scheduler;
    private final ExecutorService workerPool;
    private volatile ScheduledFuture<?> pollTask;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    // Metrics
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalBatchesFailed = new AtomicLong(0);
    private final AtomicLong lastPollTimeMs = new AtomicLong(0);
    private final AtomicInteger activeWorkers = new AtomicInteger(0);

    public ReplayService(SQSNotifier sqsNotifier, S3Transport s3Transport,
                         BulkReplayer bulkReplayer, CheckpointManager checkpointManager,
                         int pollBatchSize, int pollWaitSeconds, int workerThreads) {
        this.sqsNotifier = sqsNotifier;
        this.s3Transport = s3Transport;
        this.bulkReplayer = bulkReplayer;
        this.checkpointManager = checkpointManager;
        this.pollBatchSize = pollBatchSize;
        this.pollWaitSeconds = pollWaitSeconds;
        this.workerThreads = workerThreads;

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "es-replication-poller");
            t.setDaemon(true);
            return t;
        });

        this.workerPool = Executors.newFixedThreadPool(workerThreads, new java.util.concurrent.ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "es-replication-worker-" + counter.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            pollTask = scheduler.scheduleWithFixedDelay(
                this::pollAndReplay,
                2000,    // 2 second delay to let the node fully start
                500,     // 500ms between poll cycles (faster since workers handle processing)
                TimeUnit.MILLISECONDS
            );
            logger.info("ReplayService started: pollBatch={}, waitSec={}, workerThreads={}",
                         pollBatchSize, pollWaitSeconds, workerThreads);
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (pollTask != null) {
                pollTask.cancel(false);
            }
            scheduler.shutdown();
            workerPool.shutdown();
            try {
                if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                    workerPool.shutdownNow();
                }
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                workerPool.shutdownNow();
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("ReplayService stopped");
        }
    }

    /**
     * Single poll cycle: fetch messages from SQS and dispatch to worker pool.
     */
    private void pollAndReplay() {
        if (!running.get()) return;

        // Lazy initialization of checkpoint manager (must run outside HTTP handler thread)
        if (initialized.compareAndSet(false, true)) {
            try {
                checkpointManager.initialize();
                logger.info("Checkpoint manager initialized");
            } catch (Exception e) {
                logger.error("Failed to initialize checkpoint manager, will retry", e);
                initialized.set(false);
                return;
            }
        }

        try {
            lastPollTimeMs.set(System.currentTimeMillis());
            List<SQSNotifier.SQSMessage> messages = sqsNotifier.pollMessages(pollBatchSize, pollWaitSeconds);

            if (messages.isEmpty()) {
                return;
            }

            logger.debug("Received {} SQS messages, dispatching to {} workers (active={})",
                         messages.size(), workerThreads, activeWorkers.get());

            // Dispatch all messages to the worker pool in parallel
            List<Future<?>> futures = new ArrayList<>(messages.size());
            for (SQSNotifier.SQSMessage message : messages) {
                if (!running.get()) break;
                futures.add(workerPool.submit(() -> {
                    activeWorkers.incrementAndGet();
                    try {
                        processMessage(message);
                    } finally {
                        activeWorkers.decrementAndGet();
                    }
                }));
            }

            // Wait for all workers to finish this batch before polling again
            for (Future<?> future : futures) {
                try {
                    future.get(300, TimeUnit.SECONDS);
                } catch (Exception e) {
                    logger.warn("Worker task failed: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            logger.error("Error in poll-and-replay cycle", e);
        }
    }

    /**
     * Process a single SQS message (one S3 batch file).
     */
    private void processMessage(SQSNotifier.SQSMessage message) {
        String s3Key = null;
        try {
            // Parse the SQS message to get S3 key and metadata
            String body = message.body();
            s3Key = extractField(body, "s3_key");

            // Skip and delete invalid messages (e.g., manual test messages with non-JSON bodies)
            if (s3Key == null) {
                logger.warn("Skipping invalid SQS message (no s3_key): {}",
                            body.substring(0, Math.min(100, body.length())));
                sqsNotifier.deleteMessage(message);
                return;
            }

            String index = extractField(body, "index");
            int shardId = Integer.parseInt(extractNumericField(body, "shard_id"));
            long toSeqNo = Long.parseLong(extractNumericField(body, "to_seq_no"));

            // Download from S3
            String ndjsonContent = s3Transport.downloadBatch(s3Key);

            // Parse events
            List<ChangeEvent> events = BulkReplayer.parseNdJson(ndjsonContent);

            if (events.isEmpty()) {
                logger.debug("No events in batch {}", s3Key);
                sqsNotifier.deleteMessage(message);
                return;
            }

            // Replay all events — external versioning makes this idempotent.
            // We intentionally do NOT skip based on checkpoint because standard
            // SQS does not guarantee ordering. A later batch (higher seq_no) can
            // arrive before an earlier one, advancing the checkpoint past events
            // that were never actually replayed.
            BulkReplayer.ReplayResult result = bulkReplayer.replay(events);

            // Update checkpoint
            if (result.maxSeqNo >= 0) {
                checkpointManager.updateCheckpoint(
                    index, shardId, result.maxSeqNo, result.maxPrimaryTerm, result.succeeded);
            }

            // Delete SQS message on success
            if (result.failed == 0) {
                sqsNotifier.deleteMessage(message);
                totalBatchesProcessed.incrementAndGet();
            } else {
                // Leave message visible for retry (will reappear after visibility timeout)
                totalBatchesFailed.incrementAndGet();
                logger.warn("Batch {} had {} failures, leaving for retry", s3Key, result.failed);
            }

        } catch (Exception e) {
            totalBatchesFailed.incrementAndGet();
            logger.error("Failed to process batch {}: {}", s3Key, e.getMessage(), e);
            // Message will become visible again after SQS visibility timeout
        }
    }

    private static String extractField(String json, String field) {
        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);
        if (start == -1) return null;
        start += key.length();
        int end = json.indexOf("\"", start);
        return json.substring(start, end);
    }

    private static String extractNumericField(String json, String field) {
        String key = "\"" + field + "\":";
        int start = json.indexOf(key);
        if (start == -1) return "0";
        start += key.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return json.substring(start, end);
    }

    // Status
    public boolean isRunning() { return running.get(); }
    public long getTotalBatchesProcessed() { return totalBatchesProcessed.get(); }
    public long getTotalBatchesFailed() { return totalBatchesFailed.get(); }
    public long getLastPollTimeMs() { return lastPollTimeMs.get(); }
    public int getActiveWorkers() { return activeWorkers.get(); }
    public int getWorkerThreads() { return workerThreads; }
}
