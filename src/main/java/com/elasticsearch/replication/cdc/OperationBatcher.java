package com.elasticsearch.replication.cdc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Batches ChangeEvents by count or time window, whichever triggers first.
 * Uses a lock-free queue for ingestion (called from indexing thread)
 * and a scheduled executor for time-based flushing.
 *
 * Thread safety: add() is called from ES indexing threads (concurrent),
 * flush is single-threaded via the scheduled executor.
 */
public class OperationBatcher {

    private static final Logger logger = LogManager.getLogger(OperationBatcher.class);

    private final int maxBatchSize;
    private final long maxBatchAgeMs;
    private final long maxBatchBytes;
    private final Consumer<List<ChangeEvent>> batchConsumer;

    private final ConcurrentLinkedQueue<ChangeEvent> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger queueSize = new AtomicInteger(0);
    private final AtomicLong queueBytes = new AtomicLong(0);

    private final ScheduledExecutorService scheduler;
    private volatile ScheduledFuture<?> flushTask;
    private volatile boolean running = false;

    /**
     * @param maxBatchSize   max number of events per batch (e.g., 1000)
     * @param maxBatchAgeMs  max age in ms before flushing (e.g., 5000)
     * @param maxBatchBytes  max total bytes before flushing (e.g., 5MB)
     * @param batchConsumer  callback receiving each completed batch
     */
    public OperationBatcher(int maxBatchSize, long maxBatchAgeMs, long maxBatchBytes,
                            Consumer<List<ChangeEvent>> batchConsumer) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchAgeMs = maxBatchAgeMs;
        this.maxBatchBytes = maxBatchBytes;
        this.batchConsumer = batchConsumer;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "es-replication-batcher");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running) return;
        running = true;
        flushTask = scheduler.scheduleWithFixedDelay(
            this::timedFlush,
            maxBatchAgeMs,
            maxBatchAgeMs,
            TimeUnit.MILLISECONDS
        );
        logger.info("OperationBatcher started: maxBatch={}, maxAge={}ms, maxBytes={}",
                     maxBatchSize, maxBatchAgeMs, maxBatchBytes);
    }

    public void stop() {
        running = false;
        if (flushTask != null) {
            flushTask.cancel(false);
        }
        // Final flush of remaining events
        flush();
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        logger.info("OperationBatcher stopped");
    }

    /**
     * Add an event to the batch queue. Called from ES indexing threads.
     * This must be non-blocking to avoid impacting indexing performance.
     */
    public void add(ChangeEvent event) {
        if (!running) {
            logger.warn("Batcher not running, dropping event: {}", event);
            return;
        }
        queue.add(event);
        int size = queueSize.incrementAndGet();
        long bytes = queueBytes.addAndGet(event.estimatedSizeBytes());

        // Check if count or byte threshold is reached
        if (size >= maxBatchSize || bytes >= maxBatchBytes) {
            // Submit async flush to avoid blocking the indexing thread
            scheduler.submit(this::flush);
        }
    }

    /**
     * Time-based flush triggered by the scheduled executor.
     */
    private void timedFlush() {
        if (queueSize.get() > 0) {
            flush();
        }
    }

    /**
     * Drain the queue and dispatch the batch.
     * Synchronized on a single-threaded executor, so no concurrent flushes.
     */
    private synchronized void flush() {
        int size = queueSize.get();
        if (size == 0) return;

        List<ChangeEvent> batch = new ArrayList<>(Math.min(size, maxBatchSize));
        long batchBytes = 0;
        int count = 0;

        ChangeEvent event;
        while (count < maxBatchSize && (event = queue.poll()) != null) {
            batch.add(event);
            batchBytes += event.estimatedSizeBytes();
            count++;
        }

        queueSize.addAndGet(-count);
        this.queueBytes.addAndGet(-batchBytes);

        if (!batch.isEmpty()) {
            try {
                logger.debug("Flushing batch: {} events, ~{} bytes", batch.size(), batchBytes);
                batchConsumer.accept(batch);
            } catch (Exception e) {
                logger.error("Failed to process batch of {} events, events will be lost", batch.size(), e);
                // TODO: Phase 5 — implement local disk spill for retry
            }
        }

        // If there are still events in the queue (above maxBatchSize), schedule another flush
        if (queueSize.get() >= maxBatchSize) {
            scheduler.submit(this::flush);
        }
    }

    public int getQueueSize() {
        return queueSize.get();
    }

    public long getQueueBytes() {
        return queueBytes.get();
    }

    public boolean isRunning() {
        return running;
    }
}
