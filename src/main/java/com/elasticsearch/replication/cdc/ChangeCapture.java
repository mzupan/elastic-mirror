package com.elasticsearch.replication.cdc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Captures index and delete operations via the IndexingOperationListener interface.
 * This is installed per-index and fires on every primary shard write operation.
 *
 * Key design decisions:
 * - postIndex/postDelete (not pre-) so we capture the assigned seq_no
 * - Non-blocking: just constructs a ChangeEvent and hands to the batcher
 * - Respects index include/exclude filters to limit which indices are replicated
 */
public class ChangeCapture implements IndexingOperationListener {

    private static final Logger logger = LogManager.getLogger(ChangeCapture.class);

    private final OperationBatcher batcher;
    private final Set<String> includedIndices;
    private final Set<String> excludedIndices;
    private final Set<String> excludedPrefixes;

    // Metrics
    private final AtomicLong capturedOps = new AtomicLong(0);
    private final AtomicLong skippedOps = new AtomicLong(0);
    private final AtomicLong failedOps = new AtomicLong(0);

    public ChangeCapture(OperationBatcher batcher,
                         Set<String> includedIndices,
                         Set<String> excludedIndices) {
        this.batcher = batcher;
        this.includedIndices = includedIndices != null ? includedIndices : Set.of();
        this.excludedIndices = excludedIndices != null ? excludedIndices : Set.of();
        this.excludedPrefixes = new CopyOnWriteArraySet<>();
        // Always exclude internal replication indices
        this.excludedPrefixes.add(".");
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index operation, Engine.IndexResult result) {
        if (result.getResultType() != Engine.Result.Type.SUCCESS) {
            return;
        }

        String indexName = shardId.getIndexName();
        if (!shouldReplicate(indexName)) {
            skippedOps.incrementAndGet();
            return;
        }

        try {
            ChangeEvent event = new ChangeEvent(
                indexName,
                operation.id(),
                operation.source().utf8ToString(),
                result.getSeqNo(),
                result.getTerm(),
                ChangeEvent.OpType.INDEX,
                operation.routing(),
                shardId.id(),
                System.currentTimeMillis()
            );
            batcher.add(event);
            capturedOps.incrementAndGet();

            if (logger.isTraceEnabled()) {
                logger.trace("Captured INDEX: {}/{} seq={}", indexName, operation.id(), result.getSeqNo());
            }
        } catch (Exception e) {
            failedOps.incrementAndGet();
            logger.warn("Failed to capture INDEX event for {}/{}: {}",
                        indexName, operation.id(), e.getMessage());
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete operation, Engine.DeleteResult result) {
        if (result.getResultType() != Engine.Result.Type.SUCCESS) {
            return;
        }

        String indexName = shardId.getIndexName();
        if (!shouldReplicate(indexName)) {
            skippedOps.incrementAndGet();
            return;
        }

        try {
            // Engine.Delete doesn't expose routing in ES 7.x.
            // Retrieve routing captured by the ActionFilter if available.
            String routing = DeleteRoutingCapture.getAndRemoveRouting(indexName, operation.id());

            ChangeEvent event = new ChangeEvent(
                indexName,
                operation.id(),
                null,  // no source for deletes
                result.getSeqNo(),
                result.getTerm(),
                ChangeEvent.OpType.DELETE,
                routing,
                shardId.id(),
                System.currentTimeMillis()
            );
            batcher.add(event);
            capturedOps.incrementAndGet();

            if (logger.isTraceEnabled()) {
                logger.trace("Captured DELETE: {}/{} seq={}", indexName, operation.id(), result.getSeqNo());
            }
        } catch (Exception e) {
            failedOps.incrementAndGet();
            logger.warn("Failed to capture DELETE event for {}/{}: {}",
                        indexName, operation.id(), e.getMessage());
        }
    }

    /**
     * Determine if an index should be replicated based on include/exclude rules.
     * - If includedIndices is non-empty, only those indices are replicated.
     * - excludedIndices are always excluded.
     * - Indices starting with "." are excluded by default (system/internal indices).
     */
    private boolean shouldReplicate(String indexName) {
        // Always exclude internal indices
        for (String prefix : excludedPrefixes) {
            if (indexName.startsWith(prefix)) {
                return false;
            }
        }

        // Check explicit exclude list
        if (excludedIndices.contains(indexName)) {
            return false;
        }

        // If include list is specified, index must be in it
        if (!includedIndices.isEmpty()) {
            return includedIndices.contains(indexName) || matchesWildcard(indexName, includedIndices);
        }

        // Default: replicate everything not excluded
        return true;
    }

    /**
     * Simple wildcard matching for index patterns like "logs-*".
     */
    private boolean matchesWildcard(String indexName, Set<String> patterns) {
        for (String pattern : patterns) {
            if (pattern.endsWith("*")) {
                String prefix = pattern.substring(0, pattern.length() - 1);
                if (indexName.startsWith(prefix)) {
                    return true;
                }
            }
        }
        return false;
    }

    // Metrics accessors
    public long getCapturedOps() { return capturedOps.get(); }
    public long getSkippedOps() { return skippedOps.get(); }
    public long getFailedOps() { return failedOps.get(); }
}
