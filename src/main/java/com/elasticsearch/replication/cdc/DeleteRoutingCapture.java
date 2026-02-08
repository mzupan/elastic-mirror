package com.elasticsearch.replication.cdc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;

import java.util.concurrent.ConcurrentHashMap;

/**
 * ActionFilter that captures routing values from delete requests before they
 * reach the engine layer. Engine.Delete in ES 7.x does not expose routing,
 * so this filter intercepts the request early and stores routing in a map
 * that ChangeCapture can read from during postDelete.
 *
 * This works reliably when the coordinating node is the same as the data node
 * (single-node clusters, or when the client targets the primary shard's node).
 */
public class DeleteRoutingCapture implements ActionFilter {

    private static final Logger logger = LogManager.getLogger(DeleteRoutingCapture.class);

    /**
     * Max entries before force-clearing stale routing entries.
     * Entries are normally removed by getAndRemoveRouting(), but if a delete fails
     * before reaching postDelete, the entry leaks. This cap prevents unbounded growth.
     */
    private static final int MAX_CACHE_SIZE = 100_000;

    private static final ConcurrentHashMap<String, String> routingCache = new ConcurrentHashMap<>();

    @Override
    public int order() {
        return 0;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(
            Task task, String action, Request request,
            ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {

        try {
            if (request instanceof BulkRequest) {
                BulkRequest bulkRequest = (BulkRequest) request;
                for (DocWriteRequest<?> item : bulkRequest.requests()) {
                    if (item instanceof DeleteRequest) {
                        captureRouting((DeleteRequest) item);
                    }
                }
            } else if (request instanceof DeleteRequest) {
                captureRouting((DeleteRequest) request);
            }
        } catch (Exception e) {
            logger.debug("Error capturing delete routing: {}", e.getMessage());
        }

        chain.proceed(task, action, request, listener);
    }

    private void captureRouting(DeleteRequest request) {
        if (request.routing() != null) {
            if (routingCache.size() > MAX_CACHE_SIZE) {
                logger.warn("Routing cache exceeded {} entries, clearing stale entries", MAX_CACHE_SIZE);
                routingCache.clear();
            }
            String key = request.index() + "/" + request.id();
            routingCache.put(key, request.routing());
        }
    }

    /**
     * Retrieve and remove the captured routing for a delete operation.
     * Returns null if no routing was captured (no custom routing, or
     * the delete was coordinated on a different node).
     */
    public static String getAndRemoveRouting(String index, String id) {
        return routingCache.remove(index + "/" + id);
    }
}
