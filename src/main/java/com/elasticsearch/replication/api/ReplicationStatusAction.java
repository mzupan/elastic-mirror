package com.elasticsearch.replication.api;

import com.elasticsearch.replication.ReplicationPlugin;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ReplicationStatusAction extends BaseRestHandler {

    private final ReplicationPlugin plugin;

    public ReplicationStatusAction(ReplicationPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String getName() {
        return "replication_status_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(RestRequest.Method.GET, "/_replication/status")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> {
            try {
                Map<String, Object> status = plugin.getStatus();
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                for (Map.Entry<String, Object> entry : status.entrySet()) {
                    builder.field(entry.getKey(), entry.getValue());
                }
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder.field("error", e.getMessage());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
            }
        };
    }
}
