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

public class StopReplicationAction extends BaseRestHandler {

    private final ReplicationPlugin plugin;

    public StopReplicationAction(ReplicationPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String getName() {
        return "stop_replication_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(
            new Route(RestRequest.Method.POST, "/_replication/stop")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return channel -> {
            try {
                plugin.stopReplication();
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder.field("acknowledged", true);
                builder.field("message", "Replication stopped");
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            } catch (Exception e) {
                XContentBuilder builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder.field("acknowledged", false);
                builder.field("error", e.getMessage());
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, builder));
            }
        };
    }
}
