package com.elasticsearch.replication.replay;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks per-index-per-shard replication progress on the passive cluster.
 * Uses low-level RestClient to avoid jar hell with RestHighLevelClient.
 *
 * Stores checkpoints in a dedicated internal index (.replication_checkpoint).
 * Each checkpoint document ID: {index}:{shardId}
 */
public class CheckpointManager {

    private static final Logger logger = LogManager.getLogger(CheckpointManager.class);
    private static final String CHECKPOINT_INDEX = ".replication_checkpoint";

    private final RestClient client;
    private final ConcurrentHashMap<String, CheckpointState> cache = new ConcurrentHashMap<>();

    public CheckpointManager(RestClient client) {
        this.client = client;
    }

    public void initialize() throws IOException {
        // Check if index exists
        try {
            Request headReq = new Request("HEAD", "/" + CHECKPOINT_INDEX);
            client.performRequest(headReq);
            // Index exists
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                // Create the index
                Request createReq = new Request("PUT", "/" + CHECKPOINT_INDEX);
                createReq.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,\"auto_expand_replicas\":\"0-1\"}}");
                client.performRequest(createReq);
                logger.info("Created checkpoint index: {}", CHECKPOINT_INDEX);
            } else {
                throw e;
            }
        }
    }

    public long getLastReplayedSeqNo(String index, int shardId) {
        String key = checkpointKey(index, shardId);

        CheckpointState cached = cache.get(key);
        if (cached != null) {
            return cached.lastSeqNo;
        }

        // Load from ES index on cache miss (synchronized per key to avoid redundant reads)
        synchronized (cache) {
            // Double-check after acquiring lock
            cached = cache.get(key);
            if (cached != null) {
                return cached.lastSeqNo;
            }

            try {
                Request getReq = new Request("GET", "/" + CHECKPOINT_INDEX + "/_doc/" + key);
                Response response = client.performRequest(getReq);
                String body = readResponseBody(response);

                if (body.contains("\"found\":true")) {
                    long seqNo = extractLong(body, "last_seq_no");
                    long primaryTerm = extractLong(body, "last_primary_term");
                    long eventsReplayed = extractLong(body, "events_replayed");
                    CheckpointState state = new CheckpointState(seqNo, primaryTerm, eventsReplayed);
                    cache.put(key, state);
                    return seqNo;
                }
            } catch (ResponseException e) {
                if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                    logger.warn("Failed to read checkpoint for {}: {}", key, e.getMessage());
                }
            } catch (IOException e) {
                logger.warn("Failed to read checkpoint for {}: {}", key, e.getMessage());
            }
        }

        return -1;
    }

    public void updateCheckpoint(String index, int shardId, long lastSeqNo,
                                  long primaryTerm, int eventsInBatch) throws IOException {
        String key = checkpointKey(index, shardId);

        // Atomically advance checkpoint — never go backwards (critical for parallel workers)
        CheckpointState newState = cache.compute(key, (k, current) -> {
            if (current != null && current.lastSeqNo >= lastSeqNo) {
                // Another worker already advanced past this point — keep the higher value
                return new CheckpointState(current.lastSeqNo, current.primaryTerm,
                                           current.eventsReplayed + eventsInBatch);
            }
            long totalEvents = eventsInBatch + (current != null ? current.eventsReplayed : 0);
            return new CheckpointState(lastSeqNo, primaryTerm, totalEvents);
        });

        String json = "{\"index\":\"" + index + "\""
            + ",\"shard_id\":" + shardId
            + ",\"last_seq_no\":" + newState.lastSeqNo
            + ",\"last_primary_term\":" + newState.primaryTerm
            + ",\"events_replayed\":" + newState.eventsReplayed
            + ",\"last_updated_ms\":" + System.currentTimeMillis() + "}";

        Request indexReq = new Request("PUT", "/" + CHECKPOINT_INDEX + "/_doc/" + key);
        indexReq.setJsonEntity(json);
        client.performRequest(indexReq);

        logger.debug("Checkpoint updated: {} -> seq={}, term={}, total={}",
                      key, newState.lastSeqNo, newState.primaryTerm, newState.eventsReplayed);
    }

    public Map<String, CheckpointState> getAllCheckpoints() {
        return new ConcurrentHashMap<>(cache);
    }

    private String readResponseBody(Response response) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        }
    }

    private long extractLong(String json, String field) {
        String key = "\"" + field + "\":";
        int start = json.indexOf(key);
        if (start == -1) return 0;
        start += key.length();
        int end = start;
        while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
            end++;
        }
        return Long.parseLong(json.substring(start, end));
    }

    private static String checkpointKey(String index, int shardId) {
        return index + ":" + shardId;
    }

    public static class CheckpointState {
        public final long lastSeqNo;
        public final long primaryTerm;
        public final long eventsReplayed;

        public CheckpointState(long lastSeqNo, long primaryTerm, long eventsReplayed) {
            this.lastSeqNo = lastSeqNo;
            this.primaryTerm = primaryTerm;
            this.eventsReplayed = eventsReplayed;
        }
    }
}
