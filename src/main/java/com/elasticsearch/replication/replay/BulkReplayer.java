package com.elasticsearch.replication.replay;

import com.elasticsearch.replication.cdc.ChangeEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Replays ChangeEvents to the passive cluster using the Bulk API via low-level RestClient.
 *
 * Uses external versioning (seq_no as version) to make replay idempotent.
 * If the same event is replayed twice, the second attempt is a no-op because
 * the version is already at or past the seq_no.
 */
public class BulkReplayer {

    private static final Logger logger = LogManager.getLogger(BulkReplayer.class);

    private final RestClient client;
    private final int maxBulkSize;

    // Metrics
    private final AtomicLong totalReplayed = new AtomicLong(0);
    private final AtomicLong totalFailed = new AtomicLong(0);
    private final AtomicLong totalSkippedVersion = new AtomicLong(0);

    public BulkReplayer(RestClient client, int maxBulkSize) {
        this.client = client;
        this.maxBulkSize = maxBulkSize;
    }

    public ReplayResult replay(List<ChangeEvent> events) throws IOException {
        int succeeded = 0;
        int failed = 0;
        int versionConflicts = 0;
        long maxSeqNo = -1;
        long maxPrimaryTerm = 0;

        for (int i = 0; i < events.size(); i += maxBulkSize) {
            int end = Math.min(i + maxBulkSize, events.size());
            List<ChangeEvent> subBatch = events.subList(i, end);

            // Build NDJSON bulk body
            StringBuilder bulkBody = new StringBuilder(subBatch.size() * 512);
            for (ChangeEvent event : subBatch) {
                if (event.getOpType() == ChangeEvent.OpType.INDEX) {
                    bulkBody.append("{\"index\":{\"_index\":\"").append(escapeJson(event.getIndex()))
                            .append("\",\"_id\":\"").append(escapeJson(event.getId()))
                            .append("\",\"version\":").append(event.getSeqNo() + 1)
                            .append(",\"version_type\":\"external\"");
                    if (event.getRouting() != null) {
                        bulkBody.append(",\"routing\":\"").append(escapeJson(event.getRouting())).append("\"");
                    }
                    bulkBody.append("}}\n");
                    bulkBody.append(event.getSource()).append("\n");
                } else if (event.getOpType() == ChangeEvent.OpType.DELETE) {
                    bulkBody.append("{\"delete\":{\"_index\":\"").append(escapeJson(event.getIndex()))
                            .append("\",\"_id\":\"").append(escapeJson(event.getId()))
                            .append("\",\"version\":").append(event.getSeqNo() + 1)
                            .append(",\"version_type\":\"external\"");
                    if (event.getRouting() != null) {
                        bulkBody.append(",\"routing\":\"").append(escapeJson(event.getRouting())).append("\"");
                    }
                    bulkBody.append("}}\n");
                }

                if (event.getSeqNo() > maxSeqNo) {
                    maxSeqNo = event.getSeqNo();
                    maxPrimaryTerm = event.getPrimaryTerm();
                }
            }

            // Send bulk request
            Request request = new Request("POST", "/_bulk");
            request.setJsonEntity(bulkBody.toString());
            Response response = client.performRequest(request);

            // Parse response to count successes/failures
            String responseBody = readResponseBody(response);
            // Check if errors occurred
            if (responseBody.contains("\"errors\":true")) {
                // Parse individual items
                int[] counts = parseBulkResponse(responseBody);
                succeeded += counts[0];
                failed += counts[1];
                versionConflicts += counts[2];
            } else {
                succeeded += subBatch.size();
            }
        }

        totalReplayed.addAndGet(succeeded);
        totalFailed.addAndGet(failed);
        totalSkippedVersion.addAndGet(versionConflicts);

        logger.info("Replay complete: {} succeeded, {} version-skipped, {} failed",
                     succeeded, versionConflicts, failed);

        return new ReplayResult(succeeded, failed, versionConflicts, maxSeqNo, maxPrimaryTerm);
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

    /**
     * Parse bulk response JSON to count successes, failures, and version conflicts.
     * Returns [succeeded, failed, versionConflicts].
     */
    private int[] parseBulkResponse(String json) {
        int succeeded = 0;
        int failed = 0;
        int versionConflicts = 0;

        // Find each item result
        int pos = 0;
        while (true) {
            int statusIdx = json.indexOf("\"status\":", pos);
            if (statusIdx == -1) break;
            int statusStart = statusIdx + 9;
            int statusEnd = statusStart;
            while (statusEnd < json.length() && Character.isDigit(json.charAt(statusEnd))) statusEnd++;
            int status = Integer.parseInt(json.substring(statusStart, statusEnd));

            if (status >= 200 && status < 300) {
                succeeded++;
            } else if (status == 409) {
                versionConflicts++; // version conflict = already replayed
            } else {
                failed++;
            }
            pos = statusEnd;
        }

        return new int[]{succeeded, failed, versionConflicts};
    }

    /**
     * Parse NDJSON content back into ChangeEvents for replay.
     * Throws on any parse failure so the caller can leave the SQS message
     * visible for retry rather than silently losing records.
     */
    public static List<ChangeEvent> parseNdJson(String ndjsonContent) {
        List<ChangeEvent> events = new ArrayList<>();
        String[] lines = ndjsonContent.split("\n");
        int parseErrors = 0;
        String firstError = null;

        for (String line : lines) {
            if (line.trim().isEmpty()) continue;
            try {
                events.add(parseEventLine(line));
            } catch (Exception e) {
                parseErrors++;
                if (firstError == null) {
                    firstError = e.getMessage();
                }
                logger.error("Failed to parse NDJSON line (error {}): {} | line: {}",
                             parseErrors, e.getMessage(),
                             line.length() > 200 ? line.substring(0, 200) + "..." : line);
            }
        }
        if (parseErrors > 0) {
            throw new IllegalStateException(
                "Failed to parse " + parseErrors + " of " + (events.size() + parseErrors) +
                " NDJSON lines. First error: " + firstError);
        }
        return events;
    }

    private static ChangeEvent parseEventLine(String json) {
        // Extract source object first, then remove it to prevent
        // field-name collisions between source content and metadata fields
        String source = extractObjectField(json, "source");
        String metaJson = json;
        if (source != null) {
            int srcKeyIdx = json.indexOf("\"source\":");
            if (srcKeyIdx >= 0) {
                int srcStart = srcKeyIdx;
                int srcEnd = srcKeyIdx + "\"source\":".length() + source.length();
                // Also remove any trailing comma
                if (srcEnd < json.length() && json.charAt(srcEnd) == ',') {
                    srcEnd++;
                }
                metaJson = json.substring(0, srcStart) + json.substring(srcEnd);
            }
        }

        String index = extractStringField(metaJson, "index");
        String id = extractStringField(metaJson, "id");
        long seqNo = extractLongField(metaJson, "seq_no");
        long primaryTerm = extractLongField(metaJson, "primary_term");
        String opTypeStr = extractStringField(metaJson, "op_type");
        String routing = extractStringField(metaJson, "routing");
        int shardId = (int) extractLongField(metaJson, "shard_id");
        long timestampMs = extractLongField(metaJson, "timestamp_ms");

        if (opTypeStr == null) {
            throw new IllegalArgumentException("Missing op_type in NDJSON line (length=" + json.length() + ")");
        }
        ChangeEvent.OpType opType = ChangeEvent.OpType.valueOf(opTypeStr);

        return new ChangeEvent(index, id, source, seqNo, primaryTerm,
                               opType, routing, shardId, timestampMs);
    }

    private static String extractStringField(String json, String field) {
        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);
        if (start == -1) return null;
        start += key.length();
        // Scan for the closing quote, skipping escaped quotes
        boolean escaped = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = true;
                continue;
            }
            if (c == '"') {
                return unescapeJson(json.substring(start, i));
            }
        }
        return null;
    }

    private static long extractLongField(String json, String field) {
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

    private static String extractObjectField(String json, String field) {
        String key = "\"" + field + "\":";
        int start = json.indexOf(key);
        if (start == -1) return null;
        start += key.length();
        if (start >= json.length()) return null;
        char firstChar = json.charAt(start);
        if (firstChar != '{') return null;

        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaped) { escaped = false; continue; }
            if (c == '\\') { escaped = true; continue; }
            if (c == '"') { inString = !inString; continue; }
            if (!inString) {
                if (c == '{') depth++;
                if (c == '}') {
                    depth--;
                    if (depth == 0) return json.substring(start, i + 1);
                }
            }
        }
        return null;
    }

    private static String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                     .replace("\"", "\\\"")
                     .replace("\n", "\\n")
                     .replace("\r", "\\r")
                     .replace("\t", "\\t");
    }

    private static String unescapeJson(String value) {
        if (value == null) return null;
        return value.replace("\\\"", "\"")
                     .replace("\\\\", "\\")
                     .replace("\\n", "\n")
                     .replace("\\r", "\r")
                     .replace("\\t", "\t");
    }

    public long getTotalReplayed() { return totalReplayed.get(); }
    public long getTotalFailed() { return totalFailed.get(); }
    public long getTotalSkippedVersion() { return totalSkippedVersion.get(); }

    public static class ReplayResult {
        public final int succeeded;
        public final int failed;
        public final int versionConflicts;
        public final long maxSeqNo;
        public final long maxPrimaryTerm;

        public ReplayResult(int succeeded, int failed, int versionConflicts,
                            long maxSeqNo, long maxPrimaryTerm) {
            this.succeeded = succeeded;
            this.failed = failed;
            this.versionConflicts = versionConflicts;
            this.maxSeqNo = maxSeqNo;
            this.maxPrimaryTerm = maxPrimaryTerm;
        }
    }
}
