package com.elasticsearch.replication.cdc;

import java.io.IOException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

/**
 * Represents a single captured index or delete operation.
 * Contains all metadata needed for idempotent replay on the passive cluster.
 */
public class ChangeEvent implements Writeable, ToXContentObject {

    public enum OpType {
        INDEX,
        DELETE
    }

    private final String index;
    private final String id;
    private final String source;       // full document JSON, null for DELETE
    private final long seqNo;
    private final long primaryTerm;
    private final OpType opType;
    private final String routing;      // routing value if set, null otherwise
    private final int shardId;
    private final long timestampMs;    // capture timestamp (plugin metadata, not doc field)

    public ChangeEvent(String index, String id, String source, long seqNo,
                       long primaryTerm, OpType opType, String routing,
                       int shardId, long timestampMs) {
        this.index = index;
        this.id = id;
        this.source = source;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
        this.opType = opType;
        this.routing = routing;
        this.shardId = shardId;
        this.timestampMs = timestampMs;
    }

    public ChangeEvent(StreamInput in) throws IOException {
        this.index = in.readString();
        this.id = in.readString();
        this.source = in.readOptionalString();
        this.seqNo = in.readVLong();
        this.primaryTerm = in.readVLong();
        this.opType = in.readEnum(OpType.class);
        this.routing = in.readOptionalString();
        this.shardId = in.readVInt();
        this.timestampMs = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeString(id);
        out.writeOptionalString(source);
        out.writeVLong(seqNo);
        out.writeVLong(primaryTerm);
        out.writeEnum(opType);
        out.writeOptionalString(routing);
        out.writeVInt(shardId);
        out.writeVLong(timestampMs);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("index", index);
        builder.field("id", id);
        if (source != null) {
            builder.field("source", source);
        }
        builder.field("seq_no", seqNo);
        builder.field("primary_term", primaryTerm);
        builder.field("op_type", opType.name());
        if (routing != null) {
            builder.field("routing", routing);
        }
        builder.field("shard_id", shardId);
        builder.field("timestamp_ms", timestampMs);
        builder.endObject();
        return builder;
    }

    /**
     * Serialize to NDJSON line for batch file transport.
     */
    public String toNdJsonLine() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("{\"index\":\"").append(escapeJson(index)).append("\"");
        sb.append(",\"id\":\"").append(escapeJson(id)).append("\"");
        if (source != null) {
            // Source may contain literal newlines from pretty-printed JSON;
            // compact it since NDJSON requires one JSON object per line.
            // Literal newlines/carriage returns between JSON tokens are safe to strip;
            // newlines inside JSON string values are always escaped as \n by ES.
            sb.append(",\"source\":").append(compactJson(source));
        }
        sb.append(",\"seq_no\":").append(seqNo);
        sb.append(",\"primary_term\":").append(primaryTerm);
        sb.append(",\"op_type\":\"").append(opType.name()).append("\"");
        if (routing != null) {
            sb.append(",\"routing\":\"").append(escapeJson(routing)).append("\"");
        }
        sb.append(",\"shard_id\":").append(shardId);
        sb.append(",\"timestamp_ms\":").append(timestampMs);
        sb.append("}");
        return sb.toString();
    }

    /**
     * Remove literal whitespace between JSON tokens to ensure single-line output.
     * Only strips characters outside of JSON string values.
     */
    private static String compactJson(String json) {
        if (json == null) return "";
        StringBuilder sb = new StringBuilder(json.length());
        boolean inString = false;
        boolean escaped = false;
        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);
            if (escaped) {
                sb.append(c);
                escaped = false;
                continue;
            }
            if (c == '\\' && inString) {
                sb.append(c);
                escaped = true;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                sb.append(c);
                continue;
            }
            if (!inString && (c == '\n' || c == '\r' || c == '\t' || c == ' ')) {
                continue;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    private static String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\")
                     .replace("\"", "\\\"")
                     .replace("\n", "\\n")
                     .replace("\r", "\\r")
                     .replace("\t", "\\t");
    }

    // Getters
    public String getIndex() { return index; }
    public String getId() { return id; }
    public String getSource() { return source; }
    public long getSeqNo() { return seqNo; }
    public long getPrimaryTerm() { return primaryTerm; }
    public OpType getOpType() { return opType; }
    public String getRouting() { return routing; }
    public int getShardId() { return shardId; }
    public long getTimestampMs() { return timestampMs; }

    /**
     * Estimated byte size for batch sizing decisions.
     */
    public int estimatedSizeBytes() {
        int size = 100; // fixed overhead for metadata fields
        size += index.length();
        size += id.length();
        if (source != null) size += source.length();
        if (routing != null) size += routing.length();
        return size;
    }

    @Override
    public String toString() {
        return "ChangeEvent{" + opType + " " + index + "/" + id +
               " seq=" + seqNo + " term=" + primaryTerm +
               " shard=" + shardId + "}";
    }
}
