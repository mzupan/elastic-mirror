package com.elasticsearch.replication.transport;

import com.elasticsearch.replication.cdc.ChangeEvent;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Writes batches of ChangeEvents to S3 as NDJSON files.
 *
 * S3 key format: {basePath}/{index}/{shardId}/{primaryTerm}_{fromSeq}_{toSeq}.ndjson[.gz]
 *
 * Supports MinIO via custom endpoint + path-style access.
 */
public class S3Transport {

    private static final Logger logger = LogManager.getLogger(S3Transport.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String basePath;
    private final boolean compress;

    @SuppressWarnings("removal")
    public S3Transport(TransportConfig config) {
        this.bucket = config.getS3Bucket();
        this.basePath = normalizeBasePath(config.getS3BasePath());
        this.compress = config.isCompress();

        // Resolve credentials from ES settings (avoids System.getenv which is blocked by security manager)
        AwsCredentialsProvider credentialsProvider = resolveCredentials(config);

        // Wrap in doPrivileged — required for ES plugin security manager
        this.s3Client = AccessController.doPrivileged((PrivilegedAction<S3Client>) () -> {
            // Prevent AWS SDK from trying to read ~/.aws/credentials and ~/.aws/config
            // We have PropertyPermission but not FilePermission for those paths
            disableAwsProfileFileLoading();

            S3ClientBuilder builder = S3Client.builder()
                .region(Region.of(config.getS3Region()))
                .credentialsProvider(credentialsProvider);

            if (config.hasCustomS3Endpoint()) {
                builder.endpointOverride(URI.create(config.getS3Endpoint()));
            }
            if (config.isS3PathStyle()) {
                builder.forcePathStyle(true);
            }

            return builder.build();
        });
    }

    /**
     * Prevent AWS SDK v2 from probing ~/.aws/credentials and ~/.aws/config.
     * ES plugin security manager blocks FilePermission on those paths.
     * We redirect the SDK to /dev/null so it reads an empty file instead.
     */
    static void disableAwsProfileFileLoading() {
        System.setProperty("aws.sharedCredentialsFile", "/dev/null");
        System.setProperty("aws.configFile", "/dev/null");
    }

    private static AwsCredentialsProvider resolveCredentials(TransportConfig config) {
        if (config.hasAwsCredentials()) {
            return StaticCredentialsProvider.create(
                AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey()));
        }
        throw new IllegalArgumentException(
            "AWS credentials must be configured via replication.transport.aws.access_key and aws.secret_key");
    }

    // Constructor for testing with pre-built client
    S3Transport(S3Client s3Client, String bucket, String basePath, boolean compress) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.basePath = normalizeBasePath(basePath);
        this.compress = compress;
    }

    /**
     * Upload a batch of events to S3. Returns the S3 key.
     *
     * Groups events by index+shard for organized storage, but accepts
     * a mixed batch for simplicity — the key uses the first event's metadata.
     */
    public BatchUploadResult uploadBatch(List<ChangeEvent> events) throws IOException {
        if (events.isEmpty()) {
            throw new IllegalArgumentException("Cannot upload empty batch");
        }

        // Compute sequence range across all events
        long minSeq = Long.MAX_VALUE;
        long maxSeq = Long.MIN_VALUE;
        long primaryTerm = 0;
        String index = events.get(0).getIndex();
        int shardId = events.get(0).getShardId();

        StringBuilder ndjson = new StringBuilder(events.size() * 256);
        for (ChangeEvent event : events) {
            if (event.getSeqNo() < minSeq) minSeq = event.getSeqNo();
            if (event.getSeqNo() > maxSeq) maxSeq = event.getSeqNo();
            if (event.getPrimaryTerm() > primaryTerm) primaryTerm = event.getPrimaryTerm();
            ndjson.append(event.toNdJsonLine()).append('\n');
        }

        // Build S3 key
        String suffix = compress ? ".ndjson.gz" : ".ndjson";
        String key = basePath + index + "/" + shardId + "/" +
                     primaryTerm + "_" + minSeq + "_" + maxSeq + suffix;

        byte[] payload = preparePayload(ndjson.toString());

        String contentType = compress ? "application/gzip" : "application/x-ndjson";
        PutObjectRequest putReq = PutObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .contentType(contentType)
            .contentLength((long) payload.length)
            .build();

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            s3Client.putObject(putReq, RequestBody.fromBytes(payload));
            return null;
        });

        logger.info("Uploaded batch to s3://{}/{}: {} events, {} bytes (compressed={})",
                     bucket, key, events.size(), payload.length, compress);

        return new BatchUploadResult(key, events.size(), minSeq, maxSeq, primaryTerm,
                                     index, shardId, payload.length);
    }

    /**
     * Download and parse a batch from S3 by key.
     */
    public String downloadBatch(String key) throws IOException {
        GetObjectRequest getReq = GetObjectRequest.builder()
            .bucket(bucket)
            .key(key)
            .build();

        byte[] raw = AccessController.doPrivileged((PrivilegedAction<byte[]>) () ->
            s3Client.getObjectAsBytes(getReq).asByteArray()
        );

        if (key.endsWith(".gz")) {
            try (GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(raw));
                 BufferedReader reader = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8))) {
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line).append('\n');
                }
                return sb.toString();
            }
        }

        return new String(raw, StandardCharsets.UTF_8);
    }

    private byte[] preparePayload(String ndjson) throws IOException {
        byte[] raw = ndjson.getBytes(StandardCharsets.UTF_8);
        if (!compress) {
            return raw;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream(raw.length / 4);
        try (GZIPOutputStream gos = new GZIPOutputStream(baos)) {
            gos.write(raw);
        }
        return baos.toByteArray();
    }

    private static String normalizeBasePath(String basePath) {
        if (basePath == null || basePath.isEmpty()) return "";
        if (!basePath.endsWith("/")) basePath += "/";
        if (basePath.startsWith("/")) basePath = basePath.substring(1);
        return basePath;
    }

    public void close() {
        s3Client.close();
    }

    /**
     * Result of a batch upload, containing metadata for SQS notification.
     */
    public static class BatchUploadResult {
        private final String s3Key;
        private final int eventCount;
        private final long fromSeqNo;
        private final long toSeqNo;
        private final long primaryTerm;
        private final String index;
        private final int shardId;
        private final long payloadBytes;

        public BatchUploadResult(String s3Key, int eventCount, long fromSeqNo, long toSeqNo,
                                 long primaryTerm, String index, int shardId, long payloadBytes) {
            this.s3Key = s3Key;
            this.eventCount = eventCount;
            this.fromSeqNo = fromSeqNo;
            this.toSeqNo = toSeqNo;
            this.primaryTerm = primaryTerm;
            this.index = index;
            this.shardId = shardId;
            this.payloadBytes = payloadBytes;
        }

        public String getS3Key() { return s3Key; }
        public int getEventCount() { return eventCount; }
        public long getFromSeqNo() { return fromSeqNo; }
        public long getToSeqNo() { return toSeqNo; }
        public long getPrimaryTerm() { return primaryTerm; }
        public String getIndex() { return index; }
        public int getShardId() { return shardId; }
        public long getPayloadBytes() { return payloadBytes; }

        /**
         * Serialize to JSON for SQS message body.
         */
        public String toJson() {
            return "{\"s3_key\":\"" + s3Key + "\"" +
                   ",\"event_count\":" + eventCount +
                   ",\"from_seq_no\":" + fromSeqNo +
                   ",\"to_seq_no\":" + toSeqNo +
                   ",\"primary_term\":" + primaryTerm +
                   ",\"index\":\"" + index + "\"" +
                   ",\"shard_id\":" + shardId +
                   ",\"payload_bytes\":" + payloadBytes + "}";
        }
    }
}
