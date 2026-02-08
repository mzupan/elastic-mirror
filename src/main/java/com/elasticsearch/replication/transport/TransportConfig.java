package com.elasticsearch.replication.transport;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration for the S3/SQS transport layer.
 * All settings are dynamic where possible so they can be updated without restart.
 */
public class TransportConfig {

    private static final String PREFIX = "replication.transport.";

    // S3 settings
    public static final Setting<String> S3_BUCKET = Setting.simpleString(
        PREFIX + "s3.bucket", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> S3_BASE_PATH = Setting.simpleString(
        PREFIX + "s3.base_path", "replication/", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> S3_REGION = Setting.simpleString(
        PREFIX + "s3.region", "us-east-1", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> S3_ENDPOINT = Setting.simpleString(
        PREFIX + "s3.endpoint", "", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Boolean> S3_PATH_STYLE = Setting.boolSetting(
        PREFIX + "s3.path_style_access", false, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // SQS settings
    public static final Setting<String> SQS_QUEUE_URL = Setting.simpleString(
        PREFIX + "sqs.queue_url", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> SQS_REGION = Setting.simpleString(
        PREFIX + "sqs.region", "us-east-1", Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<String> SQS_ENDPOINT = Setting.simpleString(
        PREFIX + "sqs.endpoint", "", Setting.Property.NodeScope, Setting.Property.Dynamic);

    // AWS credentials (plain settings for dev; use ES keystore for production)
    public static final Setting<String> AWS_ACCESS_KEY = Setting.simpleString(
        PREFIX + "aws.access_key", "", Setting.Property.NodeScope, Setting.Property.Filtered);

    public static final Setting<String> AWS_SECRET_KEY = Setting.simpleString(
        PREFIX + "aws.secret_key", "", Setting.Property.NodeScope, Setting.Property.Filtered);

    // IRSA / Web Identity settings (for EKS pod identity)
    public static final Setting<String> AWS_ROLE_ARN = Setting.simpleString(
        PREFIX + "aws.role_arn", "", Setting.Property.NodeScope, Setting.Property.Filtered);

    public static final Setting<String> AWS_WEB_IDENTITY_TOKEN_FILE = Setting.simpleString(
        PREFIX + "aws.web_identity_token_file", "", Setting.Property.NodeScope, Setting.Property.Filtered);

    // Compression
    public static final Setting<Boolean> COMPRESS = Setting.boolSetting(
        PREFIX + "compress", true, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(
            S3_BUCKET, S3_BASE_PATH, S3_REGION, S3_ENDPOINT, S3_PATH_STYLE,
            SQS_QUEUE_URL, SQS_REGION, SQS_ENDPOINT,
            AWS_ACCESS_KEY, AWS_SECRET_KEY,
            AWS_ROLE_ARN, AWS_WEB_IDENTITY_TOKEN_FILE,
            COMPRESS
        );
    }

    // Resolved config holder
    private final String s3Bucket;
    private final String s3BasePath;
    private final String s3Region;
    private final String s3Endpoint;
    private final boolean s3PathStyle;
    private final String sqsQueueUrl;
    private final String sqsRegion;
    private final String sqsEndpoint;
    private final boolean compress;
    private final String awsAccessKey;
    private final String awsSecretKey;
    private final String awsRoleArn;
    private final String awsWebIdentityTokenFile;

    public TransportConfig(Settings settings) {
        this.s3Bucket = S3_BUCKET.get(settings);
        this.s3BasePath = S3_BASE_PATH.get(settings);
        this.s3Region = S3_REGION.get(settings);
        this.s3Endpoint = S3_ENDPOINT.get(settings);
        this.s3PathStyle = S3_PATH_STYLE.get(settings);
        this.sqsQueueUrl = SQS_QUEUE_URL.get(settings);
        this.sqsRegion = SQS_REGION.get(settings);
        this.sqsEndpoint = SQS_ENDPOINT.get(settings);
        this.compress = COMPRESS.get(settings);
        this.awsAccessKey = AWS_ACCESS_KEY.get(settings);
        this.awsSecretKey = AWS_SECRET_KEY.get(settings);
        this.awsRoleArn = AWS_ROLE_ARN.get(settings);
        this.awsWebIdentityTokenFile = AWS_WEB_IDENTITY_TOKEN_FILE.get(settings);
    }

    public String getS3Bucket() { return s3Bucket; }
    public String getS3BasePath() { return s3BasePath; }
    public String getS3Region() { return s3Region; }
    public String getS3Endpoint() { return s3Endpoint; }
    public boolean isS3PathStyle() { return s3PathStyle; }
    public String getSqsQueueUrl() { return sqsQueueUrl; }
    public String getSqsRegion() { return sqsRegion; }
    public String getSqsEndpoint() { return sqsEndpoint; }
    public boolean isCompress() { return compress; }
    public String getAwsAccessKey() { return awsAccessKey; }
    public String getAwsSecretKey() { return awsSecretKey; }

    public String getAwsRoleArn() { return awsRoleArn; }
    public String getAwsWebIdentityTokenFile() { return awsWebIdentityTokenFile; }

    public boolean hasAwsCredentials() {
        return awsAccessKey != null && !awsAccessKey.isEmpty()
            && awsSecretKey != null && !awsSecretKey.isEmpty();
    }

    public boolean hasIrsaConfig() {
        return awsRoleArn != null && !awsRoleArn.isEmpty()
            && awsWebIdentityTokenFile != null && !awsWebIdentityTokenFile.isEmpty();
    }

    /**
     * Check if S3 endpoint override is configured (for MinIO compatibility).
     */
    public boolean hasCustomS3Endpoint() {
        return s3Endpoint != null && !s3Endpoint.isEmpty();
    }

    /**
     * Check if SQS endpoint override is configured (for LocalStack/ElasticMQ compatibility).
     */
    public boolean hasCustomSqsEndpoint() {
        return sqsEndpoint != null && !sqsEndpoint.isEmpty();
    }

    public void validate() {
        if (s3Bucket == null || s3Bucket.isEmpty()) {
            throw new IllegalArgumentException("replication.transport.s3.bucket must be configured");
        }
        if (sqsQueueUrl == null || sqsQueueUrl.isEmpty()) {
            throw new IllegalArgumentException("replication.transport.sqs.queue_url must be configured");
        }
    }
}
