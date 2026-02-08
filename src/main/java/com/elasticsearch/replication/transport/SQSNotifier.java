package com.elasticsearch.replication.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleWithWebIdentityCredentialsProvider;

import java.nio.file.Paths;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.net.URI;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * SQS notifier using AWS SDK v2.
 *
 * Uses static credentials from config when a custom SQS endpoint is configured
 * (for ElasticMQ/LocalStack). Otherwise uses the default credential chain which
 * supports Pod Identity, IRSA, ECS task role, instance profile, and env vars.
 */
public class SQSNotifier {

    private static final Logger logger = LogManager.getLogger(SQSNotifier.class);

    private final SqsClient sqsClient;
    private final String queueUrl;

    @SuppressWarnings("removal")
    public SQSNotifier(TransportConfig config) {
        this.queueUrl = config.getSqsQueueUrl();

        this.sqsClient = AccessController.doPrivileged((PrivilegedAction<SqsClient>) () -> {
            S3Transport.disableAwsProfileFileLoading();

            AwsCredentialsProvider credentialsProvider;
            if (config.hasCustomSqsEndpoint() && config.hasAwsCredentials()) {
                // Local dev with ElasticMQ — use static creds from config
                credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey()));
                logger.info("SQS using static credentials (custom endpoint)");
            } else if (config.hasAwsCredentials()) {
                // Explicit creds in config
                credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getAwsAccessKey(), config.getAwsSecretKey()));
                logger.info("SQS using static credentials from config");
            } else {
                // Use shared IRSA resolver (same as S3Transport)
                credentialsProvider = S3Transport.resolveCredentials(config);
            }

            SqsClientBuilder builder = SqsClient.builder()
                .region(Region.of(config.getSqsRegion()))
                .credentialsProvider(credentialsProvider);

            if (config.hasCustomSqsEndpoint()) {
                builder.endpointOverride(URI.create(config.getSqsEndpoint()));
            }

            return builder.build();
        });

        logger.info("SQSNotifier initialized: queueUrl={}, region={}, customEndpoint={}",
            queueUrl, config.getSqsRegion(), config.hasCustomSqsEndpoint());
    }

    // Constructor for testing
    SQSNotifier(String queueUrl, SqsClient sqsClient) {
        this.queueUrl = queueUrl;
        this.sqsClient = sqsClient;
    }

    @SuppressWarnings("removal")
    public void notifyBatchUploaded(S3Transport.BatchUploadResult result) {
        String messageBody = result.toJson();

        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            SendMessageRequest req = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();
            SendMessageResponse resp = sqsClient.sendMessage(req);
            logger.info("SQS message sent: MessageId={}, s3_key={}, events={}",
                resp.messageId(), result.getS3Key(), result.getEventCount());
            return null;
        });
    }

    @SuppressWarnings("removal")
    public List<SQSMessage> pollMessages(int maxMessages, int waitTimeSeconds) {
        try {
            logger.debug("Polling SQS: queueUrl={}, maxMessages={}, waitTimeSeconds={}",
                queueUrl, maxMessages, waitTimeSeconds);
            List<SQSMessage> result = AccessController.doPrivileged((PrivilegedAction<List<SQSMessage>>) () -> {
                ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(Math.min(maxMessages, 10))
                    .waitTimeSeconds(Math.min(waitTimeSeconds, 20))
                    .visibilityTimeout(300)
                    .build();

                ReceiveMessageResponse resp = sqsClient.receiveMessage(req);
                List<SQSMessage> msgs = new ArrayList<>();
                for (Message msg : resp.messages()) {
                    msgs.add(new SQSMessage(msg.messageId(), msg.receiptHandle(), msg.body()));
                }
                return msgs;
            });
            if (!result.isEmpty()) {
                logger.info("Polled {} messages from SQS", result.size());
            }
            return result;
        } catch (Exception e) {
            logger.error("Failed to poll SQS: {} ({})", e.getMessage(), e.getClass().getName(), e);
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("removal")
    public void deleteMessage(SQSMessage message) {
        try {
            AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
                DeleteMessageRequest req = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle)
                    .build();
                sqsClient.deleteMessage(req);
                return null;
            });
        } catch (Exception e) {
            logger.error("Failed to delete SQS message: {}", e.getMessage());
        }
    }

    public void close() {
        if (sqsClient != null) {
            sqsClient.close();
        }
    }

    /**
     * Simple SQS message container (replaces AWS SDK Message class in public API).
     */
    public static class SQSMessage {
        public final String messageId;
        public final String receiptHandle;
        private final String body;

        public SQSMessage(String messageId, String receiptHandle, String body) {
            this.messageId = messageId;
            this.receiptHandle = receiptHandle;
            this.body = body;
        }

        public String body() {
            return body;
        }
    }
}
