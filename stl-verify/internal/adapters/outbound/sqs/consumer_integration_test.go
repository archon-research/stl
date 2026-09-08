//go:build integration

package sqs

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Only the wire proves SQS fills the count in for the attribute the receive
// asks for; the unit tests can only assert the request.
func TestIntegration_Consumer_ReceiveMessages_CountsEveryDelivery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	awsCfg := localStackAWSConfig(t, ctx)
	sqsClient := sqs.NewFromConfig(awsCfg, localStackEndpoint)
	queueURL := createFifoQueue(t, ctx, sqsClient, "consumer-receive-count-")
	sendBlockEvent(t, ctx, sqsClient, queueURL)

	consumer, err := NewConsumerWithOptions(awsCfg, Config{
		QueueURL:          queueURL,
		WaitTimeSeconds:   5,
		VisibilityTimeout: 30,
	}, slog.Default(), localStackEndpoint)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	first := receiveOne(t, ctx, consumer)
	if first.ReceiveCount != 1 {
		t.Errorf("first delivery reports ReceiveCount %d, want 1", first.ReceiveCount)
	}

	if _, err := consumer.ChangeMessageVisibilityBatch(ctx, []string{first.ReceiptHandle}, 0); err != nil {
		t.Fatalf("failed to release the message: %v", err)
	}

	second := receiveOne(t, ctx, consumer)
	if second.MessageID != first.MessageID {
		t.Fatalf("expected the released message redelivered, got %s after %s", second.MessageID, first.MessageID)
	}
	if second.ReceiveCount != 2 {
		t.Errorf("second delivery reports ReceiveCount %d, want 2", second.ReceiveCount)
	}
}

func localStackEndpoint(o *sqs.Options) {
	o.BaseEndpoint = aws.String(sharedLocalStackCfg.Endpoint)
}

func localStackAWSConfig(t *testing.T, ctx context.Context) aws.Config {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(sharedLocalStackCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}
	return awsCfg
}

func createFifoQueue(t *testing.T, ctx context.Context, client *sqs.Client, prefix string) string {
	t.Helper()
	created, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(testutil.SQSTestFifoQueueName(t, prefix)),
		Attributes: map[string]string{
			string(sqstypes.QueueAttributeNameFifoQueue):                 "true",
			string(sqstypes.QueueAttributeNameContentBasedDeduplication): "true",
		},
	})
	if err != nil {
		t.Fatalf("failed to create FIFO queue: %v", err)
	}
	return aws.ToString(created.QueueUrl)
}

func sendBlockEvent(t *testing.T, ctx context.Context, client *sqs.Client, queueURL string) {
	t.Helper()
	_, err := client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       aws.String(queueURL),
		MessageBody:    aws.String(`{"chainId":1,"blockNumber":100,"version":0,"blockHash":"0xabc"}`),
		MessageGroupId: aws.String("1"),
	})
	if err != nil {
		t.Fatalf("failed to send message: %v", err)
	}
}

func receiveOne(t *testing.T, ctx context.Context, consumer *Consumer) outbound.SQSMessage {
	t.Helper()
	messages, err := consumer.ReceiveMessages(ctx, 1)
	if err != nil {
		t.Fatalf("failed to receive: %v", err)
	}
	if len(messages) != 1 {
		t.Fatalf("expected one message, got %d", len(messages))
	}
	return messages[0]
}
