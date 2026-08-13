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

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestIntegration_DeadLetterPublisher_SendsToFifoQueue verifies that the
// dead-letter publisher sends a message to a real FIFO queue (via LocalStack)
// and that it can be read back with the expected body and MessageGroupId.
func TestIntegration_DeadLetterPublisher_SendsToFifoQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	_, lsCfg := testutil.StartLocalStack(t, ctx, "sqs")

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(lsCfg.Region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}

	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(lsCfg.Endpoint)
	})

	// Create a FIFO DLQ with content-based deduplication enabled, mirroring prod.
	queueResult, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("dlq-producer-test.fifo"),
		Attributes: map[string]string{
			string(sqstypes.QueueAttributeNameFifoQueue):                 "true",
			string(sqstypes.QueueAttributeNameContentBasedDeduplication): "true",
		},
	})
	if err != nil {
		t.Fatalf("failed to create FIFO queue: %v", err)
	}
	queueURL := *queueResult.QueueUrl

	publisher, err := NewDeadLetterPublisherWithOptions(awsCfg, Config{
		QueueURL: queueURL,
	}, slog.Default(), func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(lsCfg.Endpoint)
	})
	if err != nil {
		t.Fatalf("failed to create dead-letter publisher: %v", err)
	}

	const (
		body    = `{"chainId":43114,"blockNumber":123}`
		groupID = "43114"
	)

	if err := publisher.Publish(ctx, body, groupID); err != nil {
		t.Fatalf("failed to publish to DLQ: %v", err)
	}

	// Read the message back.
	received, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   1,
		WaitTimeSeconds:       5,
		AttributeNames:        []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatalf("failed to receive message from DLQ: %v", err)
	}

	if len(received.Messages) != 1 {
		t.Fatalf("expected 1 message on DLQ, got %d", len(received.Messages))
	}

	got := received.Messages[0]
	if aws.ToString(got.Body) != body {
		t.Errorf("unexpected body: got %q, want %q", aws.ToString(got.Body), body)
	}
	if gid := got.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)]; gid != groupID {
		t.Errorf("unexpected MessageGroupId: got %q, want %q", gid, groupID)
	}
}
