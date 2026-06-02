package sqs

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// sendMessageAPI defines the subset of SQS operations needed by the producer.
type sendMessageAPI interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

// Compile-time check that DeadLetterProducer implements outbound.DeadLetterPublisher.
var _ outbound.DeadLetterPublisher = (*DeadLetterProducer)(nil)

// DeadLetterProducer is an SQS implementation of the outbound.DeadLetterPublisher port.
// It sends failed message bodies to a dead-letter FIFO queue.
type DeadLetterProducer struct {
	client   sendMessageAPI
	queueURL string
	logger   *slog.Logger
}

// NewDeadLetterPublisher creates a new SQS dead-letter publisher.
func NewDeadLetterPublisher(cfg aws.Config, sqsConfig Config, logger *slog.Logger) (*DeadLetterProducer, error) {
	return NewDeadLetterPublisherWithOptions(cfg, sqsConfig, logger)
}

// NewDeadLetterPublisherWithOptions creates a new SQS dead-letter publisher with optional SQS client options.
func NewDeadLetterPublisherWithOptions(cfg aws.Config, sqsConfig Config, logger *slog.Logger, optFns ...func(*sqs.Options)) (*DeadLetterProducer, error) {
	if sqsConfig.QueueURL == "" {
		return nil, fmt.Errorf("queue URL is required")
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "sqs-dead-letter-publisher")

	// Build option functions with BaseEndpoint override if provided.
	finalOptFns := optFns
	if sqsConfig.BaseEndpoint != "" {
		endpointOptFn := func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(sqsConfig.BaseEndpoint)
		}
		finalOptFns = append([]func(*sqs.Options){endpointOptFn}, optFns...)
	}

	return &DeadLetterProducer{
		client:   sqs.NewFromConfig(cfg, finalOptFns...),
		queueURL: sqsConfig.QueueURL,
		logger:   logger,
	}, nil
}

// Publish sends a failed message body to the dead-letter FIFO queue.
// groupID is the FIFO MessageGroupId. The DLQ has ContentBasedDeduplication
// enabled, so no explicit MessageDeduplicationId is set.
func (p *DeadLetterProducer) Publish(ctx context.Context, body string, groupID string) error {
	_, err := p.client.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:       aws.String(p.queueURL),
		MessageBody:    aws.String(body),
		MessageGroupId: aws.String(groupID),
	})
	if err != nil {
		return fmt.Errorf("failed to send message to dead-letter queue: %w", err)
	}
	return nil
}
