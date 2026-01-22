// Package sqs provides an SQS adapter for consuming messages from AWS SQS queues.
package sqs

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// sqsAPI defines the subset of SQS operations needed by the Consumer.
type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

// Compile-time check that Consumer implements outbound.SQSConsumer
var _ outbound.SQSConsumer = (*Consumer)(nil)

// Config holds SQS consumer configuration.
type Config struct {
	// QueueURL is the URL of the SQS queue to consume from.
	QueueURL string

	// WaitTimeSeconds is how long to wait for messages (long polling).
	// Max is 20 seconds.
	WaitTimeSeconds int32
}

// ConfigDefaults returns sensible defaults for SQS consumer configuration.
func ConfigDefaults() Config {
	return Config{
		WaitTimeSeconds: 20,
	}
}

// Consumer is an SQS implementation of the outbound.SQSConsumer port.
type Consumer struct {
	client   sqsAPI
	queueURL string
	config   Config
	logger   *slog.Logger
}

// NewConsumer creates a new SQS consumer.
func NewConsumer(cfg aws.Config, sqsConfig Config, logger *slog.Logger) (*Consumer, error) {
	return NewConsumerWithOptions(cfg, sqsConfig, logger)
}

// NewConsumerWithOptions creates a new SQS consumer with optional SQS client options.
func NewConsumerWithOptions(cfg aws.Config, sqsConfig Config, logger *slog.Logger, optFns ...func(*sqs.Options)) (*Consumer, error) {
	if sqsConfig.QueueURL == "" {
		return nil, fmt.Errorf("queue URL is required")
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "sqs-consumer")

	// Apply defaults
	defaults := ConfigDefaults()
	if sqsConfig.WaitTimeSeconds == 0 {
		sqsConfig.WaitTimeSeconds = defaults.WaitTimeSeconds
	}

	return &Consumer{
		client:   sqs.NewFromConfig(cfg, optFns...),
		queueURL: sqsConfig.QueueURL,
		config:   sqsConfig,
		logger:   logger,
	}, nil
}

// ReceiveMessages fetches up to maxMessages from the queue.
func (c *Consumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	if maxMessages < 1 {
		maxMessages = 1
	}
	if maxMessages > 10 {
		maxMessages = 10 // SQS max is 10
	}

	result, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: int32(maxMessages),
		WaitTimeSeconds:     c.config.WaitTimeSeconds,
		// Request all message attributes
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to receive messages: %w", err)
	}

	messages := make([]outbound.SQSMessage, 0, len(result.Messages))
	for _, msg := range result.Messages {
		if msg.MessageId == nil || msg.ReceiptHandle == nil || msg.Body == nil {
			continue
		}
		messages = append(messages, outbound.SQSMessage{
			MessageID:     *msg.MessageId,
			ReceiptHandle: *msg.ReceiptHandle,
			Body:          *msg.Body,
		})
	}

	if len(messages) > 0 {
		c.logger.Debug("received messages", "count", len(messages))
	}

	return messages, nil
}

// DeleteMessage removes a successfully processed message from the queue.
func (c *Consumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
}

// Close closes the consumer (no-op for SQS, but satisfies interface).
func (c *Consumer) Close() error {
	return nil
}
