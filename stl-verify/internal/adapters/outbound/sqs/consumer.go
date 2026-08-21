// Package sqs provides an SQS adapter for consuming messages from AWS SQS queues.
package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// sqsAPI defines the subset of SQS operations needed by the Consumer.
type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// maxVisibilityTimeoutSeconds is the SQS ceiling for a per-message visibility
// timeout (12 hours).
const maxVisibilityTimeoutSeconds = 43200

// receiveSlack is the headroom added to the configured long-poll wait when
// bounding one poll. It covers connection setup and response teardown around
// the single attempt noRetryForPoll allows, so a healthy poll is never cut
// short by its own deadline.
const receiveSlack = 5 * time.Second

// Compile-time check that Consumer implements outbound.SQSConsumer
var _ outbound.SQSConsumer = (*Consumer)(nil)

// Config holds SQS consumer configuration.
type Config struct {
	// QueueURL is the URL of the SQS queue to consume from.
	QueueURL string

	// WaitTimeSeconds is how long to wait for messages (long polling).
	// Max is 20 seconds.
	WaitTimeSeconds int32

	// VisibilityTimeout is how long a message is hidden from other consumers
	// after being received. It must exceed the worker per-message handler budget
	// so a message is not redelivered while it is still being processed.
	// Defaults to 180 seconds (see ConfigDefaults).
	VisibilityTimeout int32

	// BaseEndpoint is an optional override for the SQS endpoint.
	// Used for local development with LocalStack or similar.
	BaseEndpoint string
}

// ConfigDefaults returns sensible defaults for SQS consumer configuration.
func ConfigDefaults() Config {
	return Config{
		WaitTimeSeconds: 20,
		// Must exceed the worker per-message handler budget
		// (sqsutil.DefaultHandlerTimeout = 120s) so a message is not redelivered
		// while its handler is still running.
		VisibilityTimeout: 180,
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
	if sqsConfig.VisibilityTimeout == 0 {
		sqsConfig.VisibilityTimeout = defaults.VisibilityTimeout
	}

	if err := ValidatePollBudget(sqsConfig.WaitTimeSeconds); err != nil {
		return nil, err
	}

	// Build option functions with BaseEndpoint override if provided
	finalOptFns := optFns
	if sqsConfig.BaseEndpoint != "" {
		endpointOptFn := func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(sqsConfig.BaseEndpoint)
		}
		finalOptFns = append([]func(*sqs.Options){endpointOptFn}, optFns...)
	}

	return &Consumer{
		client:   sqs.NewFromConfig(cfg, finalOptFns...),
		queueURL: sqsConfig.QueueURL,
		config:   sqsConfig,
		logger:   logger,
	}, nil
}

// VisibilityTimeout reports the configured per-receive visibility timeout.
func (c *Consumer) VisibilityTimeout() time.Duration {
	return time.Duration(c.config.VisibilityTimeout) * time.Second
}

// ReceiveMessages fetches up to maxMessages from the queue.
//
// The long poll runs on a context detached from ctx's cancellation and
// deadline, bounded instead by the wait time plus slack: cancelling ctx
// (SIGTERM) mid-poll aborts the HTTP request, but SQS still hands the next
// message to that dead request, so its receipt handle never reaches the process
// and nothing can release it — the FIFO message group then stalls for the full
// visibility timeout. A cancelled ctx therefore only stops the *next* poll from
// starting; the caller is responsible for releasing a batch it can no longer
// process.
func (c *Consumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("not starting a poll: %w", err)
	}

	pollCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.pollBudget())
	defer cancel()

	result, err := c.client.ReceiveMessage(pollCtx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: clampBatchSize(maxMessages),
		WaitTimeSeconds:     c.config.WaitTimeSeconds,
		VisibilityTimeout:   c.config.VisibilityTimeout,
		// Request all message attributes
		MessageAttributeNames: []string{"All"},
	}, noRetryForPoll)
	if err != nil {
		return nil, fmt.Errorf("failed to receive messages: %w", err)
	}

	messages := toSQSMessages(result.Messages)
	if len(messages) > 0 {
		c.logger.Debug("received messages", "count", len(messages))
	}

	return messages, nil
}

// noRetryForPoll gives the long poll one attempt: each SDK retry waits the full
// WaitTimeSeconds again, so a retry inside pollBudget is one the budget kills
// mid-flight. The poll loop retries a failed receive on its next tick instead.
func noRetryForPoll(o *sqs.Options) { o.Retryer = aws.NopRetryer{} }

// pollBudget bounds one detached long poll, so a hung request cannot outlive
// the shutdown window it has to fit inside (see lifecycle.ShutdownTimeout).
func (c *Consumer) pollBudget() time.Duration {
	return PollBudget(c.config.WaitTimeSeconds)
}

// PollBudget reports the wall time one long poll may take: the configured SQS
// wait plus receiveSlack. Exported because it is the first stage of the
// shutdown chain derived on lifecycle.ShutdownTimeout.
func PollBudget(waitTimeSeconds int32) time.Duration {
	return time.Duration(waitTimeSeconds)*time.Second + receiveSlack
}

// ValidatePollBudget returns an error if one long poll plus the release of the
// batch it returns cannot finish inside the graceful-shutdown window. Past that
// window Stop() is abandoned mid-poll, and the message SQS handed to the
// abandoned request is stranded for the queue's visibility timeout on every
// rollout — the blackout the budget exists to prevent. WaitTimeSeconds is a
// per-worker flag/env (SQS_WAIT_TIME), so this runs at construction the way
// sqsutil.ValidateVisibilityTimeout runs at loop startup.
func ValidatePollBudget(waitTimeSeconds int32) error {
	need := PollBudget(waitTimeSeconds) + sqsutil.ShutdownCleanupTimeout
	if need >= lifecycle.ShutdownTimeout {
		return fmt.Errorf("sqs: WaitTimeSeconds %d needs %s to finish one poll and release its batch, "+
			"which does not fit the graceful-shutdown window %s", waitTimeSeconds, need, lifecycle.ShutdownTimeout)
	}
	return nil
}

func clampBatchSize(maxMessages int) int32 {
	return int32(min(max(maxMessages, 1), 10)) // SQS accepts 1..10
}

func toSQSMessages(received []sqstypes.Message) []outbound.SQSMessage {
	messages := make([]outbound.SQSMessage, 0, len(received))
	for _, msg := range received {
		if msg.MessageId == nil || msg.ReceiptHandle == nil || msg.Body == nil {
			continue
		}
		messages = append(messages, outbound.SQSMessage{
			MessageID:     *msg.MessageId,
			ReceiptHandle: *msg.ReceiptHandle,
			Body:          *msg.Body,
		})
	}
	return messages
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

// ChangeMessageVisibility resets the received message's visibility timeout,
// counted from now and rounded to whole seconds (the SQS wire unit).
func (c *Consumer) ChangeMessageVisibility(ctx context.Context, receiptHandle string, visibility time.Duration) error {
	seconds := min(max(int64(visibility.Round(time.Second)/time.Second), 0), maxVisibilityTimeoutSeconds)

	_, err := c.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(c.queueURL),
		ReceiptHandle:     aws.String(receiptHandle),
		VisibilityTimeout: int32(seconds),
	})
	if err != nil {
		return fmt.Errorf("failed to change message visibility: %w", err)
	}
	return nil
}

// Close closes the consumer (no-op for SQS, but satisfies interface).
func (c *Consumer) Close() error {
	return nil
}
