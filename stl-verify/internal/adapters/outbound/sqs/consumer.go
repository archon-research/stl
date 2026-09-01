// Package sqs provides an SQS adapter for consuming messages from AWS SQS queues.
package sqs

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// sqsAPI defines the subset of SQS operations needed by the Consumer.
type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, params *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

// SQS ceilings: a per-message visibility timeout (12 hours) and
// ReceiveMessage's WaitTimeSeconds.
const (
	maxVisibilityTimeoutSeconds = 43200
	maxLongPollSeconds          = 20
)

// receiveSlack covers connection setup and teardown around one long poll.
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
	// after being received. SQS starts one clock for every message a receive
	// returns, so it must cover all of them plus the shutdown drain and settles;
	// sqsutil.ValidateVisibilityTimeout is the enforced form of that, checked at
	// boot. Defaults to 180 seconds (see ConfigDefaults).
	VisibilityTimeout int32

	// BaseEndpoint is an optional override for the SQS endpoint.
	// Used for local development with LocalStack or similar.
	BaseEndpoint string
}

// ConfigDefaults returns sensible defaults for SQS consumer configuration.
func ConfigDefaults() Config {
	return Config{
		WaitTimeSeconds: maxLongPollSeconds,
		// Covers one message per receive at the default handler budget plus the
		// shutdown drain and settles (145s); consumer_defaults_test.go pins that
		// against sqsutil.ValidateVisibilityTimeout rather than the literal.
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

	if err := validateWaitTime(sqsConfig.WaitTimeSeconds); err != nil {
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

// The long poll runs detached from ctx: aborting an in-flight receive strands
// the message SQS already assigned to it for the visibility timeout, so a
// cancelled ctx only stops the next poll from starting.
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

// noRetryForPoll gives the long poll one attempt: a retried ReceiveMessage
// waits out WaitTimeSeconds again, which pollBudget cuts off mid-flight,
// stranding a message SQS may have assigned to it. Deletes keep the retryer.
func noRetryForPoll(o *sqs.Options) { o.Retryer = aws.NopRetryer{} }

func (c *Consumer) pollBudget() time.Duration {
	return PollBudget(c.config.WaitTimeSeconds)
}

// PollBudget reports the wall time one long poll may take. Exported: it is the
// first stage of the shutdown chain derived on lifecycle.ShutdownTimeout.
func PollBudget(waitTimeSeconds int32) time.Duration {
	return time.Duration(waitTimeSeconds)*time.Second + receiveSlack
}

// validateWaitTime rejects a long-poll wait outside the range SQS accepts: out
// of range, every ReceiveMessage fails InvalidParameterValue for the life of
// the pod. Here, because most workers parse SQS_WAIT_TIME with a bare Atoi.
func validateWaitTime(waitTimeSeconds int32) error {
	if waitTimeSeconds < 0 || waitTimeSeconds > maxLongPollSeconds {
		return fmt.Errorf("sqs: WaitTimeSeconds %d is outside the range SQS accepts [0,%d]",
			waitTimeSeconds, maxLongPollSeconds)
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

func (c *Consumer) ChangeMessageVisibilityBatch(ctx context.Context, receiptHandles []string, visibility time.Duration) (map[string]error, error) {
	if len(receiptHandles) == 0 {
		return nil, nil
	}
	if len(receiptHandles) > outbound.MaxVisibilityBatchSize {
		return nil, fmt.Errorf("failed to change message visibility: %d handles exceeds the SQS batch ceiling of %d",
			len(receiptHandles), outbound.MaxVisibilityBatchSize)
	}

	result, err := c.client.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(c.queueURL),
		Entries:  visibilityBatchEntries(receiptHandles, visibility),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to change visibility of %d messages: %w", len(receiptHandles), err)
	}
	return refusalsByHandle(receiptHandles, result.Failed)
}

// Each entry's Id is its index in the request, which SQS echoes back on failure.
func visibilityBatchEntries(receiptHandles []string, visibility time.Duration) []sqstypes.ChangeMessageVisibilityBatchRequestEntry {
	entries := make([]sqstypes.ChangeMessageVisibilityBatchRequestEntry, 0, len(receiptHandles))
	for i, handle := range receiptHandles {
		entries = append(entries, sqstypes.ChangeMessageVisibilityBatchRequestEntry{
			Id:                aws.String(strconv.Itoa(i)),
			ReceiptHandle:     aws.String(handle),
			VisibilityTimeout: visibilitySeconds(visibility),
		})
	}
	return entries
}

// An Id matching no handle is reported rather than dropped: the caller counts
// releases per message, so a dropped refusal would read as a success. The
// entries SQS did name travel with it — the batch was applied either way, and
// failing them all would misreport every message it released.
func refusalsByHandle(receiptHandles []string, failed []sqstypes.BatchResultErrorEntry) (map[string]error, error) {
	if len(failed) == 0 {
		return nil, nil
	}
	refusals := make(map[string]error, len(failed))
	var unattributable []string
	for _, entry := range failed {
		index, err := strconv.Atoi(aws.ToString(entry.Id))
		if err != nil || index < 0 || index >= len(receiptHandles) {
			unattributable = append(unattributable, aws.ToString(entry.Id))
			continue
		}
		refusals[receiptHandles[index]] = fmt.Errorf("%s: %s", aws.ToString(entry.Code), aws.ToString(entry.Message))
	}
	if len(unattributable) > 0 {
		return refusals, fmt.Errorf("failed to change message visibility: SQS refused entries %v, which match no handle in the request",
			unattributable)
	}
	return refusals, nil
}

func visibilitySeconds(visibility time.Duration) int32 {
	return int32(min(max(int64(visibility.Round(time.Second)/time.Second), 0), maxVisibilityTimeoutSeconds))
}

// Close closes the consumer (no-op for SQS, but satisfies interface).
func (c *Consumer) Close() error {
	return nil
}
