// Package sns implements the EventSink interface using AWS SNS.
//
// This adapter publishes block data events to an SNS FIFO topic, where downstream
// consumers receive messages via subscribed SQS FIFO queues. Events are serialized
// as JSON messages with message attributes for filtering.
//
// Architecture:
//
// SNS FIFO Topic (single topic for all event types)
//
//	├── SQS FIFO Queue (transformer) - processes block data
//	└── SQS FIFO Queue (backup) - stores for replay
//
// FIFO Ordering & Deduplication:
//   - MessageGroupId: chainId (ensures per-chain ordering)
//   - MessageDeduplicationId: {chainId}:{blockHash}:{version}
//     This explicit dedup ID enables safe rolling deployments where two watcher
//     instances may briefly run simultaneously - SNS will deduplicate based on
//     the deterministic ID rather than message content.
//
// Message Attributes:
//   - eventType: "block"
//   - chainId: The blockchain chain ID as a string
//   - blockNumber: The block number as a string
//
// For testing, use the memory.EventSink adapter instead.
package sns

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const tracerName = "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/sns"

// Compile-time check that EventSink implements outbound.EventSink
var _ outbound.EventSink = (*EventSink)(nil)

// SNSPublisher defines the subset of SNS client methods used by EventSink.
// This interface allows for easy mocking in tests.
type SNSPublisher interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

// Config holds configuration for the SNS event sink.
type Config struct {
	// TopicARN is the ARN of the SNS FIFO topic to publish all events to.
	// All event types (blocks, receipts, traces, blobs) go to this single topic.
	TopicARN string

	// MaxRetries is the maximum number of retry attempts for transient failures.
	// Set to 0 to disable retries.
	MaxRetries int

	// InitialBackoff is the initial delay before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each retry.
	BackoffFactor float64

	// PublishTimeout is the maximum time to wait for a single publish operation.
	// If zero, defaults to 10 seconds.
	PublishTimeout time.Duration

	// Logger is the structured logger for the sink.
	Logger *slog.Logger
}

// ConfigDefaults returns a config with default values.
func ConfigDefaults() Config {
	return Config{
		MaxRetries:     3,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
		BackoffFactor:  2.0,
		PublishTimeout: 10 * time.Second,
		Logger:         slog.Default(),
	}
}

// EventSink publishes events to AWS SNS.
type EventSink struct {
	client    SNSPublisher
	config    Config
	logger    *slog.Logger
	closeOnce sync.Once
	closed    bool
	mu        sync.RWMutex
}

// NewEventSink creates a new SNS event sink.
func NewEventSink(client SNSPublisher, config Config) (*EventSink, error) {
	if client == nil {
		return nil, errors.New("sns client is required")
	}
	if config.TopicARN == "" {
		return nil, errors.New("topic ARN is required")
	}

	// Apply defaults for unset values
	defaults := ConfigDefaults()
	if config.MaxRetries == 0 {
		config.MaxRetries = defaults.MaxRetries
	}
	if config.InitialBackoff == 0 {
		config.InitialBackoff = defaults.InitialBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaults.MaxBackoff
	}
	if config.BackoffFactor == 0 {
		config.BackoffFactor = defaults.BackoffFactor
	}
	if config.PublishTimeout == 0 {
		config.PublishTimeout = defaults.PublishTimeout
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &EventSink{
		client: client,
		config: config,
		logger: config.Logger.With("component", "sns-eventsink"),
	}, nil
}

// Publish publishes an event to the SNS FIFO topic.
func (s *EventSink) Publish(ctx context.Context, event outbound.Event) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "sns.Publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "aws_sns"),
			attribute.String("messaging.destination", s.config.TopicARN),
			attribute.String("messaging.event_type", string(event.EventType())),
			attribute.Int64("block.number", event.GetBlockNumber()),
			attribute.Int64("chain.id", event.GetChainID()),
		),
	)
	defer span.End()

	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		err := errors.New("event sink is closed")
		span.RecordError(err)
		span.SetStatus(codes.Error, "event sink is closed")
		return err
	}
	s.mu.RUnlock()

	// Serialize event to JSON
	messageBytes, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to marshal event")
		return fmt.Errorf("failed to marshal event: %w", err)
	}
	message := string(messageBytes)

	// Build message attributes for filtering
	attributes := map[string]types.MessageAttributeValue{
		"eventType": {
			DataType:    aws.String("String"),
			StringValue: aws.String(string(event.EventType())),
		},
		"chainId": {
			DataType:    aws.String("Number"),
			StringValue: aws.String(strconv.FormatInt(event.GetChainID(), 10)),
		},
		"blockNumber": {
			DataType:    aws.String("Number"),
			StringValue: aws.String(strconv.FormatInt(event.GetBlockNumber(), 10)),
		},
	}

	// FIFO: Use chainId as MessageGroupId to ensure per-chain ordering
	// Use explicit MessageDeduplicationId for deterministic deduplication
	// (content-based dedup would fail due to ReceivedAt timestamp differences)
	messageGroupID := strconv.FormatInt(event.GetChainID(), 10)
	deduplicationID := event.DeduplicationID()

	input := &sns.PublishInput{
		TopicArn:               aws.String(s.config.TopicARN),
		Message:                aws.String(message),
		MessageAttributes:      attributes,
		MessageGroupId:         aws.String(messageGroupID),
		MessageDeduplicationId: aws.String(deduplicationID),
	}

	// Publish with retry logic
	err = s.publishWithRetry(ctx, input, event)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish")
	}
	return err
}

// publishWithRetry attempts to publish with exponential backoff on transient failures.
func (s *EventSink) publishWithRetry(ctx context.Context, input *sns.PublishInput, event outbound.Event) error {
	var lastErr error
	backoff := s.config.InitialBackoff

	for attempt := 0; attempt <= s.config.MaxRetries; attempt++ {
		if attempt > 0 {
			s.logger.Warn("request failed, retrying",
				"attempt", attempt,
				"maxRetries", s.config.MaxRetries,
				"backoff", backoff,
				"error", lastErr,
				"eventType", event.EventType(),
				"blockNumber", event.GetBlockNumber(),
			)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}

			// Increase backoff for next attempt
			backoff = time.Duration(float64(backoff) * s.config.BackoffFactor)
			if backoff > s.config.MaxBackoff {
				backoff = s.config.MaxBackoff
			}
		}

		// Create a timeout context for this publish attempt to prevent indefinite blocking
		publishCtx, cancel := context.WithTimeout(ctx, s.config.PublishTimeout)
		_, err := s.client.Publish(publishCtx, input)
		cancel()

		if err == nil {
			return nil
		}

		lastErr = err

		// Check if parent context was cancelled (shutdown signal)
		parentCanceled := ctx.Err() != nil

		// Check if error is retryable
		if !isRetryableError(err, parentCanceled) {
			return fmt.Errorf("failed to publish to SNS: %w", err)
		}
	}

	s.logger.Error("request failed after all retries",
		"maxRetries", s.config.MaxRetries,
		"error", lastErr,
		"eventType", event.EventType(),
		"blockNumber", event.GetBlockNumber(),
	)

	return fmt.Errorf("failed to publish to SNS after %d retries: %w", s.config.MaxRetries, lastErr)
}

// isRetryableError determines if an error should trigger a retry.
// parentCtxCanceled indicates if the parent context (not the publish timeout) was cancelled.
func isRetryableError(err error, parentCtxCanceled bool) bool {
	if err == nil {
		return false
	}

	// If parent context was cancelled (shutdown signal), don't retry
	if parentCtxCanceled {
		return false
	}

	// Publish timeout (DeadlineExceeded from our timeout context) IS retryable
	// as it indicates a transient network issue
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Parent context cancellation is not retryable
	if errors.Is(err, context.Canceled) {
		return false
	}

	// Check for specific SNS throttling errors
	var throttleErr *types.ThrottledException
	if errors.As(err, &throttleErr) {
		return true
	}

	// Check for internal errors (transient)
	var internalErr *types.InternalErrorException
	if errors.As(err, &internalErr) {
		return true
	}

	// Check for KMS throttling (if topic uses KMS encryption)
	var kmsThrottleErr *types.KMSThrottlingException
	if errors.As(err, &kmsThrottleErr) {
		return true
	}

	// Default to retrying on unknown errors (network issues, etc.)
	return true
}

// Close marks the sink as closed and prevents further publishing.
func (s *EventSink) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.logger.Info("SNS event sink closed")
	})
	return nil
}
