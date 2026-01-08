// Package sns implements the EventSink interface using AWS SNS.
//
// This adapter publishes block data events to an SNS topic, where downstream
// consumers can subscribe to receive notifications when new block data is
// available in the cache. Events are serialized as JSON messages.
//
// Features:
//   - Automatic retry logic with exponential backoff for transient failures
//   - Configurable timeouts and backoff parameters
//   - Message attributes for filtering by event type, chain ID, and block number
//   - Graceful shutdown with context cancellation
//
// Message Attributes:
//   - eventType: "block", "receipts", "traces", or "blobs"
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

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that EventSink implements outbound.EventSink
var _ outbound.EventSink = (*EventSink)(nil)

// SNSPublisher defines the subset of SNS client methods used by EventSink.
// This interface allows for easy mocking in tests.
type SNSPublisher interface {
	Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
}

// TopicARNs holds the ARNs of the SNS topics to publish events to.
// Each event type is published to its own topic.
type TopicARNs struct {
	// Blocks is the ARN for block events.
	Blocks string
	// Receipts is the ARN for receipts events.
	Receipts string
	// Traces is the ARN for traces events.
	Traces string
	// Blobs is the ARN for blob events.
	Blobs string
}

// Config holds configuration for the SNS event sink.
type Config struct {
	// Topics contains the ARNs of the SNS topics for each event type.
	Topics TopicARNs

	// MaxRetries is the maximum number of retry attempts for transient failures.
	// Set to 0 to disable retries.
	MaxRetries int

	// InitialBackoff is the initial delay before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each retry.
	BackoffFactor float64

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
	if config.Topics.Blocks == "" {
		return nil, errors.New("blocks topic ARN is required")
	}
	if config.Topics.Receipts == "" {
		return nil, errors.New("receipts topic ARN is required")
	}
	if config.Topics.Traces == "" {
		return nil, errors.New("traces topic ARN is required")
	}
	if config.Topics.Blobs == "" {
		return nil, errors.New("blobs topic ARN is required")
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
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &EventSink{
		client: client,
		config: config,
		logger: config.Logger.With("component", "sns-eventsink"),
	}, nil
}

// Publish publishes an event to SNS.
func (s *EventSink) Publish(ctx context.Context, event outbound.Event) error {
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return errors.New("event sink is closed")
	}
	s.mu.RUnlock()

	// Get the topic ARN for this event type
	topicARN := s.getTopicARN(event.EventType())
	if topicARN == "" {
		return fmt.Errorf("no topic ARN configured for event type: %s", event.EventType())
	}

	// Serialize event to JSON
	messageBytes, err := json.Marshal(event)
	if err != nil {
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

	input := &sns.PublishInput{
		TopicArn:          aws.String(topicARN),
		Message:           aws.String(message),
		MessageAttributes: attributes,
	}

	// Publish with retry logic
	return s.publishWithRetry(ctx, input, event)
}

// getTopicARN returns the topic ARN for the given event type.
func (s *EventSink) getTopicARN(eventType outbound.EventType) string {
	switch eventType {
	case outbound.EventTypeBlock:
		return s.config.Topics.Blocks
	case outbound.EventTypeReceipts:
		return s.config.Topics.Receipts
	case outbound.EventTypeTraces:
		return s.config.Topics.Traces
	case outbound.EventTypeBlobs:
		return s.config.Topics.Blobs
	default:
		return ""
	}
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

		_, err := s.client.Publish(ctx, input)
		if err == nil {
			return nil
		}

		lastErr = err

		// Check if error is retryable
		if !isRetryableError(err) {
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
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Context errors are not retryable
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
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
