package sns

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockSNSClient implements SNSPublisher for testing.
type mockSNSClient struct {
	publishFunc func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error)
	calls       []*sns.PublishInput
}

func (m *mockSNSClient) Publish(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
	m.calls = append(m.calls, params)
	if m.publishFunc != nil {
		return m.publishFunc(ctx, params, optFns...)
	}
	return &sns.PublishOutput{
		MessageId: aws.String("test-message-id"),
	}, nil
}

// testTopicARN returns a test topic ARN.
const testTopicARN = "arn:aws:sns:us-east-1:123456789:ethereum-blocks.fifo"

func TestNewEventSink_RequiresClient(t *testing.T) {
	_, err := NewEventSink(nil, Config{TopicARN: testTopicARN})
	if err == nil {
		t.Error("expected error for nil client")
	}
	if err.Error() != "sns client is required" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewEventSink_RequiresTopicARN(t *testing.T) {
	_, err := NewEventSink(&mockSNSClient{}, Config{TopicARN: ""})
	if err == nil {
		t.Error("expected error for missing topic ARN")
	}
	if err.Error() != "topic ARN is required" {
		t.Errorf("expected error %q, got %q", "topic ARN is required", err.Error())
	}
}

func TestNewEventSink_AppliesDefaults(t *testing.T) {
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		TopicARN: testTopicARN,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sink.config.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", sink.config.MaxRetries)
	}
	if sink.config.InitialBackoff != 100*time.Millisecond {
		t.Errorf("expected InitialBackoff=100ms, got %v", sink.config.InitialBackoff)
	}
	if sink.config.MaxBackoff != 5*time.Second {
		t.Errorf("expected MaxBackoff=5s, got %v", sink.config.MaxBackoff)
	}
	if sink.config.BackoffFactor != 2.0 {
		t.Errorf("expected BackoffFactor=2.0, got %v", sink.config.BackoffFactor)
	}
}

func TestPublish_Success(t *testing.T) {
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		TopicARN: testTopicARN,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{
		ChainID:     1,
		BlockNumber: 12345,
		BlockHash:   "0xabc123",
		ParentHash:  "0xdef456",
		ReceivedAt:  time.Now(),
	}

	err = sink.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(client.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(client.calls))
	}

	call := client.calls[0]
	if *call.TopicArn != testTopicARN {
		t.Errorf("unexpected topic ARN: %s, expected %s", *call.TopicArn, testTopicARN)
	}

	// Verify FIFO attributes
	if call.MessageGroupId == nil || *call.MessageGroupId != "1" {
		t.Errorf("expected MessageGroupId=1 (chainId), got %v", call.MessageGroupId)
	}

	// Verify message is valid JSON
	var decoded outbound.BlockEvent
	if err := json.Unmarshal([]byte(*call.Message), &decoded); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}
	if decoded.BlockNumber != 12345 {
		t.Errorf("expected block number 12345, got %d", decoded.BlockNumber)
	}

	// Verify message attributes
	if call.MessageAttributes["eventType"].StringValue == nil ||
		*call.MessageAttributes["eventType"].StringValue != "block" {
		t.Error("missing or incorrect eventType attribute")
	}
	if call.MessageAttributes["chainId"].StringValue == nil ||
		*call.MessageAttributes["chainId"].StringValue != "1" {
		t.Error("missing or incorrect chainId attribute")
	}
	if call.MessageAttributes["blockNumber"].StringValue == nil ||
		*call.MessageAttributes["blockNumber"].StringValue != "12345" {
		t.Error("missing or incorrect blockNumber attribute")
	}
}

func TestPublish_RetryOnThrottling(t *testing.T) {
	callCount := 0
	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			callCount++
			if callCount < 3 {
				return nil, &types.ThrottledException{Message: aws.String("throttled")}
			}
			return &sns.PublishOutput{MessageId: aws.String("success")}, nil
		},
	}

	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}

	if callCount != 3 {
		t.Errorf("expected 3 calls, got %d", callCount)
	}
}

func TestPublish_RetriesExhausted(t *testing.T) {
	callCount := 0
	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			callCount++
			return nil, &types.ThrottledException{Message: aws.String("throttled")}
		},
	}

	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     2,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(context.Background(), event)
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}

	// Initial attempt + 2 retries = 3 calls
	if callCount != 3 {
		t.Errorf("expected 3 calls (1 + 2 retries), got %d", callCount)
	}
}

func TestPublish_ContextCancelled(t *testing.T) {
	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			return nil, &types.ThrottledException{Message: aws.String("throttled")}
		},
	}

	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     10,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(ctx, event)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestPublish_AfterClose(t *testing.T) {
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		TopicARN: testTopicARN,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = sink.Close()
	if err != nil {
		t.Fatalf("unexpected error on close: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(context.Background(), event)
	if err == nil {
		t.Error("expected error when publishing after close")
	}
	if err.Error() != "event sink is closed" {
		t.Errorf("unexpected error: %v", err)
	}

	if len(client.calls) != 0 {
		t.Errorf("expected 0 calls after close, got %d", len(client.calls))
	}
}

func TestClose_Idempotent(t *testing.T) {
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		TopicARN: testTopicARN,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Close multiple times should not panic
	for i := 0; i < 3; i++ {
		err = sink.Close()
		if err != nil {
			t.Fatalf("unexpected error on close %d: %v", i, err)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	defaults := ConfigDefaults()

	if defaults.MaxRetries != 3 {
		t.Errorf("expected MaxRetries=3, got %d", defaults.MaxRetries)
	}
	if defaults.InitialBackoff != 100*time.Millisecond {
		t.Errorf("expected InitialBackoff=100ms, got %v", defaults.InitialBackoff)
	}
	if defaults.MaxBackoff != 5*time.Second {
		t.Errorf("expected MaxBackoff=5s, got %v", defaults.MaxBackoff)
	}
	if defaults.BackoffFactor != 2.0 {
		t.Errorf("expected BackoffFactor=2.0, got %v", defaults.BackoffFactor)
	}
	if defaults.Logger == nil {
		t.Error("expected non-nil default logger")
	}
}

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		parentCanceled bool
		retryable      bool
	}{
		{
			name:           "nil error",
			err:            nil,
			parentCanceled: false,
			retryable:      false,
		},
		{
			name:           "context cancelled",
			err:            context.Canceled,
			parentCanceled: false,
			retryable:      false,
		},
		{
			name:           "context deadline exceeded - publish timeout",
			err:            context.DeadlineExceeded,
			parentCanceled: false,
			retryable:      true, // Publish timeout IS retryable
		},
		{
			name:           "context deadline exceeded - parent cancelled",
			err:            context.DeadlineExceeded,
			parentCanceled: true,
			retryable:      false, // Parent cancelled is NOT retryable
		},
		{
			name:           "throttle exception",
			err:            &types.ThrottledException{Message: aws.String("throttled")},
			parentCanceled: false,
			retryable:      true,
		},
		{
			name:           "internal error",
			err:            &types.InternalErrorException{Message: aws.String("internal")},
			parentCanceled: false,
			retryable:      true,
		},
		{
			name:           "KMS throttling",
			err:            &types.KMSThrottlingException{Message: aws.String("kms throttled")},
			parentCanceled: false,
			retryable:      true,
		},
		{
			name:           "generic error",
			err:            errors.New("some error"),
			parentCanceled: false,
			retryable:      true,
		},
		{
			name:           "generic error - but parent cancelled",
			err:            errors.New("some error"),
			parentCanceled: true,
			retryable:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err, tt.parentCanceled)
			if result != tt.retryable {
				t.Errorf("expected isRetryableError=%v, got %v", tt.retryable, result)
			}
		})
	}
}

func TestPublish_RetryOnInternalError(t *testing.T) {
	callCount := 0
	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			callCount++
			if callCount < 2 {
				return nil, &types.InternalErrorException{Message: aws.String("internal error")}
			}
			return &sns.PublishOutput{MessageId: aws.String("success")}, nil
		},
	}

	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}

	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestPublish_MessageContainsAllFields(t *testing.T) {
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		TopicARN: testTopicARN,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Now().Truncate(time.Second)
	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    12345,
		Version:        2,
		BlockHash:      "0xabc123",
		ParentHash:     "0xdef456",
		BlockTimestamp: 1704067200,
		ReceivedAt:     now,
		IsReorg:        true,
		IsBackfill:     false,
	}

	err = sink.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	call := client.calls[0]
	var decoded outbound.BlockEvent
	if err := json.Unmarshal([]byte(*call.Message), &decoded); err != nil {
		t.Fatalf("failed to unmarshal message: %v", err)
	}

	if decoded.ChainID != 1 {
		t.Errorf("expected ChainID=1, got %d", decoded.ChainID)
	}
	if decoded.BlockNumber != 12345 {
		t.Errorf("expected BlockNumber=12345, got %d", decoded.BlockNumber)
	}
	if decoded.Version != 2 {
		t.Errorf("expected Version=2, got %d", decoded.Version)
	}
	if decoded.BlockHash != "0xabc123" {
		t.Errorf("expected BlockHash=0xabc123, got %s", decoded.BlockHash)
	}
	if decoded.ParentHash != "0xdef456" {
		t.Errorf("expected ParentHash=0xdef456, got %s", decoded.ParentHash)
	}
	if decoded.BlockTimestamp != 1704067200 {
		t.Errorf("expected BlockTimestamp=1704067200, got %d", decoded.BlockTimestamp)
	}
	if !decoded.IsReorg {
		t.Error("expected IsReorg=true")
	}
	if decoded.IsBackfill {
		t.Error("expected IsBackfill=false")
	}
}

// unknownEvent is a mock event with an unknown event type for testing.
// With a single topic, all events go to the same topic regardless of type.
type unknownEvent struct{}

func (e unknownEvent) EventType() outbound.EventType { return "unknown" }
func (e unknownEvent) GetBlockNumber() int64         { return 100 }
func (e unknownEvent) GetChainID() int64             { return 1 }

func TestPublish_UnknownEventType(t *testing.T) {
	// With single topic architecture, all events (including unknown types) are published
	// to the same topic. The eventType is just a message attribute for filtering.
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		TopicARN: testTopicARN,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Unknown event types should still publish successfully
	err = sink.Publish(context.Background(), unknownEvent{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(client.calls) != 1 {
		t.Errorf("expected 1 SNS call, got %d", len(client.calls))
	}

	// Verify the event type attribute is set
	if client.calls[0].MessageAttributes["eventType"].StringValue == nil ||
		*client.calls[0].MessageAttributes["eventType"].StringValue != "unknown" {
		t.Error("expected eventType attribute to be 'unknown'")
	}
}

// unmarshalableEvent is a mock event that fails JSON marshaling.
type unmarshalableEvent struct {
	BadField chan int // channels cannot be marshaled to JSON
}

func (e unmarshalableEvent) EventType() outbound.EventType { return outbound.EventTypeBlock }
func (e unmarshalableEvent) GetBlockNumber() int64         { return 100 }
func (e unmarshalableEvent) GetChainID() int64             { return 1 }

func TestPublish_MarshalError(t *testing.T) {
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		TopicARN: testTopicARN,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Create event with a field that cannot be marshaled
	event := unmarshalableEvent{BadField: make(chan int)}

	err = sink.Publish(context.Background(), event)
	if err == nil {
		t.Fatal("expected error for marshal failure")
	}
	// Verify this is a JSON marshaling error (chan types cannot be marshaled)
	var unsupportedTypeErr *json.UnsupportedTypeError
	if !errors.As(err, &unsupportedTypeErr) {
		t.Errorf("expected json.UnsupportedTypeError, got: %v", err)
	}

	if len(client.calls) != 0 {
		t.Errorf("expected 0 SNS calls, got %d", len(client.calls))
	}
}

func TestPublish_NonRetryableError(t *testing.T) {
	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			// Return context.Canceled which is not retryable
			return nil, context.Canceled
		},
	}

	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(context.Background(), event)
	if err == nil {
		t.Fatal("expected error for non-retryable failure")
	}

	// Should fail immediately without retrying
	if len(client.calls) != 1 {
		t.Errorf("expected 1 call (no retries for non-retryable error), got %d", len(client.calls))
	}

	// Error should wrap the original error
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected error to wrap context.Canceled, got: %v", err)
	}
}

func TestPublish_BackoffCappedAtMax(t *testing.T) {
	callCount := 0
	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			callCount++
			if callCount < 5 {
				return nil, &types.ThrottledException{Message: aws.String("throttled")}
			}
			return &sns.PublishOutput{MessageId: aws.String("success")}, nil
		},
	}

	// Set MaxBackoff very low so it gets capped during retries
	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     5,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     2 * time.Millisecond, // Very low max
		BackoffFactor:  10.0,                 // High factor to quickly exceed max
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}

	if callCount != 5 {
		t.Errorf("expected 5 calls, got %d", callCount)
	}
}

func TestPublish_DeadlineExceededIsRetryable(t *testing.T) {
	// Publish timeout (DeadlineExceeded) IS retryable because it indicates
	// a transient network issue with the SNS endpoint
	callCount := 0
	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			callCount++
			if callCount < 3 {
				return nil, context.DeadlineExceeded
			}
			return &sns.PublishOutput{MessageId: aws.String("success")}, nil
		},
	}

	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("expected success after retry, got error: %v", err)
	}

	// Should retry and succeed on 3rd call
	if callCount != 3 {
		t.Errorf("expected 3 calls (2 retries before success), got %d", callCount)
	}
}

func TestPublish_ParentContextCancelledNotRetryable(t *testing.T) {
	// When parent context is cancelled (shutdown signal), we should NOT retry
	callCount := 0
	ctx, cancel := context.WithCancel(context.Background())

	client := &mockSNSClient{
		publishFunc: func(ctx context.Context, params *sns.PublishInput, optFns ...func(*sns.Options)) (*sns.PublishOutput, error) {
			callCount++
			// Cancel the parent context to simulate shutdown
			cancel()
			return nil, context.DeadlineExceeded
		},
	}

	sink, err := NewEventSink(client, Config{
		TopicARN:       testTopicARN,
		MaxRetries:     3,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
		BackoffFactor:  2.0,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 100}
	err = sink.Publish(ctx, event)
	if err == nil {
		t.Fatal("expected error when parent context is cancelled")
	}

	// Should fail immediately without retrying because parent context is cancelled
	if callCount != 1 {
		t.Errorf("expected 1 call (no retries when parent cancelled), got %d", callCount)
	}
}
