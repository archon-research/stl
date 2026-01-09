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

// testTopics returns a TopicARNs struct with test values for all event types.
func testTopics() TopicARNs {
	return TopicARNs{
		Blocks:   "arn:aws:sns:us-east-1:123456789:blocks",
		Receipts: "arn:aws:sns:us-east-1:123456789:receipts",
		Traces:   "arn:aws:sns:us-east-1:123456789:traces",
		Blobs:    "arn:aws:sns:us-east-1:123456789:blobs",
	}
}

func TestNewEventSink_RequiresClient(t *testing.T) {
	_, err := NewEventSink(nil, Config{Topics: testTopics()})
	if err == nil {
		t.Error("expected error for nil client")
	}
	if err.Error() != "sns client is required" {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewEventSink_RequiresTopicARN(t *testing.T) {
	tests := []struct {
		name    string
		topics  TopicARNs
		wantErr string
	}{
		{
			name:    "missing blocks topic",
			topics:  TopicARNs{Receipts: "x", Traces: "x", Blobs: "x"},
			wantErr: "blocks topic ARN is required",
		},
		{
			name:    "missing receipts topic",
			topics:  TopicARNs{Blocks: "x", Traces: "x", Blobs: "x"},
			wantErr: "receipts topic ARN is required",
		},
		{
			name:    "missing traces topic",
			topics:  TopicARNs{Blocks: "x", Receipts: "x", Blobs: "x"},
			wantErr: "traces topic ARN is required",
		},
		{
			name:    "missing blobs topic",
			topics:  TopicARNs{Blocks: "x", Receipts: "x", Traces: "x"},
			wantErr: "blobs topic ARN is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewEventSink(&mockSNSClient{}, Config{Topics: tt.topics})
			if err == nil {
				t.Error("expected error for missing topic ARN")
			}
			if err.Error() != tt.wantErr {
				t.Errorf("expected error %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestNewEventSink_AppliesDefaults(t *testing.T) {
	client := &mockSNSClient{}
	sink, err := NewEventSink(client, Config{
		Topics: testTopics(),
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
	topics := testTopics()
	sink, err := NewEventSink(client, Config{
		Topics: topics,
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
		CacheKey:    "block:1:12345",
	}

	err = sink.Publish(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(client.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(client.calls))
	}

	call := client.calls[0]
	if *call.TopicArn != topics.Blocks {
		t.Errorf("unexpected topic ARN: %s, expected %s", *call.TopicArn, topics.Blocks)
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

func TestPublish_AllEventTypes(t *testing.T) {
	topics := testTopics()

	tests := []struct {
		name        string
		event       outbound.Event
		eventType   string
		expectedARN string
	}{
		{
			name: "BlockEvent",
			event: outbound.BlockEvent{
				ChainID:     1,
				BlockNumber: 100,
				CacheKey:    "block:1:100",
			},
			eventType:   "block",
			expectedARN: topics.Blocks,
		},
		{
			name: "ReceiptsEvent",
			event: outbound.ReceiptsEvent{
				ChainID:     1,
				BlockNumber: 100,
				CacheKey:    "receipts:1:100",
			},
			eventType:   "receipts",
			expectedARN: topics.Receipts,
		},
		{
			name: "TracesEvent",
			event: outbound.TracesEvent{
				ChainID:     1,
				BlockNumber: 100,
				CacheKey:    "traces:1:100",
			},
			eventType:   "traces",
			expectedARN: topics.Traces,
		},
		{
			name: "BlobsEvent",
			event: outbound.BlobsEvent{
				ChainID:     1,
				BlockNumber: 100,
				CacheKey:    "blobs:1:100",
			},
			eventType:   "blobs",
			expectedARN: topics.Blobs,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockSNSClient{}
			sink, err := NewEventSink(client, Config{
				Topics: topics,
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			err = sink.Publish(context.Background(), tt.event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(client.calls) != 1 {
				t.Fatalf("expected 1 call, got %d", len(client.calls))
			}

			call := client.calls[0]
			if *call.TopicArn != tt.expectedARN {
				t.Errorf("expected topic ARN=%s, got %s", tt.expectedARN, *call.TopicArn)
			}
			if *call.MessageAttributes["eventType"].StringValue != tt.eventType {
				t.Errorf("expected eventType=%s, got %s", tt.eventType, *call.MessageAttributes["eventType"].StringValue)
			}
		})
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
		Topics:         testTopics(),
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
		Topics:         testTopics(),
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
		Topics:         testTopics(),
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
		Topics: testTopics(),
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
		Topics: testTopics(),
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
		name      string
		err       error
		retryable bool
	}{
		{
			name:      "nil error",
			err:       nil,
			retryable: false,
		},
		{
			name:      "context cancelled",
			err:       context.Canceled,
			retryable: false,
		},
		{
			name:      "context deadline exceeded",
			err:       context.DeadlineExceeded,
			retryable: false,
		},
		{
			name:      "throttle exception",
			err:       &types.ThrottledException{Message: aws.String("throttled")},
			retryable: true,
		},
		{
			name:      "internal error",
			err:       &types.InternalErrorException{Message: aws.String("internal")},
			retryable: true,
		},
		{
			name:      "KMS throttling",
			err:       &types.KMSThrottlingException{Message: aws.String("kms throttled")},
			retryable: true,
		},
		{
			name:      "generic error",
			err:       errors.New("some error"),
			retryable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
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
		Topics:         testTopics(),
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
		Topics: testTopics(),
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
		CacheKey:       "block:1:12345",
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
	if decoded.CacheKey != "block:1:12345" {
		t.Errorf("expected CacheKey=block:1:12345, got %s", decoded.CacheKey)
	}
	if !decoded.IsReorg {
		t.Error("expected IsReorg=true")
	}
	if decoded.IsBackfill {
		t.Error("expected IsBackfill=false")
	}
}
