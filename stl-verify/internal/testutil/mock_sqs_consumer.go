package testutil

import (
	"context"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MockSQSConsumer implements outbound.SQSConsumer for testing.
type MockSQSConsumer struct {
	ReceiveMessagesFn              func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
	DeleteMessageFn                func(ctx context.Context, receiptHandle string) error
	ChangeMessageVisibilityBatchFn func(ctx context.Context, receiptHandles []string, visibility time.Duration) (map[string]error, error)
	CloseFn                        func() error
	VisibilityTimeoutFn            func() time.Duration
}

func (m *MockSQSConsumer) VisibilityTimeout() time.Duration {
	if m.VisibilityTimeoutFn != nil {
		return m.VisibilityTimeoutFn()
	}
	return 300 * time.Second
}

func (m *MockSQSConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	if m.ReceiveMessagesFn != nil {
		return m.ReceiveMessagesFn(ctx, maxMessages)
	}
	return nil, nil
}

func (m *MockSQSConsumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	if m.DeleteMessageFn != nil {
		return m.DeleteMessageFn(ctx, receiptHandle)
	}
	return nil
}

func (m *MockSQSConsumer) ChangeMessageVisibilityBatch(ctx context.Context, receiptHandles []string, visibility time.Duration) (map[string]error, error) {
	if m.ChangeMessageVisibilityBatchFn != nil {
		return m.ChangeMessageVisibilityBatchFn(ctx, receiptHandles, visibility)
	}
	return nil, nil
}

func (m *MockSQSConsumer) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
