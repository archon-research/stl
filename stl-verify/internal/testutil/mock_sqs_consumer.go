package testutil

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MockSQSConsumer implements outbound.SQSConsumer for testing.
type MockSQSConsumer struct {
	ReceiveMessagesFn func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
	DeleteMessageFn   func(ctx context.Context, receiptHandle string) error
	CloseFn           func() error
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

func (m *MockSQSConsumer) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
