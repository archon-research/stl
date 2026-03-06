package testutil

import (
	"context"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MockSQSConsumer implements outbound.SQSConsumer for tests.
type MockSQSConsumer struct {
	Mu sync.Mutex

	ReceiveMessagesFn func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
	DeleteMessageFn   func(ctx context.Context, receiptHandle string) error
	CloseFn           func() error

	DeleteMessageCalls  int
	ReceiveMessageCalls int
	CloseCalls          int
}

func (m *MockSQSConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	m.Mu.Lock()
	m.ReceiveMessageCalls++
	m.Mu.Unlock()
	if m.ReceiveMessagesFn != nil {
		return m.ReceiveMessagesFn(ctx, maxMessages)
	}
	return nil, nil
}

func (m *MockSQSConsumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	m.Mu.Lock()
	m.DeleteMessageCalls++
	m.Mu.Unlock()
	if m.DeleteMessageFn != nil {
		return m.DeleteMessageFn(ctx, receiptHandle)
	}
	return nil
}

func (m *MockSQSConsumer) Close() error {
	m.Mu.Lock()
	m.CloseCalls++
	m.Mu.Unlock()
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}
