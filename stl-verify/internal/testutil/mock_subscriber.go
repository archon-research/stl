package testutil

import (
	"context"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MockSubscriber is a test subscriber that emits headers on demand.
// It satisfies the outbound.BlockSubscriber interface plus SetOnReconnect
// used by tests that exercise reconnection callbacks.
type MockSubscriber struct {
	mu       sync.Mutex
	headers  chan outbound.BlockHeader
	closed   bool
	OnReconn func()
}

func NewMockSubscriber() *MockSubscriber {
	return &MockSubscriber{
		headers: make(chan outbound.BlockHeader, 100),
	}
}

func (m *MockSubscriber) Subscribe(ctx context.Context) (<-chan outbound.BlockHeader, error) {
	return m.headers, nil
}

func (m *MockSubscriber) Unsubscribe() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.headers)
	}
	return nil
}

func (m *MockSubscriber) HealthCheck(ctx context.Context) error {
	return nil
}

func (m *MockSubscriber) SetOnReconnect(callback func()) {
	m.OnReconn = callback
}

func (m *MockSubscriber) SendHeader(header outbound.BlockHeader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.headers <- header
	}
}
