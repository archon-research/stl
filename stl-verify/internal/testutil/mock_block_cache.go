package testutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ErrCacheClosed is a sentinel error for testing cache-closed scenarios.
var ErrCacheClosed = errors.New("cache connection closed")

var _ outbound.BlockCache = (*MockBlockCache)(nil)

// MockBlockCache implements outbound.BlockCache for testing.
type MockBlockCache struct {
	SetBlockDataFn func(ctx context.Context, chainID, blockNumber int64, version int, data outbound.BlockDataInput) error
	GetBlockFn     func(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error)
	GetReceiptsFn  func(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error)
	GetTracesFn    func(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error)
	GetBlobsFn     func(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error)
	DeleteBlockFn  func(ctx context.Context, chainID, blockNumber int64, version int) error
	CloseFn        func() error

	mu       sync.RWMutex
	receipts map[string]json.RawMessage
	err      error
}

// NewMockBlockCache returns a MockBlockCache pre-configured with an in-memory receipts store.
// Use SetReceipts/SetError to control test data.
func NewMockBlockCache() *MockBlockCache {
	return &MockBlockCache{receipts: make(map[string]json.RawMessage)}
}

// SetReceipts stores receipt data for a given block in the in-memory store.
func (m *MockBlockCache) SetReceipts(chainID, blockNumber int64, version int, data json.RawMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receipts[receiptsKey(chainID, blockNumber, version)] = data
}

// SetError configures the mock to return this error from GetReceipts (via the default implementation).
func (m *MockBlockCache) SetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.err = err
}

func (m *MockBlockCache) SetBlockData(ctx context.Context, chainID, blockNumber int64, version int, data outbound.BlockDataInput) error {
	if m.SetBlockDataFn != nil {
		return m.SetBlockDataFn(ctx, chainID, blockNumber, version, data)
	}
	return nil
}

func (m *MockBlockCache) GetBlock(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	if m.GetBlockFn != nil {
		return m.GetBlockFn(ctx, chainID, blockNumber, version)
	}
	return nil, nil
}

func (m *MockBlockCache) GetReceipts(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	if m.GetReceiptsFn != nil {
		return m.GetReceiptsFn(ctx, chainID, blockNumber, version)
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.err != nil {
		return nil, m.err
	}
	key := receiptsKey(chainID, blockNumber, version)
	data, ok := m.receipts[key]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (m *MockBlockCache) GetTraces(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	if m.GetTracesFn != nil {
		return m.GetTracesFn(ctx, chainID, blockNumber, version)
	}
	return nil, nil
}

func (m *MockBlockCache) GetBlobs(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	if m.GetBlobsFn != nil {
		return m.GetBlobsFn(ctx, chainID, blockNumber, version)
	}
	return nil, nil
}

func (m *MockBlockCache) DeleteBlock(ctx context.Context, chainID, blockNumber int64, version int) error {
	if m.DeleteBlockFn != nil {
		return m.DeleteBlockFn(ctx, chainID, blockNumber, version)
	}
	return nil
}

func (m *MockBlockCache) Close() error {
	if m.CloseFn != nil {
		return m.CloseFn()
	}
	return nil
}

func receiptsKey(chainID, blockNumber int64, version int) string {
	return fmt.Sprintf("%d:%d:%d", chainID, blockNumber, version)
}
