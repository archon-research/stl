package testutil

import (
	"context"
	"errors"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MockMulticaller implements outbound.Multicaller for testing.
type MockMulticaller struct {
	mu              sync.Mutex
	ExecuteFn       func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error)
	ExecuteAtHashFn func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error)
	CallCount       int
	// Invocations records every Execute/ExecuteAtHash call in order, so a test can
	// assert on the calls issued, the block a read was pinned to, and which entry
	// point was used, without a bespoke recording double per package.
	Invocations []Invocation
	Addr        common.Address
}

// Invocation is one recorded Execute/ExecuteAtHash call.
type Invocation struct {
	Calls       []outbound.Call
	BlockNumber *big.Int    // set by Execute; nil on the hash-pinned path
	BlockHash   common.Hash // set by ExecuteAtHash; zero on the number path
	ViaHash     bool        // true when the call arrived through ExecuteAtHash
}

func NewMockMulticaller() *MockMulticaller {
	return &MockMulticaller{
		Addr: common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"),
	}
}

func (m *MockMulticaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	m.mu.Lock()
	m.CallCount++
	m.Invocations = append(m.Invocations, Invocation{Calls: calls, BlockNumber: blockNumber})
	m.mu.Unlock()
	if m.ExecuteFn != nil {
		return m.ExecuteFn(ctx, calls, blockNumber)
	}
	return nil, errors.New("Execute not mocked")
}

func (m *MockMulticaller) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	m.mu.Lock()
	m.CallCount++
	m.Invocations = append(m.Invocations, Invocation{Calls: calls, BlockHash: blockHash, ViaHash: true})
	m.mu.Unlock()
	if m.ExecuteAtHashFn != nil {
		return m.ExecuteAtHashFn(ctx, calls, blockHash)
	}
	// VEC-471 moved many indexers' state reads from Execute to ExecuteAtHash.
	// Their existing tests configure ExecuteFn with shape-based dispatchers keyed
	// on calls alone (never the block arg), so forwarding an unset ExecuteAtHashFn
	// to ExecuteFn keeps those tests valid for both entry points without
	// per-test rewiring. Tests that assert the hash-pinned path was actually used
	// set ExecuteAtHashFn explicitly, which takes precedence over this fallback.
	if m.ExecuteFn != nil {
		return m.ExecuteFn(ctx, calls, nil)
	}
	return nil, errors.New("ExecuteAtHash not mocked")
}

func (m *MockMulticaller) Address() common.Address {
	return m.Addr
}
