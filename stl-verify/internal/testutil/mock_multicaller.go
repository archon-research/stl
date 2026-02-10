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
	mu        sync.Mutex
	ExecuteFn func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error)
	CallCount int
	Addr      common.Address
}

func NewMockMulticaller() *MockMulticaller {
	return &MockMulticaller{
		Addr: common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11"),
	}
}

func (m *MockMulticaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	m.mu.Lock()
	m.CallCount++
	m.mu.Unlock()
	if m.ExecuteFn != nil {
		return m.ExecuteFn(ctx, calls, blockNumber)
	}
	return nil, errors.New("Execute not mocked")
}

func (m *MockMulticaller) Address() common.Address {
	return m.Addr
}
