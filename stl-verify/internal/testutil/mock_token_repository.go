package testutil

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// MockTokenRepository implements outbound.TokenRepository for testing.
type MockTokenRepository struct {
	GetOrCreateTokenFn          func(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error)
	GetOrCreateTokensFn         func(ctx context.Context, tx pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error)
	MarkTokenSymbolPendingFn    func(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, anchorBlock int64) error
	ListTokensPendingSymbolFn   func(ctx context.Context, chainID int64, limit int) ([]outbound.PendingTokenSymbol, error)
	ResolveTokenSymbolFn        func(ctx context.Context, chainID int64, address common.Address, symbol string) error
	MarkTokenSymbolUnresolvedFn func(ctx context.Context, chainID int64, address common.Address) error
}

func (m *MockTokenRepository) GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error) {
	if m.GetOrCreateTokenFn != nil {
		return m.GetOrCreateTokenFn(ctx, tx, chainID, address, symbol, decimals, createdAtBlock)
	}
	return 1, nil
}

func (m *MockTokenRepository) GetOrCreateTokens(ctx context.Context, tx pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error) {
	if m.GetOrCreateTokensFn != nil {
		return m.GetOrCreateTokensFn(ctx, tx, tokens)
	}
	result := make(map[common.Address]int64, len(tokens))
	for i, t := range tokens {
		if m.GetOrCreateTokenFn != nil {
			id, err := m.GetOrCreateTokenFn(ctx, tx, t.ChainID, t.Address, t.Symbol, t.Decimals, t.CreatedAtBlock)
			if err != nil {
				return nil, err
			}
			result[t.Address] = id
		} else {
			result[t.Address] = int64(i + 1)
		}
	}
	return result, nil
}

func (m *MockTokenRepository) MarkTokenSymbolPending(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, anchorBlock int64) error {
	if m.MarkTokenSymbolPendingFn != nil {
		return m.MarkTokenSymbolPendingFn(ctx, tx, chainID, address, anchorBlock)
	}
	return nil
}

func (m *MockTokenRepository) ListTokensPendingSymbol(ctx context.Context, chainID int64, limit int) ([]outbound.PendingTokenSymbol, error) {
	if m.ListTokensPendingSymbolFn != nil {
		return m.ListTokensPendingSymbolFn(ctx, chainID, limit)
	}
	return nil, nil
}

func (m *MockTokenRepository) ResolveTokenSymbol(ctx context.Context, chainID int64, address common.Address, symbol string) error {
	if m.ResolveTokenSymbolFn != nil {
		return m.ResolveTokenSymbolFn(ctx, chainID, address, symbol)
	}
	return nil
}

func (m *MockTokenRepository) MarkTokenSymbolUnresolved(ctx context.Context, chainID int64, address common.Address) error {
	if m.MarkTokenSymbolUnresolvedFn != nil {
		return m.MarkTokenSymbolUnresolvedFn(ctx, chainID, address)
	}
	return nil
}
