package testutil

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
)

// MockTokenRepository implements outbound.TokenRepository for testing.
type MockTokenRepository struct {
	GetOrCreateTokenFn func(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error)
}

func (m *MockTokenRepository) GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error) {
	if m.GetOrCreateTokenFn != nil {
		return m.GetOrCreateTokenFn(ctx, tx, chainID, address, symbol, decimals, createdAtBlock)
	}
	return 1, nil
}

func (m *MockTokenRepository) GetOrCreateTokens(ctx context.Context, tx pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error) {
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
