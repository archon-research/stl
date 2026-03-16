package testutil

import (
	"context"

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
