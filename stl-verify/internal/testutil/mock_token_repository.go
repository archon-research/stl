package testutil

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockTokenRepository implements outbound.TokenRepository for testing.
type MockTokenRepository struct {
	GetOrCreateTokenFn    func(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error)
	GetTokenIDBySymbolFn  func(ctx context.Context, chainID int64, symbol string) (int64, error)
	UpsertTokensFn        func(ctx context.Context, tokens []*entity.Token) error
	UpsertReceiptTokensFn func(ctx context.Context, tokens []*entity.ReceiptToken) error
	UpsertDebtTokensFn    func(ctx context.Context, tokens []*entity.DebtToken) error
}

func (m *MockTokenRepository) GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error) {
	if m.GetOrCreateTokenFn != nil {
		return m.GetOrCreateTokenFn(ctx, tx, chainID, address, symbol, decimals, createdAtBlock)
	}
	return 1, nil
}

func (m *MockTokenRepository) GetTokenIDBySymbol(ctx context.Context, chainID int64, symbol string) (int64, error) {
	if m.GetTokenIDBySymbolFn != nil {
		return m.GetTokenIDBySymbolFn(ctx, chainID, symbol)
	}
	return 1, nil
}

func (m *MockTokenRepository) UpsertTokens(ctx context.Context, tokens []*entity.Token) error {
	if m.UpsertTokensFn != nil {
		return m.UpsertTokensFn(ctx, tokens)
	}
	return nil
}

func (m *MockTokenRepository) UpsertReceiptTokens(ctx context.Context, tokens []*entity.ReceiptToken) error {
	if m.UpsertReceiptTokensFn != nil {
		return m.UpsertReceiptTokensFn(ctx, tokens)
	}
	return nil
}

func (m *MockTokenRepository) UpsertDebtTokens(ctx context.Context, tokens []*entity.DebtToken) error {
	if m.UpsertDebtTokensFn != nil {
		return m.UpsertDebtTokensFn(ctx, tokens)
	}
	return nil
}
