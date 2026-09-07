package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockDebtTokenRepository implements outbound.DebtTokenRepository for testing.
type MockDebtTokenRepository struct {
	GetOrCreateDebtTokenFn func(ctx context.Context, tx pgx.Tx, token entity.DebtToken) (int64, error)

	// Calls records every DebtToken passed to GetOrCreateDebtToken, in order,
	// so tests can assert that the write happened with the expected values.
	Calls []entity.DebtToken
}

func (m *MockDebtTokenRepository) GetOrCreateDebtToken(ctx context.Context, tx pgx.Tx, token entity.DebtToken) (int64, error) {
	m.Calls = append(m.Calls, token)
	if m.GetOrCreateDebtTokenFn != nil {
		return m.GetOrCreateDebtTokenFn(ctx, tx, token)
	}
	return 1, nil
}
