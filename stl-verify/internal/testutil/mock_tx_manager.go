package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// MockTxManager implements outbound.TxManager for testing.
type MockTxManager struct {
	WithTransactionFn func(ctx context.Context, fn func(tx pgx.Tx) error) error
}

func (m *MockTxManager) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	if m.WithTransactionFn != nil {
		return m.WithTransactionFn(ctx, fn)
	}
	// Default: execute directly with nil tx (repos are mocked, don't need a real tx).
	return fn(nil)
}
