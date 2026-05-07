package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockReceiptTokenRepository implements outbound.ReceiptTokenRepository for testing.
type MockReceiptTokenRepository struct {
	GetOrCreateReceiptTokenFn func(ctx context.Context, tx pgx.Tx, token entity.ReceiptToken) (int64, error)
}

func (m *MockReceiptTokenRepository) GetOrCreateReceiptToken(ctx context.Context, tx pgx.Tx, token entity.ReceiptToken) (int64, error) {
	if m.GetOrCreateReceiptTokenFn != nil {
		return m.GetOrCreateReceiptTokenFn(ctx, tx, token)
	}
	return 1, nil
}
