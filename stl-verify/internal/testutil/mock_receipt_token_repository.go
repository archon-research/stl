package testutil

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// MockReceiptTokenRepository implements outbound.ReceiptTokenRepository for testing.
type MockReceiptTokenRepository struct {
	ListTrackedReceiptTokensFn func(ctx context.Context, chainID int64) ([]outbound.TrackedReceiptToken, error)
}

func (m *MockReceiptTokenRepository) ListTrackedReceiptTokens(ctx context.Context, chainID int64) ([]outbound.TrackedReceiptToken, error) {
	if m.ListTrackedReceiptTokensFn != nil {
		return m.ListTrackedReceiptTokensFn(ctx, chainID)
	}
	return nil, nil
}
