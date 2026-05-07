package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/jackc/pgx/v5"
)

// ReceiptTokenRepository defines the interface for receipt token persistence.
type ReceiptTokenRepository interface {
	// GetOrCreateReceiptToken upserts a receipt token and returns its ID.
	// Uses ON CONFLICT (chain_id, receipt_token_address) for idempotent upserts.
	GetOrCreateReceiptToken(ctx context.Context, tx pgx.Tx, token entity.ReceiptToken) (int64, error)
}
