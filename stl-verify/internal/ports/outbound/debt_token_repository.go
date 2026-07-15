package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/jackc/pgx/v5"
)

// DebtTokenRepository defines the interface for debt token persistence.
type DebtTokenRepository interface {
	// GetOrCreateDebtToken upserts a debt token and returns its ID.
	// Uses ON CONFLICT (protocol_id, underlying_token_id) for idempotent upserts.
	GetOrCreateDebtToken(ctx context.Context, tx pgx.Tx, token entity.DebtToken) (int64, error)
}
