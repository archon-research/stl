package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PositionMaterializerRepository implements
// outbound.PositionMaterializer.
var _ outbound.PositionMaterializer = (*PositionMaterializerRepository)(nil)

// PositionMaterializerRepository invokes the shared materializer function
// (materialize_position_projection, VEC-402) for a projection view. The function
// owns all correctness logic; this adapter is the call site.
type PositionMaterializerRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPositionMaterializerRepository creates a PositionMaterializerRepository.
func NewPositionMaterializerRepository(pool *pgxpool.Pool, logger *slog.Logger) *PositionMaterializerRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PositionMaterializerRepository{pool: pool, logger: logger}
}

// Materialize runs materialize_position_projection for one view. The single
// SELECT is its own transaction, honoring the one-view-per-transaction contract
// documented on the function (per-view advisory xact lock). The regclass cast
// fails loudly on a view name that does not resolve to a relation, so a
// misconfigured projection list cannot be silently skipped.
func (r *PositionMaterializerRepository) Materialize(ctx context.Context, view string, buildID int) (int64, error) {
	var changed int64
	if err := r.pool.QueryRow(ctx,
		`SELECT materialize_position_projection($1::regclass, $2)`, view, buildID,
	).Scan(&changed); err != nil {
		return 0, fmt.Errorf("materializing projection %s: %w", view, err)
	}
	return changed, nil
}
