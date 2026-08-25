package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
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

// Materialize runs materialize_position_projection for one view, retrying
// transient transaction errors.
//
// position_state carries compression and S3 tiering policies, and both are
// chunk-level actors taking AccessExclusiveLock per chunk. A policy job running
// concurrently with a materializer run can deadlock it (SQLSTATE 40P01), with the
// materializer as the victim. The migration orders its INSERT by chunk key so the
// two acquire chunk locks in the same order, which is the repo's standing answer
// to writer deadlocks (ADR-0002, as in token_repository and position_repository)
// — but ordering narrows the window rather than closing it, so the caller-side
// half of that pattern belongs here. The function is idempotent (NOT EXISTS +
// ON CONFLICT DO NOTHING), so a retry re-runs safely: a deadlocked attempt
// committed nothing, and a redundant attempt inserts zero rows.
//
// Config matches blockstate_repository.go rather than being tuned here. Worth a
// reviewer's eye: those values were chosen for row-level contention and give
// roughly half a second of total backoff, while a compression job can hold a
// chunk lock for longer than that (~900ms observed for one run_job over 100
// chunks). If that proves too short in practice the values want raising, but not
// by guesswork ahead of a measurement from a real runner.
func (r *PositionMaterializerRepository) Materialize(ctx context.Context, view string, buildID int) (int64, error) {
	cfg := retry.Config{
		MaxRetries:     10,
		InitialBackoff: 1 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		BackoffFactor:  2.0,
		Jitter:         true,
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		r.logger.Debug("retryable tx error, retrying materialization",
			"attempt", attempt,
			"view", view,
			"build_id", buildID,
			"backoff", backoff)
	}

	return retry.Do(ctx, cfg, isRetryableTxError, onRetry, func() (int64, error) {
		return r.materializeOnce(ctx, view, buildID)
	})
}

// materializeOnce is a single materialization attempt. The SELECT is its own
// transaction, honoring the one-view-per-transaction contract documented on the
// function (per-view advisory xact lock). The regclass cast fails loudly on a
// view name that does not resolve to a relation, so a misconfigured projection
// list cannot be silently skipped.
func (r *PositionMaterializerRepository) materializeOnce(ctx context.Context, view string, buildID int) (int64, error) {
	var changed int64
	if err := r.pool.QueryRow(ctx,
		`SELECT materialize_position_projection($1::regclass, $2)`, view, buildID,
	).Scan(&changed); err != nil {
		return 0, fmt.Errorf("materializing projection %s: %w", view, err)
	}
	return changed, nil
}
