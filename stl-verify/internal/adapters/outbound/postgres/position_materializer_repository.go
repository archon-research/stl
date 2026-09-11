package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PositionMaterializerRepository implements
// outbound.PositionMaterializer.
var _ outbound.PositionMaterializer = (*PositionMaterializerRepository)(nil)

// PositionMaterializerRepository calls one per-projection materializer function
// (materialize_<projection>, VEC-402). The function owns all correctness logic,
// including its projection's own pre-flight refusals; this adapter is the call site.
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

// Materialize calls one materializer function, retrying transient transaction
// errors.
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
func (r *PositionMaterializerRepository) Materialize(ctx context.Context, materializer string, buildID int, runID int64) (int64, error) {
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
			"materializer", materializer,
			"build_id", buildID,
			"run_id", runID,
			"backoff", backoff)
	}

	return retry.Do(ctx, cfg, isRetryableTxError, onRetry, func() (int64, error) {
		return r.materializeOnce(ctx, materializer, buildID, runID)
	})
}

// RefusedByProjection reads each projection's positions_refused from its latest run row.
// position_projection_run is keyed (projection, created_at), so the newest row per projection
// is one index scan; a projection that has never run is absent rather than zero.
func (r *PositionMaterializerRepository) RefusedByProjection(ctx context.Context) (map[string]int64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (projection) projection, positions_refused
		  FROM position_projection_run
		 ORDER BY projection, created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("reading positions_refused: %w", err)
	}
	defer rows.Close()

	out := map[string]int64{}
	for rows.Next() {
		var projection string
		var refused int64
		if err := rows.Scan(&projection, &refused); err != nil {
			return nil, fmt.Errorf("scanning positions_refused: %w", err)
		}
		out[projection] = refused
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating positions_refused: %w", err)
	}
	return out, nil
}

// materializeOnce is a single materialization attempt. The SELECT is its own
// transaction, honoring the one-projection-per-transaction contract documented on
// the shared function (per-view advisory xact lock). The function name is quoted as
// an identifier, so a misconfigured entry fails loudly as an unknown function and
// cannot be silently skipped or read as SQL.
// The arguments are named, not positional: the wrappers do not share an argument list
// (materialize_maple_loan takes a skew tolerance between the two provenance parameters), so a
// positional second argument would land on whatever that projection declares there.
func (r *PositionMaterializerRepository) materializeOnce(ctx context.Context, materializer string, buildID int, runID int64) (int64, error) {
	var changed int64
	q := fmt.Sprintf(`SELECT %s(p_build_id => $1, p_run_id => $2)`, pgx.Identifier{materializer}.Sanitize())
	// buildregistry.RunID, not the bare int64: its Valuer maps 0 to NULL, because a zero would name a
	// writer_run row that does not exist and run_id carries no FK to catch it.
	if err := r.pool.QueryRow(ctx, q, buildID, buildregistry.RunID(runID)).Scan(&changed); err != nil {
		return 0, fmt.Errorf("running %s: %w", materializer, err)
	}
	return changed, nil
}
