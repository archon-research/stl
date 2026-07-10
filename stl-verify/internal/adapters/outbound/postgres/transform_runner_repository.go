package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that TransformRunnerRepository implements outbound.TransformRunner.
var _ outbound.TransformRunner = (*TransformRunnerRepository)(nil)

// TransformRunnerRepository runs the transformation layer's generated run
// functions and reads the table list from transformed._sources. Each
// transformed._run_<table>() drains that table's change queue
// (transformed._pending_<table>) and upserts the transformed rows.
type TransformRunnerRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewTransformRunnerRepository creates a TransformRunnerRepository.
func NewTransformRunnerRepository(pool *pgxpool.Pool, logger *slog.Logger) *TransformRunnerRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &TransformRunnerRepository{pool: pool, logger: logger}
}

// ListSources returns every source registered in transformed._sources.
func (r *TransformRunnerRepository) ListSources(ctx context.Context) ([]string, error) {
	rows, err := r.pool.Query(ctx, `SELECT source FROM transformed._sources ORDER BY source`)
	if err != nil {
		return nil, fmt.Errorf("listing transform sources: %w", err)
	}
	defer rows.Close()

	var sources []string
	for rows.Next() {
		var source string
		if err := rows.Scan(&source); err != nil {
			return nil, fmt.Errorf("scanning transform source: %w", err)
		}
		sources = append(sources, source)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating transform sources: %w", err)
	}
	return sources, nil
}

// drainBatch is the max rows transformed._run_<t>() consumes per call (its
// LIMIT). Must match the LIMIT in the generated migration: RunTable loops until a
// call consumes fewer than this, i.e. the queue is drained, so each call's DELETE
// stays a bounded transaction even after a long backlog.
const drainBatch = 10000

// maxDrainIterations caps the drain loop so a source being written faster than it
// drains cannot spin forever within one tick; the remainder is picked up next tick.
const maxDrainIterations = 1000

// RunTable drains source's queue by calling transformed._run_<source>() until a
// call consumes fewer than drainBatch rows, and returns the totals consumed (queue
// rows drained) and upserted (rows actually written; <= consumed after the IS
// DISTINCT FROM guard). Each _run call autocommits, so partial progress persists.
//
// When ctx is done it returns the accumulated totals AND ctx.Err(): the caller
// cannot otherwise tell a clean per-source time-slice expiry (context.DeadlineExceeded,
// carry the rest to the next tick) from a real parent shutdown/activity cancellation
// (context.Canceled, a genuine abort). The caller inspects the error to decide.
//
// The function name is built from source and quoted as an identifier; source
// originates from transformed._sources (our own controlled table), so this is not
// attacker-controlled, but it is quoted regardless.
func (r *TransformRunnerRepository) RunTable(ctx context.Context, source string) (consumed, upserted int64, err error) {
	fn := pgx.Identifier{"transformed", "_run_" + source}.Sanitize()
	for range maxDrainIterations {
		// Between-iteration budget check: stop before starting another batch.
		if ctx.Err() != nil {
			return consumed, upserted, ctx.Err()
		}
		var c, u int64
		if scanErr := r.pool.QueryRow(ctx, "SELECT consumed, upserted FROM "+fn+"()").Scan(&c, &u); scanErr != nil {
			// Budget/cancel mid-call: the batch rolled back (queue restored) and
			// earlier batches committed. Surface the context error so the caller can
			// distinguish a clean slice expiry from a real cancellation.
			if ctx.Err() != nil {
				return consumed, upserted, ctx.Err()
			}
			return consumed, upserted, fmt.Errorf("running transform %q: %w", source, scanErr)
		}
		consumed += c
		upserted += u
		if c < drainBatch {
			return consumed, upserted, nil
		}
	}
	r.logger.Warn("transform drain hit iteration cap; remaining queue picked up next tick",
		"source", source, "consumed", consumed, "cap", maxDrainIterations)
	return consumed, upserted, nil
}

// QueueStatus reads the per-source backlog from transformed._queue_status.
func (r *TransformRunnerRepository) QueueStatus(ctx context.Context) ([]outbound.QueueDepth, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT source, pending, COALESCE(EXTRACT(EPOCH FROM (now() - oldest_enqueued_at)), 0)
		FROM transformed._queue_status ORDER BY source`)
	if err != nil {
		return nil, fmt.Errorf("reading transform queue status: %w", err)
	}
	defer rows.Close()

	var out []outbound.QueueDepth
	for rows.Next() {
		var d outbound.QueueDepth
		if err := rows.Scan(&d.Source, &d.Pending, &d.OldestAgeSecs); err != nil {
			return nil, fmt.Errorf("scanning transform queue status: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating transform queue status: %w", err)
	}
	return out, nil
}

// RefreshParity incrementally re-verifies source's parity ledger via
// transformed._parity_refresh(source). source is passed as a value argument (not
// an identifier), and originates from transformed._sources.
func (r *TransformRunnerRepository) RefreshParity(ctx context.Context, source string) error {
	if _, err := r.pool.Exec(ctx, "SELECT transformed._parity_refresh($1)", source); err != nil {
		return fmt.Errorf("refreshing transform parity %q: %w", source, err)
	}
	return nil
}

// ParityStatus reads the per-source raw-vs-transformed parity from
// transformed._parity_status.
func (r *TransformRunnerRepository) ParityStatus(ctx context.Context) ([]outbound.ParityRow, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT source, raw_rows, transformed_rows, pending_rows, drift
		FROM transformed._parity_status ORDER BY source`)
	if err != nil {
		return nil, fmt.Errorf("reading transform parity status: %w", err)
	}
	defer rows.Close()

	var out []outbound.ParityRow
	for rows.Next() {
		var p outbound.ParityRow
		if err := rows.Scan(&p.Source, &p.RawRows, &p.TransformedRows, &p.PendingRows, &p.Drift); err != nil {
			return nil, fmt.Errorf("scanning transform parity status: %w", err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating transform parity status: %w", err)
	}
	return out, nil
}
