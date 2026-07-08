package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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
// call consumes fewer than drainBatch rows, and returns the total consumed. The
// function name is built from source and quoted as an identifier; source
// originates from transformed._sources (our own controlled table), so this is not
// attacker-controlled, but it is quoted regardless.
func (r *TransformRunnerRepository) RunTable(ctx context.Context, source string) (int64, error) {
	fn := pgx.Identifier{"transformed", "_run_" + source}.Sanitize()
	var total int64
	for range maxDrainIterations {
		var consumed int64
		if err := r.pool.QueryRow(ctx, "SELECT "+fn+"()").Scan(&consumed); err != nil {
			return total, fmt.Errorf("running transform %q: %w", source, err)
		}
		total += consumed
		if consumed < drainBatch {
			return total, nil
		}
	}
	r.logger.Warn("transform drain hit iteration cap; remaining queue picked up next tick",
		"source", source, "consumed", total, "cap", maxDrainIterations)
	return total, nil
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

// BootstrapTable invokes transformed._bootstrap_<source>(from, to), copying the
// pre-existing raw rows in the [from, to) window of the source's observation-time
// column into the transformed table (ON CONFLICT DO UPDATE, IS DISTINCT FROM
// guarded, so re-running is idempotent). It is the one-off
// history backfill run outside the worker, not part of steady-state refresh; the
// worker's enqueue triggers cover everything written from bootstrap onward. Same
// controlled-identifier reasoning as RunTable.
func (r *TransformRunnerRepository) BootstrapTable(ctx context.Context, source string, from, to time.Time) (int64, error) {
	fn := pgx.Identifier{"transformed", "_bootstrap_" + source}.Sanitize()
	var rows int64
	if err := r.pool.QueryRow(ctx, "SELECT "+fn+"($1, $2)", from, to).Scan(&rows); err != nil {
		return 0, fmt.Errorf("bootstrapping transform %q [%s, %s): %w", source, from, to, err)
	}
	return rows, nil
}
