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

// RunTable invokes transformed._run_<source>() and returns the rows upserted.
// The function name is built from source and quoted as an identifier; source
// originates from transformed._sources (our own controlled table), so this is
// not attacker-controlled, but it is quoted regardless.
func (r *TransformRunnerRepository) RunTable(ctx context.Context, source string) (int64, error) {
	fn := pgx.Identifier{"transformed", "_run_" + source}.Sanitize()
	var rows int64
	if err := r.pool.QueryRow(ctx, "SELECT "+fn+"()").Scan(&rows); err != nil {
		return 0, fmt.Errorf("running transform %q: %w", source, err)
	}
	return rows, nil
}

// BootstrapTable invokes transformed._bootstrap_<source>(from, to), copying the
// pre-existing raw rows in the [from, to) window of the source's observation-time
// column into the transformed table (ON CONFLICT DO NOTHING). It is the one-off
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
