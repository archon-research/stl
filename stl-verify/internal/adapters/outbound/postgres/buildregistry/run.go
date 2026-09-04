package buildregistry

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RunID identifies one writer_run: the process start that wrote a governed row.
// Typed for the same reason as BuildID.
type RunID int64

// Value binds the zero RunID as NULL: no run was opened, and run_id NULL is what
// "written before tracking" means, whereas 0 would name a run that does not exist.
func (id RunID) Value() (driver.Value, error) {
	if id == 0 {
		return nil, nil
	}
	return int64(id), nil
}

// OpenRun records one process start in writer_run and returns its id. The row's
// reference_snapshot is pg_current_snapshot() of a REPEATABLE READ transaction in
// which load then reads the process's reference data, so "the reference rows this
// run saw" is exactly the rows visible in that snapshot with
// valid_from <= referenceEffectiveAt (ADR-0006 §2, §4). load receives the run id
// so it can construct the repositories that will carry it; a nil load records the
// run alone. If load fails nothing is committed: a run row for a process that
// never started would be provenance nobody could trust.
//
// A process that reloads its reference data must open a new run.
func (r *Registry) OpenRun(ctx context.Context, referenceEffectiveAt time.Time, load func(tx pgx.Tx, runID RunID) error) (RunID, error) {
	if referenceEffectiveAt.IsZero() {
		return 0, fmt.Errorf("opening writer run: referenceEffectiveAt must be set")
	}

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, fmt.Errorf("opening writer run: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID RunID
	if err := tx.QueryRow(ctx, `
		INSERT INTO writer_run (build_id, reference_snapshot, reference_effective_at)
		VALUES ($1, pg_current_snapshot()::text, $2)
		RETURNING id`, int(r.buildID), referenceEffectiveAt.UTC()).Scan(&runID); err != nil {
		return 0, fmt.Errorf("opening writer run for build %d: %w", r.buildID, err)
	}

	if load != nil {
		if err := load(tx, runID); err != nil {
			return 0, fmt.Errorf("loading reference data for run %d: %w", runID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing writer run %d: %w", runID, err)
	}
	return runID, nil
}
