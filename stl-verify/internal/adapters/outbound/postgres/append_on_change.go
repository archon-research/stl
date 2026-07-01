package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// AppendOnChange serializes a read-latest-then-insert decision for one natural
// key: it takes the per-key advisory xact lock, reads the latest row, and calls
// insert only when there is no prior row or the candidate differs. This closes
// the read-then-write race that ON CONFLICT alone cannot guard (ADR-0002 §3).
// lockKey must be the SAME natural-key string the table's pv trigger locks on.
//
// readLatest returns latest == nil when no prior row exists. changed receives
// that same latest (nil on no prior row, where it must report true) and decides
// whether insert runs. insert is expected to use ON CONFLICT DO NOTHING so a
// replayed event stays idempotent.
func AppendOnChange[T any](
	ctx context.Context,
	tx pgx.Tx,
	lockKey string,
	readLatest func(ctx context.Context, tx pgx.Tx) (latest *T, err error),
	changed func(latest *T) bool,
	insert func(ctx context.Context, tx pgx.Tx) error,
) error {
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
		lockKey,
	); err != nil {
		return fmt.Errorf("locking %q: %w", lockKey, err)
	}

	latest, err := readLatest(ctx, tx)
	if err != nil {
		return fmt.Errorf("reading latest for %q: %w", lockKey, err)
	}

	if !changed(latest) {
		return nil
	}

	if err := insert(ctx, tx); err != nil {
		return fmt.Errorf("inserting for %q: %w", lockKey, err)
	}
	return nil
}
