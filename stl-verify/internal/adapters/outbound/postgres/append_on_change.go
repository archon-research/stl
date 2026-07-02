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
//
// lockKey must identify the ENTITY whose append-decision is being serialized:
// a block-free natural key (e.g. "curve_config|<pool_id>",
// "uniswap_v3_tick|<pool_id>|<tick>"), shared by every writer of that table's
// append decision. It is deliberately distinct from the table's pv trigger
// lock key, which is a row-identity key that includes block number + version.
// A block-scoped lockKey would let two writers for the same entity at
// different blocks take different locks and not serialize, reopening the
// read-then-write race this helper exists to close.
//
// readLatest returns latest == nil when no prior row exists, in which case
// AppendOnChange inserts directly without calling changed. Otherwise changed
// receives that (guaranteed non-nil) latest and decides whether insert runs.
// insert is expected to use ON CONFLICT DO NOTHING so a replayed event stays
// idempotent.
func AppendOnChange[T any](
	ctx context.Context,
	tx pgx.Tx,
	lockKey string,
	readLatest func(ctx context.Context, tx pgx.Tx) (latest *T, err error),
	changed func(latest *T) bool,
	insert func(ctx context.Context, tx pgx.Tx) error,
) error {
	// Lock the key here so the helper is self-contained: any caller (e.g. a
	// single-key writer with no batch pre-lock) gets correct read-then-write
	// serialization. A caller that already holds this exact key in the same
	// transaction (the multi-pool config batch pre-locks all keys in sorted
	// order for cross-pool deadlock avoidance) re-acquires it here as a safe,
	// reference-counted no-op — xact advisory locks are reentrant and released
	// together at COMMIT/ROLLBACK.
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

	if latest != nil && !changed(latest) {
		return nil
	}

	if err := insert(ctx, tx); err != nil {
		return fmt.Errorf("inserting for %q: %w", lockKey, err)
	}
	return nil
}
