package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// lockAdvisoryKeys takes one transaction-scoped advisory lock per key in a single
// round-trip, in the order given. Callers pass keys already sorted on a total
// order every writer of the same domain agrees on; two transactions acquiring an
// overlapping pair in opposite orders deadlock.
//
// pg_advisory_xact_lock is taken left-to-right as unnest() yields rows, so
// ORDER BY ord is what preserves the caller's order.
//
// subject names the domain in the error, since the callers guard different
// read-latest-then-insert decisions (ADR-0006).
func lockAdvisoryKeys(ctx context.Context, tx pgx.Tx, keys []string, subject string) error {
	if len(keys) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended(k, 0))
		 FROM unnest($1::text[]) WITH ORDINALITY AS u(k, ord)
		 ORDER BY ord`,
		keys,
	); err != nil {
		return fmt.Errorf("locking %d %s: %w", len(keys), subject, err)
	}
	return nil
}
