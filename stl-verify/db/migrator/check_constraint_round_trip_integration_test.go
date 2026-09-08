//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestHypertableCheckConstraintsSurviveTheTieringRoundTrip performs, for every
// hypertable, the attach Tiger Cloud's tiering performs when it adds the OSM chunk: a
// child carrying the parent's CHECK constraints re-created from their deparsed text,
// joined with ALTER TABLE ... INHERIT, which PostgreSQL refuses unless each constraint
// deparses to the same text as the parent's. The local harness has no tiering
// (add_tiering_policy is skipped with a NOTICE), so a shape that fails only there passed
// CI until it reached staging; plain-table clones reproduce the check because the
// comparison is PostgreSQL's, not the extension's. Every hypertable is covered, not only
// the tiered ones, because tiering is a policy added after the fact. The failing shapes
// are recorded under "Tiering round-trip trap" in db/migrations/AGENTS.md.
func TestHypertableCheckConstraintsSurviveTheTieringRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	rows, err := pool.Query(ctx, `
		SELECT format('%I.%I', hypertable_schema, hypertable_name)
		FROM timescaledb_information.hypertables
		ORDER BY 1`)
	if err != nil {
		t.Fatalf("list hypertables: %v", err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect hypertables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("no hypertables found; the enumeration is broken")
	}

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer func() { _ = tx.Rollback(ctx) }()

			// The parent clone keeps the stored constraint trees verbatim; the child gets
			// the same constraints, by name, re-parsed from their deparsed text.
			if _, err := tx.Exec(ctx,
				`CREATE TABLE round_trip_parent (LIKE `+table+` INCLUDING CONSTRAINTS)`,
			); err != nil {
				t.Fatalf("clone parent of %s: %v", table, err)
			}
			var childDDL string
			if err := tx.QueryRow(ctx, `
				SELECT format('CREATE TABLE round_trip_child (LIKE round_trip_parent%s)',
					coalesce(', ' || string_agg(
						format('CONSTRAINT %I CHECK (%s)', conname, pg_get_expr(conbin, conrelid)),
						', ' ORDER BY conname), ''))
				FROM pg_constraint
				WHERE conrelid = $1::regclass AND contype = 'c'`, table,
			).Scan(&childDDL); err != nil {
				t.Fatalf("build child DDL for %s: %v", table, err)
			}
			if _, err := tx.Exec(ctx, childDDL); err != nil {
				t.Fatalf("clone child of %s: %v\n%s", table, err, childDDL)
			}

			if _, err := tx.Exec(ctx, `ALTER TABLE round_trip_child INHERIT round_trip_parent`); err != nil {
				t.Fatalf("a CHECK constraint on %s does not survive the deparse/reparse round trip "+
					"tiering performs when it attaches the OSM chunk (see the tiering round-trip "+
					"trap in db/migrations/AGENTS.md): %v", table, err)
			}
		})
	}
}
