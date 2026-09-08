//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
)

// TestHypertableCheckConstraintsSurviveTheTieringRoundTrip asserts that every CHECK
// constraint on every hypertable comes back identical after a deparse-and-reparse round
// trip through an inheritance child.
//
// That round trip is what Tiger Cloud's tiering performs the moment add_tiering_policy
// runs: it creates the OSM chunk as a foreign table carrying the hypertable's CHECK
// constraints re-parsed from their text form, then attaches it with
// ALTER TABLE ... INHERIT, and PostgreSQL refuses the attach unless each same-named
// constraint's parse tree is equal to the parent's. A BETWEEN nested inside a compound
// AND does not survive that: BETWEEN expands to its own AND node, so the parent keeps a
// nested tree while the re-parsed text flattens into a single AND, and the migration
// dies with
//
//	child table "osm_chunk_N" has different definition for check constraint ... (42804)
//
// which is how 20260819_120000_create_uniswap_v4_tables.sql blocked every staging
// deploy on 2026-09-07 (VEC-475). The local harness has no tiering (add_tiering_policy is
// skipped with a NOTICE), so the migration passed CI while failing on staging. This test
// runs the same attach against plain-table clones, which is sufficient because the
// comparison is PostgreSQL's, not the extension's. It covers every hypertable, not just
// the tiered ones: tiering is a policy added after the fact, and the shape is wrong on
// its own.
func TestHypertableCheckConstraintsSurviveTheTieringRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	rows, err := pool.Query(ctx, `
		SELECT hypertable_name
		FROM timescaledb_information.hypertables
		WHERE hypertable_schema = 'public'
		ORDER BY hypertable_name`)
	if err != nil {
		t.Fatalf("list hypertables: %v", err)
	}
	tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatalf("collect hypertables: %v", err)
	}
	if len(tables) == 0 {
		t.Fatal("no hypertables found in public; the enumeration is broken")
	}

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			defer func() { _ = tx.Rollback(ctx) }()

			// The parent clone copies the stored constraint trees verbatim.
			if _, err := tx.Exec(ctx,
				`CREATE TABLE round_trip_parent (LIKE `+pgx.Identifier{table}.Sanitize()+` INCLUDING CONSTRAINTS)`,
			); err != nil {
				t.Fatalf("clone parent of %s: %v", table, err)
			}

			// The child gets the same constraints, by name, re-parsed from their deparsed
			// text, exactly as the OSM chunk does.
			var childDDL string
			if err := tx.QueryRow(ctx, `
				SELECT format('CREATE TABLE round_trip_child (LIKE %I%s)', $1::text,
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
					"that tiering performs when it attaches the OSM chunk; rewrite it without a "+
					"BETWEEN nested inside AND/OR: %v", table, err)
			}
		})
	}
}
