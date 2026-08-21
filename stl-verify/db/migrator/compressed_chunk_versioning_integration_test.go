//go:build integration

package migrator_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestCompressedConvertedHypertablesHaveAVersionFunction guards the one thing that keeps
// ADR-0002's corrections-as-new-rows model working on a columnstored chunk: the INSERT,
// not the BEFORE INSERT trigger, has to decide processing_version.
//
// Why the trigger is too late — the arbiter resolves first, against a version still at its
// DEFAULT — is in 20260821_120000_morpho_adapter_state_version_function.sql and ADR-0002 §3.
// What a table needs to escape it is a next_processing_version_<table> function that its
// INSERT calls and its trigger delegates to, so both agree on the version and the lock key.
//
// Scoped to the strictly-append-only converted set rather than every compressed
// hypertable: those are the tables whose whole point is that a correction is a new row,
// and the set grows table by table (see db/migrations/AGENTS.md), so a future converted
// hypertable is covered here the moment it is added. The legacy tables share the defect
// and are deliberately not asserted on — converting them is its own work.
//
// A catalogue assertion, so it proves the function EXISTS, not that the table's writer
// calls it. The behavioural half is per-table and lives with the repository:
// TestSaveAdapterState_NewBuildAppendsIntoACompressedChunk.
func TestCompressedConvertedHypertablesHaveAVersionFunction(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	tables := compressedConvertedTables(t, ctx, pool)
	if len(tables) == 0 {
		t.Fatal("no converted table resolved as a compressed hypertable; either the " +
			"compression-settings query stopped matching or the converted list drifted")
	}

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			var exists bool
			if err := pool.QueryRow(ctx, `
				SELECT EXISTS (
					SELECT 1 FROM pg_proc p
					JOIN pg_namespace n ON n.oid = p.pronamespace
					WHERE n.nspname = 'public' AND p.proname = 'next_processing_version_' || $1
				)`, table).Scan(&exists); err != nil {
				t.Fatalf("look up the version function for %s: %v", table, err)
			}
			if !exists {
				t.Errorf("%s is a compressed hypertable with no next_processing_version_%s function, "+
					"so its INSERT can only leave processing_version to the trigger — every correction "+
					"row for a position an already-compressed chunk holds is silently dropped", table, table)
			}
		})
	}
}

// compressedConvertedTables returns the append-only converted tables that are compressed
// hypertables, read from TimescaleDB's own settings rather than a second hand-kept list —
// so a converted table that gains or loses compression moves itself in and out of scope.
func compressedConvertedTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT hypertable::text
		FROM timescaledb_information.hypertable_compression_settings
		WHERE hypertable::text = ANY($1)
		ORDER BY 1`, convertedAppendOnlyTables)
	if err != nil {
		t.Fatalf("query compressed converted hypertables: %v", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			t.Fatalf("scan compression-settings row: %v", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read compression-settings rows: %v", err)
	}
	return tables
}
