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

	// Converted tables that predate the version-function pattern and are queued on the
	// VEC-615 sweep. An entry here is a known hole, not a pass: remove it when the
	// table's function + INSERT-side call land.
	versionFunctionPending := map[string]string{
		"psm3_alm_shares": "VEC-615",
	}

	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			if ticket, ok := versionFunctionPending[table]; ok {
				t.Skipf("%s is on the %s sweep; corrections into its compressed chunks are dropped until then", table, ticket)
			}
			// Scope DERIVED, not listed. The defect needs a table that assigns its own
			// processing_version: the arbiter resolves against the DEFAULT because the BEFORE INSERT
			// trigger has not fired yet. A table with no such trigger cannot be in that shape -- its
			// INSERT must name the column -- and a next_processing_version_<table> for it would have to
			// invent a version its source never issued.
			//
			// Read from pg_trigger rather than kept as an exclusion list: a list is a fourth place a
			// table name has to be maintained (after AGENTS.md, convertedAppendOnlyTables and
			// schema_master), and it silently rots. This cannot: the moment such a table gains a
			// trigger it re-enters scope and fails below for want of the function.
			// Only a BEFORE INSERT ROW trigger can produce the defect: it is the one shape that can set
			// NEW.processing_version before the ON CONFLICT arbiter resolves. tgtype bits are 1 = ROW,
			// 2 = BEFORE, 4 = INSERT, and all three are required. Counting every trigger was too broad --
			// a cache maintainer's AFTER trigger, or a BEFORE ... FOR EACH STATEMENT trigger, matched it
			// and demanded a version function for a table whose INSERT already supplies the column.
			//
			// #644 makes this identical change, since position_current's maintainer exposes the same
			// over-breadth; whichever lands second drops its copy.
			var assigningTriggers int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM pg_trigger
				WHERE tgrelid = $1::regclass AND NOT tgisinternal
				  AND (tgtype & 3) = 3 AND (tgtype & 4) <> 0`, table).Scan(&assigningTriggers); err != nil {
				t.Fatalf("look up BEFORE INSERT row triggers on %s: %v", table, err)
			}
			if assigningTriggers == 0 {
				t.Skipf("%s carries no BEFORE INSERT row trigger, so its INSERT supplies processing_version itself and no "+
					"version function applies (behaviour covered by TestPositionState/\"a correction for "+
					"a position an already-compressed chunk holds is stored, not dropped\")", table)
			}
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
