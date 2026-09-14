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
			// BEFORE INSERT ... FOR EACH ROW only: that is the shape that can assign
			// NEW.processing_version and so hand the compressed-chunk arbiter a DEFAULT. An AFTER,
			// statement-level or UPDATE/DELETE-only trigger cannot, so its table's INSERT still names the
			// column itself.
			if beforeInsertRowTriggers(ctx, t, pool, table) == 0 {
				t.Skipf("%s carries no BEFORE INSERT row trigger, so its INSERT supplies processing_version "+
					"itself and no version function applies (behaviour covered by TestPositionState/\"a "+
					"correction for a position an already-compressed chunk holds is stored, not dropped\")", table)
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

// beforeInsertRowTriggers counts the user triggers on table shaped to assign NEW.processing_version:
// BEFORE (tgtype & 2), FOR EACH ROW (tgtype & 1) and firing on INSERT (tgtype & 4). The INSERT bit keeps
// a BEFORE UPDATE or DELETE row trigger out of the count: it never sees the row the arbiter resolves.
func beforeInsertRowTriggers(ctx context.Context, t *testing.T, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, `
		SELECT count(*) FROM pg_trigger
		WHERE tgrelid = $1::regclass AND NOT tgisinternal
		  AND (tgtype & 2) = 2 AND (tgtype & 1) = 1 AND (tgtype & 4) = 4`, table).Scan(&n); err != nil {
		t.Fatalf("look up triggers on %s: %v", table, err)
	}
	return n
}

// The predicate that scopes the check above. A trigger that cannot fire on INSERT, or fires per statement,
// never sees the row the compressed-chunk arbiter resolves, so it must not pull its table back into scope:
// a BEFORE UPDATE row trigger is tgtype 19 (ROW|BEFORE|UPDATE) and matched a mask that read BEFORE and ROW
// but not INSERT.
func TestVersionFunctionGuardCountsOnlyBeforeInsertRowTriggers(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	defer cleanup()

	// The guard skips position_state on the premise that it carries none; TimescaleDB's own insert blocker
	// must stay out of the count for that to hold.
	if n := beforeInsertRowTriggers(ctx, t, pool, "position_state"); n != 0 {
		t.Fatalf("position_state already counts %d BEFORE INSERT row triggers; the guard's skip rests on 0", n)
	}
	if _, err := pool.Exec(ctx,
		`CREATE FUNCTION public.probe_noop() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`); err != nil {
		t.Fatalf("create the probe trigger function: %v", err)
	}
	for _, tc := range []struct {
		shape string
		adds  int
	}{
		{"BEFORE UPDATE ON position_state FOR EACH ROW", 0},
		{"BEFORE DELETE ON position_state FOR EACH ROW", 0},
		{"AFTER INSERT ON position_state FOR EACH ROW", 0},
		{"BEFORE INSERT ON position_state FOR EACH STATEMENT", 0},
		{"BEFORE INSERT ON position_state FOR EACH ROW", 1},
		{"BEFORE INSERT OR UPDATE ON position_state FOR EACH ROW", 1},
	} {
		t.Run(tc.shape, func(t *testing.T) {
			if _, err := pool.Exec(ctx, `CREATE TRIGGER probe_trg `+tc.shape+` EXECUTE FUNCTION public.probe_noop()`); err != nil {
				t.Fatalf("create the probe trigger: %v", err)
			}
			defer func() {
				if _, err := pool.Exec(ctx, `DROP TRIGGER probe_trg ON position_state`); err != nil {
					t.Errorf("drop the probe trigger: %v", err)
				}
			}()
			if got := beforeInsertRowTriggers(ctx, t, pool, "position_state"); got != tc.adds {
				t.Errorf("a %s trigger counts %d, want %d", tc.shape, got, tc.adds)
			}
		})
	}
}
