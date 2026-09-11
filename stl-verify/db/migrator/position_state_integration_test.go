//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// mppCols is the aliased column list the projection-view contract requires.
const mppCols = `v(chain_id,protocol_id,instrument_key,holder_id,quantity,deal_type,block_number,block_version,processing_version,block_timestamp)`

var (
	lineCommentRE         = regexp.MustCompile(`(?m)--.*$`)
	onConflictDoNothingRE = regexp.MustCompile(`(?is)ON\s+CONFLICT\b[^;]*\bDO\s+NOTHING\b`)
	doUpdateRE            = regexp.MustCompile(`(?is)\bDO\s+UPDATE\b`)
)

// psFixture owns the one migrated schema TestPositionState and its topic groups share, plus the
// helpers every group needs. It exists so the groups can live in their own functions instead of one
// 1,900-line body, WITHOUT each paying a container start and an ApplyAll (stl-verify/AGENTS.md,
// "Share setup, don't repeat it"). Subtests stay safe against each other by using distinct
// instrument_keys, exactly as before.
type psFixture struct {
	ctx  context.Context
	pool *pgxpool.Pool
}

// newPositionStateFixture applies every migration to a fresh database and disables its scheduled jobs.
// setupMigratedPostgres, not bare setupPostgres+ApplyAll: left scheduled, policy_compression can fire
// mid-subtest -- every fixture here is stamped 2026-01/02, already past the 2-day window -- taking
// AccessExclusiveLock per chunk and flaking the concurrency and physical-shape assertions. The policy
// ROW is still asserted; only its scheduling is off.
func newPositionStateFixture(t *testing.T) (*psFixture, func()) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	return &psFixture{ctx: ctx, pool: pool}, cleanup
}

// mpp (re)creates a projection view from a VALUES body, runs the materializer expecting success, and
// returns the rows it inserted -- so a suppressed write is visible to assertions instead of passing as
// a silent no-op.
func (f *psFixture) mpp(t *testing.T, name, valuesBody, reason string) int64 {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `CREATE OR REPLACE VIEW `+name+` AS `+valuesBody); err != nil {
		t.Fatalf("create view %s: %v", name, err)
	}
	var inserted int64
	if err := f.pool.QueryRow(f.ctx, `SELECT materialize_position_projection($1::regclass)`, name).Scan(&inserted); err != nil {
		t.Fatalf("materialize %s: %v", name, err)
	}
	return inserted
}

// mppErr runs the materializer expecting an error whose text contains want.
func (f *psFixture) mppErr(t *testing.T, name, valuesBody, reason, want string) {
	t.Helper()
	if _, err := f.pool.Exec(f.ctx, `CREATE OR REPLACE VIEW `+name+` AS `+valuesBody); err != nil {
		t.Fatalf("create view %s: %v", name, err)
	}
	_, err := f.pool.Exec(f.ctx, `SELECT materialize_position_projection($1::regclass)`, name)
	if err == nil {
		t.Fatalf("%s: expected an error containing %q, got none", name, want)
	}
	if want != "" && !strings.Contains(err.Error(), want) {
		t.Errorf("%s: error = %v, want it to contain %q", name, err, want)
	}
}

// mppN is mpp with an int count, for subtests asserting an observation was actually INSERTED rather
// than merely that the call did not error.
func (f *psFixture) mppN(t *testing.T, name, valuesBody, reason string) int {
	t.Helper()
	return int(f.mpp(t, name, valuesBody, reason))
}

// row builds one contract-shaped projection row. Pure, so it needs no fixture.
func row(ik, holder string, qty int, code string, bn, bv, pv int) string {
	return "(1::int,10::bigint,'" + ik + "'::text,'" + holder + "'::text," + strconv.Itoa(qty) + "::numeric,'" + code + "'::text," +
		strconv.Itoa(bn) + "::bigint," + strconv.Itoa(bv) + "::int," + strconv.Itoa(pv) + "::int,'2026-01-01'::timestamptz)"
}

// valuesOf wraps rows into a projection body carrying the contract's column names.
func valuesOf(rows ...string) string {
	return `SELECT * FROM (VALUES ` + strings.Join(rows, ",") + `) ` + mppCols
}

// TestPositionState drives the real materialize_position_projection through one shared schema (a single
// ApplyAll) across the adversarial cases from this PR's review history — every subtest fails on the
// pre-fix code it guards. Subtests use distinct instrument_keys so they share state safely; setup is
// shared per stl-verify/AGENTS.md ("Share setup, don't repeat it").
func TestPositionState(t *testing.T) {
	f, cleanup := newPositionStateFixture(t)
	defer cleanup()

	// A mutation sweep was run against an earlier revision of this migration and its results used to
	// live here as a table. It has been removed rather than updated, for two reasons. It had gone false:
	// roughly twenty of its rows named guards and subtests belonging to the classification design that
	// left this PR (a BEFORE trigger, position_classification writes, a canonicality filter, a
	// model-based fuzz oracle), and the table had been reordered under prose that indexed into it by row
	// number, so its own commentary pointed at the wrong rows. And it was the wrong artefact for a source
	// tree: a mutation sweep is a point-in-time experiment against one revision, so an in-tree copy
	// decays silently while reading as current coverage -- which is exactly what happened, and it
	// misrepresented coverage to reviewers in the meantime.
	//
	// Current sweep results belong in the PR description, where they can be corrected. What stays here is
	// the property every subtest below is written to have: each fails on the specific defect it names,
	// and says in its own comment what that defect is.

	// --- recency guard ---
	psTestRecencyGuard(t, f)
	// --- input robustness ---
	psTestInputRobustness(t, f)
	// --- structure / physical ---
	psTestStructurePhysical(t, f)
	// --- identity integrity (position_key contract, not exercised above) ---
	psTestIdentityIntegrityPositionkeyContract(t, f)
	// --- contract edges (missing / extra column, FK) ---
	psTestContractEdgesMissingExtra(t, f)
	// --- quantity CHECK (negative, NaN) ---
	psTestQuantityCheckNegativeNan(t, f)
	// --- empty projection, return count, multi-view txn, concurrency ---
	psTestEmptyProjectionReturnCount(t, f)
	// --- atomicity / NOT-NULL observation columns ---
	psTestAtomicityNotnullObservationColumns(t, f)
	// --- coordinate / structural-id CHECKs (round 5; each fails on the pre-fix code, which stored these) ---
	psTestCoordinateStructuralidChecksRound(t, f)
	// --- narrowed grants (round 5): append-only must hold against history REWRITES, not just DELETE ---
	psTestNarrowedGrantsRound5(t, f)
	// --- ported input matrix (previously scratch-only) ---
	psTestPortedInputMatrixPreviously(t, f)
	// --- ACL completions and semantic pins ---
	psTestAclCompletionsAndSemantic(t, f)
	// --- round-9 fixes (each reproduced fail-first on the pre-fix code) ---
	psTestRound9FixesEachReproduced(t, f)
	// --- model-based fuzz ---
	psTestModelbasedFuzz(t, f)
	// --- guards a data assertion cannot see: asserted on the mechanism ---
	psTestGuardsADataAssertion(t, f)
	// --- deal_type: carry, type gate, vocabulary gate (VEC-401) ---
	psTestDealTypeCode(t, f)
	// --- deal_type: the direct-INSERT path the materializer gates cannot reach ---
	psTestDealTypeCodeDirectInsert(t, f)
	// --- this migration must survive a re-apply (restore, partial-apply recovery) ---
	psTestDealTypeCodeMigrationIsReRunnable(t, f)
	// --- a higher block cannot carry an earlier instant, or the two caches disagree about now ---
	psTestBlockTimeMonotonicPerPosition(t, f)
	// --- every completed run is recorded, so a stale position is detectable ---
	psTestCompletedRunIsRecorded(t, f)
	// --- off-chain observations: chain NULL, block_number = epoch of the instant, never mixed ---
	psTestClosure(t, f)
	psTestOffChainObservations(t, f)
	// --- run_id provenance: every row the spine writes names its writer run (ADR-0006 §2) ---
	psTestRunIDProvenance(t, f)
}

// psTestRecencyGuard covers: recency guard
func psTestRecencyGuard(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mppN := f.mppN

	t.Run("every leg of the insert-suppression predicate admits a genuinely new observation", func(t *testing.T) {
		// The predicate suppresses an insert only when position_id, block_number, block_version AND
		// processing_version all match a stored row. Drop any single leg and a reorg or a reprocess
		// reads as already-present, so the observation is DISCARDED -- no error, no warning, and a
		// return count of 0 that is indistinguishable from a clean rerun. Measured with block_version
		// dropped: the reorg case below returns 0 and leaves 1 row where 2 are correct.
		//
		// The previous version of this subtest made two mpp() calls and asserted nothing, so all three
		// version legs could be deleted with the suite still green.
		cases := []struct {
			name         string
			bn, bv, pv   int
			wantInserted int
		}{
			{"the original observation", 100, 0, 0, 1},
			{"a reorg bumps block_version at the same block", 100, 1, 0, 1},
			{"a reprocess bumps processing_version at the same block and version", 100, 1, 1, 1},
			{"a later block", 101, 0, 0, 1},
			{"re-emitting a stored observation inserts nothing", 100, 1, 0, 0},
		}
		want := 0
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := mppN(t, "vlegs", valuesOf(row("ilegs", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", tc.bn, tc.bv, tc.pv)), tc.name)
				if got != tc.wantInserted {
					t.Errorf("materialize inserted %d rows; want %d. A dropped leg of the suppression predicate silently discards this observation",
						got, tc.wantInserted)
				}
				want += tc.wantInserted
				var stored int
				if err := pool.QueryRow(ctx,
					`SELECT count(*) FROM position_state WHERE position_id = position_id(1,10,'ilegs','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&stored); err != nil {
					t.Fatal(err)
				}
				if stored != want {
					t.Errorf("position_state holds %d observations for this position; want %d", stored, want)
				}
			})
		}
	})
}

// psTestInputRobustness covers: input robustness
func psTestInputRobustness(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("numeric(30,18) quantity passes the contract (finding :197)", func(t *testing.T) {
		mpp(t, "vtm", `SELECT 1::int chain_id,10::bigint protocol_id,'itm'::text instrument_key,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text holder_id,`+
			`5::numeric(30,18) quantity,'LOAN'::text deal_type,100::bigint block_number,0::int block_version,`+
			`0::int processing_version,'2026-01-01'::timestamptz block_timestamp`, "typmod")
	})

	t.Run("float8 quantity fails the contract", func(t *testing.T) {
		mppErr(t, "vfl", `SELECT 1::int chain_id,10::bigint protocol_id,'ifl'::text instrument_key,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text holder_id,`+
			`1.5::float8 quantity,'LOAN'::text deal_type,1::bigint block_number,0::int block_version,`+
			`0::int processing_version,'2026-01-01'::timestamptz block_timestamp`, "flt", "column contract")
	})

	t.Run("double-emitted logical key raises", func(t *testing.T) {
		mppErr(t, "vde", valuesOf(row("ide", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 7, "LOAN", 600, 0, 0), row("ide", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 9, "LOAN", 600, 0, 0)), "de", "double-emit")
	})

	t.Run("re-emitted observation with a changed block_timestamp does NOT raise (kept-stored)", func(t *testing.T) {
		// Superseded by the round-9 :296 decision: raising wedged at-least-once wall-clock sources
		// forever. The run succeeds, warns, and keeps the stored row (asserted in detail by the
		// dedicated :296 subtest below).
		mpp(t, "vts", valuesOf(row("its", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0)), "store")
		mpp(t, "vts", `SELECT * FROM (VALUES (1::int,10::bigint,'its'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-09-09'::timestamptz)) `+mppCols, "shift")
		var ts string
		if err := pool.QueryRow(ctx, `SELECT block_timestamp::text FROM position_state WHERE position_id = position_id(1,10,'its','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&ts); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(ts, "2026-01-01") {
			t.Errorf("stored block_timestamp = %s; want the original 2026-01-01 kept", ts)
		}
	})

	t.Run("uppercase holder rejected by the hex CHECK", func(t *testing.T) {
		mppErr(t, "vuh", `SELECT * FROM (VALUES (1::int,10::bigint,'iuh'::text,'0xAB'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) `+mppCols, "uh", "position_state_holder_hex_chk")
	})

	t.Run("Infinity quantity is refused by name before the write", func(t *testing.T) {
		mppErr(t, "vinf", `SELECT * FROM (VALUES (1::int,10::bigint,'iinf'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,'Infinity'::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) `+mppCols, "inf", "negative or non-finite quantity")
	})
}

// psTestStructurePhysical covers: structure / physical
func psTestStructurePhysical(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mpp := f.mpp

	t.Run("temp-table drop spares a permanent _mpp_src (finding :206)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE TABLE public._mpp_src (sentinel int)`); err != nil {
			t.Fatalf("create permanent _mpp_src: %v", err)
		}
		defer pool.Exec(ctx, `DROP TABLE IF EXISTS public._mpp_src`)
		if _, err := pool.Exec(ctx, `INSERT INTO public._mpp_src VALUES (777)`); err != nil {
			t.Fatal(err)
		}
		mpp(t, "vp", valuesOf(row("ip", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0)), "perm")
		var sentinel int
		if err := pool.QueryRow(ctx, `SELECT sentinel FROM public._mpp_src`).Scan(&sentinel); err != nil {
			t.Fatalf("the materializer dropped the caller's permanent _mpp_src: %v", err)
		}
		if sentinel != 777 {
			t.Errorf("permanent _mpp_src sentinel = %d; want 777", sentinel)
		}
	})

	t.Run("NULL p_view raises instead of silently skipping the lock", func(t *testing.T) {
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection(NULL::regclass)`)
		if err == nil || !strings.Contains(err.Error(), "p_view must not be NULL") {
			t.Errorf("NULL p_view: got %v; want the explicit raise", err)
		}
	})

	t.Run("no default block_timestamp index (finding :100)", func(t *testing.T) {
		var present bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass('position_state_block_timestamp_idx') IS NOT NULL`).Scan(&present); err != nil {
			t.Fatal(err)
		}
		if present {
			t.Error("create_hypertable added a default block_timestamp index; expected create_default_indexes => FALSE")
		}
	})

	t.Run("position_state is a hypertable with 1-day chunks", func(t *testing.T) {
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM timescaledb_information.hypertables WHERE hypertable_name = 'position_state'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("position_state is not a hypertable (rows=%d); create_hypertable missing", n)
		}
		var interval string
		if err := pool.QueryRow(ctx, `SELECT time_interval::text FROM timescaledb_information.dimensions WHERE hypertable_name = 'position_state'`).Scan(&interval); err != nil {
			t.Fatal(err)
		}
		if interval != "1 day" {
			t.Errorf("chunk interval = %q; want 1 day", interval)
		}
	})

	t.Run("advisory lock key is search_path-independent (finding :169/:24, via pg_locks)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW lockprobe AS `+valuesOf(row("ilk", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1, "LOAN", 1, 0, 0))); err != nil {
			t.Fatal(err)
		}
		// Discriminating fixture, second attempt. The first tried varying the CALLER's search_path, and a
		// review proved that tautological for a reason worth recording: the function carries
		// SET search_path FROM CURRENT, so the caller's path never reaches the body and p_view::text
		// rendered identically both times — a text-keyed mutation survived.
		//
		// What DOES vary inside the body is "$user". The pinned value is `search_path="$user", public`,
		// and "$user" re-resolves to current_user at call time, so the same regclass oid renders
		// differently depending on WHO calls: measured, 'stl_readwrite.lockprobe'::regclass::text is
		// `stl_readwrite.lockprobe` for a caller whose "$user" schema does not exist, and bare
		// `lockprobe` for stl_readwrite, whose "$user" schema does. So the fixture varies the ROLE, not
		// the search_path, and a p_view::text key differs across the two calls while the catalogue-derived
		// key does not.
		if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS lockshadow`); err != nil {
			t.Fatal(err)
		}
		// Both fixture schemas are dropped when this subtest finishes. Left behind, the role-named one
		// (stl_readwrite) keeps `"$user"` resolving to a real schema for every LATER subtest, so the suite
		// sat in the very shape this subtest constructs to prove a vulnerability -- and any later
		// unqualified reference could resolve there instead of public.
		t.Cleanup(func() {
			for _, sch := range []string{"lockshadow", "stl_readwrite"} {
				if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS `+sch+` CASCADE`); err != nil {
					t.Errorf("drop fixture schema %s: %v", sch, err)
				}
			}
		})
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW lockshadow.lockprobe AS `+valuesOf(row("ilkshadow", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1, "LOAN", 1, 0, 0))); err != nil {
			t.Fatal(err)
		}
		// A schema named for the role, so "$user" resolves to it for stl_readwrite and to nothing for the
		// harness superuser. The view inside it is the one both calls will name.
		for _, stmt := range []string{
			`CREATE SCHEMA IF NOT EXISTS stl_readwrite`,
			`CREATE OR REPLACE VIEW stl_readwrite.lockprobe AS ` + valuesOf(row("ilkuser", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 1, "LOAN", 1, 0, 0)),
			`GRANT USAGE ON SCHEMA stl_readwrite TO stl_readwrite`,
			`GRANT SELECT ON stl_readwrite.lockprobe TO stl_readwrite`,
		} {
			if _, err := pool.Exec(ctx, stmt); err != nil {
				t.Fatalf("fixture %q: %v", stmt, err)
			}
		}
		// Call the REAL function inside a transaction under a given search_path, then read the advisory
		// lock it is holding (xact lock, still held pre-commit). Comparing the held (classid,objid) across
		// two search_paths pins the actual key — a hardcoded copy of the expression could not.
		heldLock := func(asRole, view string) (int64, int64) {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			// SET LOCAL: scoped to this transaction and discarded on rollback, so search_path cannot leak
			// back to the pooled connection. pgxpool reuses connections without resetting session state, and
			// a leaked `pg_catalog, public` would make a later test's unqualified CREATE VIEW resolve to
			// pg_catalog and fail with permission denied (42501).
			if asRole != "" {
				if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+asRole); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := tx.Exec(ctx, `SELECT materialize_position_projection($1::regclass)`, view); err != nil {
				t.Fatalf("materialize %s as role %q: %v", view, asRole, err)
			}
			var classid, objid int64
			if err := tx.QueryRow(ctx,
				`SELECT classid::bigint, objid::bigint FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() ORDER BY classid, objid LIMIT 1`).Scan(&classid, &objid); err != nil {
				t.Fatalf("read advisory lock as role %q: %v", asRole, err)
			}
			return classid, objid
		}
		// SAME regclass oid, two callers whose "$user" resolves differently.
		c1, o1 := heldLock("", "stl_readwrite.lockprobe")
		c2, o2 := heldLock("stl_readwrite", "stl_readwrite.lockprobe")
		if c1 != c2 || o1 != o2 {
			t.Errorf("advisory lock key differs by CALLER: harness=(%d,%d) stl_readwrite=(%d,%d); a p_view::text key renders \"stl_readwrite.lockprobe\" for one and \"lockprobe\" for the other, so the per-view lock would not serialize concurrent runs", c1, o1, c2, o2)
		}
		// Distinctness, the other half: stability alone is satisfied by keying on the bare relname, which
		// would make two same-named views in different schemas share one lock and serialize needlessly.
		c3, o3 := heldLock("", "lockshadow.lockprobe")
		if c1 == c3 && o1 == o3 {
			t.Errorf("same-named views in different schemas share advisory lock (%d,%d); the key ignores the schema", c3, o3)
		}
	})
}

// psTestIdentityIntegrityPositionkeyContract covers: identity integrity (position_key contract, not exercised above)
func psTestIdentityIntegrityPositionkeyContract(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("blank holder_id rejected even on a zero-quantity row", func(t *testing.T) {
		// Identity fields feed position_id() for EVERY row, unlike deal_type (pre-flight (4) checks
		// only non-zero rows). A blank holder must fail regardless of quantity — here on a zero row.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ihb'::text,''::text,0::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vhb", body, "blankholder", "holder_id is required")
	})

	t.Run("null holder_id rejected", func(t *testing.T) {
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ihn'::text,NULL::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vhn", body, "nullholder", "holder_id is required")
	})

	t.Run("semicolon in instrument_key rejected (id-collision guard)", func(t *testing.T) {
		// The ';' delimiter is unescaped in position_key; an instrument_key containing it could collide two
		// distinct identities onto one id, so it must be rejected rather than hashed.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'a;b'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vsemi", body, "semi", "delimiter")
	})

	t.Run("null chain_id is legal and does not collide with a set chain_id", func(t *testing.T) {
		// chain_id/protocol_id are nullable structural fields (render empty in position_key). A NULL-chain
		// position is OFF-CHAIN, so its block_number is its instant in epoch seconds (1767225600 =
		// 2026-01-01T00:00:00Z); it must materialize AND hash distinctly from the same instrument/holder
		// at chain 1.
		body := `SELECT * FROM (VALUES ` +
			`(NULL::int,10::bigint,'inc'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,1767225600::bigint,0::int,0::int,'2026-01-01'::timestamptz),` +
			`(1::int,10::bigint,'inc'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,9::numeric,'BORROW'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mpp(t, "vnc", body, "nullchain")
		var nullRows, setRows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE position_id = position_id(NULL, 10, 'inc', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&nullRows); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE position_id = position_id(1, 10, 'inc', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&setRows); err != nil {
			t.Fatal(err)
		}
		if nullRows != 1 || setRows != 1 {
			t.Errorf("null vs set chain collided: null=%d set=%d rows; want 1 each (distinct ids)", nullRows, setRows)
		}
	})
}

// psTestContractEdgesMissingExtra covers: contract edges (missing / extra column, FK)
func psTestContractEdgesMissingExtra(t *testing.T, f *psFixture) {
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("view missing a contract column is rejected as MISSING", func(t *testing.T) {
		// Drop block_number, a still-required column; the contract check must name it MISSING before any
		// write. (This used to drop deal_type, which is no longer part of the required contract --
		// so the case silently stopped testing anything the moment that column left.)
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'imz'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,0::int,0::int,'2026-01-01'::timestamptz)) v(chain_id,protocol_id,instrument_key,holder_id,quantity,block_version,processing_version,block_timestamp)`
		mppErr(t, "vmz", body, "missing", "block_number (MISSING)")
	})

	t.Run("extra column in the view is tolerated", func(t *testing.T) {
		// The temp projection selects contract columns by name, so an unrelated extra column is ignored.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ix'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz,'ignored'::text)) v(chain_id,protocol_id,instrument_key,holder_id,quantity,deal_type,block_number,block_version,processing_version,block_timestamp,extra)`
		mpp(t, "vx", body, "extra")
	})
}

// psTestQuantityCheckNegativeNan covers: quantity CHECK (negative, NaN)
func psTestQuantityCheckNegativeNan(t *testing.T, f *psFixture) {
	mppErr := f.mppErr

	t.Run("negative quantity is refused by name before the write", func(t *testing.T) {
		mppErr(t, "vneg", valuesOf(row("ineg", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", -5, "LOAN", 100, 0, 0)), "neg", "negative or non-finite quantity")
	})

	t.Run("NaN quantity is refused by name before the write", func(t *testing.T) {
		// NaN sorts above every finite numeric, so it would clear closure's quantity > 0; the pre-write
		// check names it, and the table CHECK still guards a direct INSERT.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'inan'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,'NaN'::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vnan", body, "nan", "negative or non-finite quantity")
	})
}

// psTestEmptyProjectionReturnCount covers: empty projection, return count, multi-view txn, concurrency
func psTestEmptyProjectionReturnCount(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("p_build_id is stamped on the rows written, and defaults to 0", func(t *testing.T) {
		// The two-argument form was called nowhere, so replacing p_build_id with a literal 0 in the
		// INSERT was an unkilled mutation: every row would be stamped "pre-tracking" and the ADR-0002
		// provenance column would be uniformly wrong but entirely plausible.
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vbid AS `+valuesOf(row("ibid", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vbid'::regclass, 4242)`).Scan(&n); err != nil {
			t.Fatalf("materialize with an explicit build id: %v", err)
		}
		if n != 1 {
			t.Fatalf("inserted %d rows; want 1", n)
		}
		var got int
		if err := pool.QueryRow(ctx,
			`SELECT build_id FROM position_state WHERE position_id = position_id(1,10,'ibid','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != 4242 {
			t.Errorf("build_id = %d; want 4242 -- the p_build_id argument is not reaching the row", got)
		}

		// And the documented default really is 0, not some other constant.
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vbid0 AS `+valuesOf(row("ibid0", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection('vbid0'::regclass)`); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT build_id FROM position_state WHERE position_id = position_id(1,10,'ibid0','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Errorf("build_id = %d with the argument omitted; want 0 (pre-tracking)", got)
		}
	})

	t.Run("compression is configured and its policy exists", func(t *testing.T) {
		// Deleting the policy, or changing the segmentby/orderby, was an unkillable mutation. Unlike
		// tiering (a Cloud primitive no CI engine has) compression is available in OSS TimescaleDB, so
		// this is assertable here.
		var enabled bool
		if err := pool.QueryRow(ctx,
			`SELECT compression_enabled FROM timescaledb_information.hypertables
			  WHERE hypertable_name = 'position_state'`).Scan(&enabled); err != nil {
			t.Fatal(err)
		}
		if !enabled {
			t.Error("compression is not enabled on position_state")
		}

		var segmentBy, orderBy string
		if err := pool.QueryRow(ctx, `
			SELECT coalesce(string_agg(attname, ',') FILTER (WHERE segmentby_column_index IS NOT NULL), ''),
			       coalesce(string_agg(attname || CASE WHEN orderby_asc THEN ' ASC' ELSE ' DESC' END,
			                           ',' ORDER BY orderby_column_index)
			                FILTER (WHERE orderby_column_index IS NOT NULL), '')
			  FROM timescaledb_information.compression_settings
			 WHERE hypertable_name = 'position_state'`).Scan(&segmentBy, &orderBy); err != nil {
			t.Fatal(err)
		}
		if segmentBy != "position_id" {
			t.Errorf("compress_segmentby = %q; want position_id (the entity key)", segmentBy)
		}
		// TimescaleDB appends the partition column to orderby if the DDL omits it, so the stored setting
		// is the version tuple followed by block_timestamp -- not what the ALTER TABLE literally says.
		// Asserted as stored, with the version tuple required to LEAD so chunk batches are ordered for
		// the latest-per-position read rather than by time alone.
		if orderBy != "block_number DESC,block_version DESC,processing_version DESC,block_timestamp DESC" {
			t.Errorf("compress_orderby = %q; want the version tuple leading, then the appended partition column", orderBy)
		}

		var jobs int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM timescaledb_information.jobs
			  WHERE hypertable_name = 'position_state' AND proc_name = 'policy_compression'`).Scan(&jobs); err != nil {
			t.Fatal(err)
		}
		if jobs != 1 {
			t.Errorf("policy_compression jobs = %d; want 1", jobs)
		}
		// The lag was unasserted, so changing 2 days to 30 survived. 2 days is the repo-wide default.
		var lag string
		if err := pool.QueryRow(ctx,
			`SELECT (config->>'compress_after') FROM timescaledb_information.jobs
			  WHERE hypertable_name = 'position_state' AND proc_name = 'policy_compression'`).Scan(&lag); err != nil {
			t.Fatal(err)
		}
		if lag != "2 days" {
			t.Errorf("compression lag = %q; want \"2 days\"", lag)
		}
	})

	t.Run("a correction for a position an already-compressed chunk holds is stored, not dropped", func(t *testing.T) {
		// The exact failure TestCompressedConvertedHypertablesHaveAVersionFunction (on main) says a
		// compressed converted hypertable suffers without a next_processing_version_<table> function:
		// "every correction row for a position an already-compressed chunk holds is silently dropped".
		// The subtest above only proves a *different* position lands in a compressed chunk, which is a
		// weaker claim. This drives the guard's own scenario: same position, chunk already compressed,
		// then a source correction on each of the two correction axes.
		seed := row("icorr", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 500, 0, 0)
		mpp(t, "vcorr", valuesOf(seed), "seed the position")
		if _, err := pool.Exec(ctx, `SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('position_state') c`); err != nil {
			t.Fatalf("compress: %v", err)
		}
		var holds bool
		if err := pool.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1 FROM timescaledb_information.chunks ch
			  WHERE ch.hypertable_name = 'position_state' AND ch.is_compressed
			    AND EXISTS (SELECT 1 FROM position_state p
			                WHERE p.position_id = position_id(1,10,'icorr','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
			                  AND p.block_timestamp >= ch.range_start
			                  AND p.block_timestamp <  ch.range_end))`).Scan(&holds); err != nil {
			t.Fatal(err)
		}
		if !holds {
			t.Fatal("the seeded position is not inside a compressed chunk, so this subtest would prove nothing")
		}

		// The corrections re-emit through the SAME view, since a position is owned by one projection;
		// a real projection re-emits its history and the anti-join suppresses what is already stored.
		// processing_version is the axis the guard names: the source reprocessed the same block.
		pvFix := row("icorr", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 7, "LOAN", 500, 0, 1)
		if n := mpp(t, "vcorr", valuesOf(seed, pvFix), "processing_version correction"); n != 1 {
			t.Errorf("the processing_version correction inserted %d rows; want 1 (it was dropped)", n)
		}
		// block_version is the other correction axis (a reorg at the same block).
		bvFix := row("icorr", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 8, "LOAN", 500, 1, 0)
		if n := mpp(t, "vcorr", valuesOf(seed, pvFix, bvFix), "block_version correction"); n != 1 {
			t.Errorf("the block_version correction inserted %d rows; want 1 (it was dropped)", n)
		}

		type obs struct{ bv, pv, qty int }
		want := []obs{{0, 0, 5}, {0, 1, 7}, {1, 0, 8}}
		rows, err := pool.Query(ctx, `
			SELECT block_version, processing_version, quantity FROM position_state
			 WHERE position_id = position_id(1,10,'icorr','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')
			 ORDER BY block_version, processing_version`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var got []obs
		for rows.Next() {
			var o obs
			if err := rows.Scan(&o.bv, &o.pv, &o.qty); err != nil {
				t.Fatal(err)
			}
			got = append(got, o)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatalf("the position has %d observations %v; want %d %v -- a correction was dropped by the "+
				"compressed chunk", len(got), got, len(want), want)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("observation %d = %+v; want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("the write path works against a COMPRESSED chunk", func(t *testing.T) {
		// The compression subtests above assert catalogue state only, so every load-bearing sentence in
		// the migration's compression paragraphs was unfalsifiable by CI. Compression is available in OSS
		// TimescaleDB (unlike tiering), so the behaviour IS testable here.
		mpp(t, "vcmp", valuesOf(row("icmp", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0)), "seed before compressing")
		if _, err := pool.Exec(ctx, `SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('position_state') c`); err != nil {
			t.Fatalf("compress: %v", err)
		}
		var compressed int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name = 'position_state' AND is_compressed`).Scan(&compressed); err != nil {
			t.Fatal(err)
		}
		if compressed == 0 {
			t.Fatal("no chunk is compressed, so this subtest would prove nothing")
		}

		// A re-projection of unchanged history must insert nothing and not error: the NOT EXISTS
		// anti-join filters every row before ON CONFLICT sees it, which is why the decompression limit
		// is never reached on this write path.
		var again int64
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vcmp'::regclass)`).Scan(&again); err != nil {
			t.Fatalf("re-projection over compressed chunks: %v", err)
		}
		if again != 0 {
			t.Errorf("re-projection inserted %d rows; want 0", again)
		}

		// A genuinely new observation must still land, in a compressed chunk, and be readable.
		mpp(t, "vcmp2", valuesOf(row("icmp2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 9, "LOAN", 101, 0, 0)), "new observation into a compressed chunk")
		var qty int
		if err := pool.QueryRow(ctx,
			`SELECT quantity FROM position_state WHERE position_id = position_id(1,10,'icmp2','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&qty); err != nil {
			t.Fatalf("the new observation was not stored: %v", err)
		}
		if qty != 9 {
			t.Errorf("stored quantity = %d; want 9", qty)
		}

		// And the PK is still enforced across the compressed/uncompressed boundary.
		var dupes int
		if err := pool.QueryRow(ctx, `
			SELECT count(*) FROM (
			  SELECT 1 FROM position_state
			  GROUP BY position_id, block_number, block_version, processing_version, block_timestamp
			  HAVING count(*) > 1) d`).Scan(&dupes); err != nil {
			t.Fatal(err)
		}
		if dupes != 0 {
			t.Errorf("%d duplicate logical keys after writing to compressed chunks; the PK is not enforced there", dupes)
		}

		// Hand the shared schema back uncompressed. This subtest compressed EVERY chunk, so without
		// this every later subtest ran columnstore-only -- a state none of them chose and none assert,
		// which quietly changes what they exercise.
		if _, err := pool.Exec(ctx,
			`SELECT decompress_chunk(c, if_compressed => true) FROM show_chunks('position_state') c`); err != nil {
			t.Fatalf("decompress teardown: %v", err)
		}
	})

	t.Run("the tiering block is present, narrow, and set to 1 year", func(t *testing.T) {
		// Asserted on the migration's TEXT, which is the only option: add_tiering_policy is a Timescale
		// Cloud primitive that does not exist on any CI engine, so the DO block's success path is never
		// executed here and every behavioural mutation to it survives. Precedent for reading a migration
		// as the assertion is append_only_grants_integration_test.go and migrator_integration_test.go.
		raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), "20260818_130000_create_position_state.sql"))
		if err != nil {
			t.Fatalf("read migration: %v", err)
		}
		sql := string(raw)
		for _, want := range []struct{ what, text string }{
			{"the tiering call", "PERFORM add_tiering_policy('position_state', INTERVAL '1 year', if_not_exists => TRUE);"},
			{"a NARROW handler", "EXCEPTION WHEN undefined_function OR feature_not_supported THEN"},
		} {
			if !strings.Contains(sql, want.text) {
				t.Errorf("migration is missing %s: %q", want.what, want.text)
			}
		}
		// WHEN OTHERS would swallow a permission error, a policy conflict or a typo'd table name and
		// report them all as "unavailable, skipping" -- via a NOTICE the production migrator discards.
		if strings.Contains(sql, "EXCEPTION WHEN OTHERS") {
			t.Error("migration contains EXCEPTION WHEN OTHERS; every handler here must name its SQLSTATEs")
		}
	})

	t.Run("a dangling regclass oid raises before the lock is taken", func(t *testing.T) {
		// The migration documents this as reproduced -- no lock acquired (hashtextextended is STRICT so a
		// NULL key is a silent no-op) and projection <> v_qualname NULL for every row, so every ownership
		// violation passes. It had no test; only the literal-NULL arm did.
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection(2147483647::oid::regclass)`)
		if err == nil {
			t.Fatal("a dangling regclass oid was accepted; the run would proceed lock-free with the ownership check disabled")
		}
		if !strings.Contains(err.Error(), "does not name an existing relation") {
			t.Errorf("error = %v; want the dangling-relation guard to be the one that fired", err)
		}
	})

	t.Run("block zero is legal, and a zero quantity is stored as the close of a position", func(t *testing.T) {
		// Both bounds are inclusive: genesis is block 0, and a zero quantity is the closing observation.
		// A lone zero is a leading zero and is not an observation, so the close follows a positive.
		mpp(t, "vzero", valuesOf(row("izero", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 0, 0, 0), row("izero", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 0, "LOAN", 1, 0, 0)), "block 0 then a close")
		var stored []string
		if err := pool.QueryRow(ctx,
			`SELECT array_agg(block_number::text || '=' || quantity::text ORDER BY block_number) FROM position_state WHERE position_id = position_id(1,10,'izero','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`,
		).Scan(&stored); err != nil {
			t.Fatalf("read: %v", err)
		}
		if strings.Join(stored, ",") != "0=5,1=0" {
			t.Errorf("stored %v; want the block-0 open and the zero close", stored)
		}
	})

	t.Run("NOT NULL holds on every column a direct INSERT can reach", func(t *testing.T) {
		// instrument_key, holder_id and created_at are unreachable as NULL through the materializer --
		// position_key() raises first -- but stl_readwrite holds INSERT, and the suite already tests that
		// path for the position_id width. Dropping any of these NOT NULLs stored a NULL.
		base := map[string]string{
			"position_id": "sha256('nn'::bytea)", "chain_id": "1", "protocol_id": "10",
			"instrument_key": "'inn'", "holder_id": "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "quantity": "5",
			"block_number": "100", "block_version": "0", "processing_version": "0",
			"block_timestamp": "'2026-01-01'::timestamptz", "projection": "'public.v'", "build_id": "0",
		}
		order := []string{"position_id", "chain_id", "protocol_id", "instrument_key", "holder_id", "quantity",
			"block_number", "block_version", "processing_version", "block_timestamp", "projection", "build_id"}
		for _, nullCol := range []string{"instrument_key", "holder_id", "created_at"} {
			t.Run("a NULL "+nullCol, func(t *testing.T) {
				cols, vals := append([]string{}, order...), []string{}
				for _, c := range order {
					if c == nullCol {
						vals = append(vals, "NULL")
					} else {
						vals = append(vals, base[c])
					}
				}
				if nullCol == "created_at" {
					cols = append(cols, "created_at")
					vals = append(vals, "NULL")
				}
				_, err := pool.Exec(ctx, `INSERT INTO position_state (`+strings.Join(cols, ",")+`) VALUES (`+strings.Join(vals, ",")+`)`)
				if err == nil {
					t.Fatalf("a NULL %s was accepted", nullCol)
				}
				if !strings.Contains(err.Error(), `null value in column "`+nullCol+`"`) {
					t.Errorf("error = %v; want the %s NOT NULL to fire", err, nullCol)
				}
			})
		}
	})

	t.Run("the genesis instant itself is legal", func(t *testing.T) {
		// ts_sane is `>= '2009-01-03 00:00:00+00'`, and the boundary being inclusive is deliberate --
		// genesis is a legal block time. Tightening it to a strict `>` survived, because nothing stored
		// exactly that instant.
		mpp(t, "vgen", `SELECT 1::int chain_id,10::bigint protocol_id,'igen'::text instrument_key,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text holder_id,`+
			`5::numeric quantity,'LOAN'::text deal_type,0::bigint block_number,0::int block_version,`+
			`0::int processing_version,'2009-01-03 00:00:00+00'::timestamptz block_timestamp`, "genesis")
		var ts string
		if err := pool.QueryRow(ctx,
			`SELECT block_timestamp::text FROM position_state WHERE position_id = position_id(1,10,'igen','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&ts); err != nil {
			t.Fatalf("the genesis instant was not stored: %v", err)
		}
	})

	t.Run("the insert arm is ON CONFLICT DO NOTHING", func(t *testing.T) {
		// Not behaviourally reachable: NOT EXISTS plus pre-flight (2) plus the per-view advisory lock mean
		// no test can present a conflicting row, so removing the clause entirely is invisible. But the
		// whole compression-safety argument in this migration rests on the arm existing and never being
		// DO UPDATE, so it is asserted on the shipped text, as the tiering block is.
		var src string
		if err := pool.QueryRow(ctx,
			`SELECT prosrc FROM pg_proc WHERE proname = 'materialize_position_projection'`).Scan(&src); err != nil {
			t.Fatal(err)
		}
		// Strip -- comments first, and match the CLAUSE rather than the words. The body discusses the
		// conflict arm in prose, so a bare substring match on the raw source passes even with the real
		// clause deleted -- measured: it did. Same comment-stripping the qualification class-check uses.
		code := lineCommentRE.ReplaceAllString(src, "")
		if !onConflictDoNothingRE.MatchString(code) {
			t.Error("the insert has no ON CONFLICT ... DO NOTHING arm; compression safety and the append-only rule both depend on it")
		}
		if doUpdateRE.MatchString(code) {
			t.Error("the function contains a DO UPDATE arm; that requires UPDATE privilege and breaks append-only")
		}
	})

	t.Run("a NULL quantity and a NULL build_id are rejected", func(t *testing.T) {
		// NOT NULL is the ONLY guard on either, because a CHECK admits NULL: `quantity >= 0 AND
		// quantity <> 'NaN' AND quantity < 'Infinity'` evaluates to NULL for a NULL input, and so does
		// `build_id >= 0`. Measured with the NOT NULL dropped, both store successfully through the
		// sanctioned write path -- a NULL quantity poisons every downstream SUM, which is the same
		// failure the '< Infinity' leg exists for, and a NULL build_id makes ADR-0002 provenance blank
		// for a whole run. Neither is repairable: UPDATE and DELETE are revoked.
		nullQty := `SELECT 1::int chain_id,10::bigint protocol_id,'inq'::text instrument_key,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text holder_id,` +
			`NULL::numeric quantity,'LOAN'::text deal_type,100::bigint block_number,0::int block_version,` +
			`0::int processing_version,'2026-01-01'::timestamptz block_timestamp`
		mppErr(t, "vnq", nullQty, "null quantity", "quantity=NULL")

		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vnb AS `+valuesOf(row("inb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection('vnb'::regclass, NULL::integer)`)
		if err == nil {
			t.Fatal("a NULL build_id was accepted through the public signature")
		}
		// The argument guard fires first now, which is the point of it: the column's NOT NULL only
		// fired on runs that actually inserted, so a NULL p_build_id passed silently on a no-op run.
		// The column constraint stays proven by "NOT NULL holds on every column a direct INSERT can
		// reach".
		if !strings.Contains(err.Error(), "p_build_id must not be NULL") {
			t.Errorf("error = %v; want the p_build_id argument guard to be what fires", err)
		}
	})

	t.Run("a negative build_id is rejected", func(t *testing.T) {
		// p_build_id is caller-supplied and lands in the coordinate CHECK. Dropping the build_id term
		// from that constraint survived the suite because no test ever passed a negative one.
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vnegb AS `+valuesOf(row("inegb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection('vnegb'::regclass, -1)`)
		if err == nil {
			t.Fatal("a negative build_id was accepted")
		}
		if !strings.Contains(err.Error(), "coord_nonneg") {
			t.Errorf("error = %v; want the coordinate CHECK to be the one that fired", err)
		}
	})

	t.Run("projection is NOT NULL", func(t *testing.T) {
		// A NULL projection makes the ownership check's `own.projection <> v_qualname` evaluate NULL for
		// every row, silently disabling cross-view enforcement -- the same NULL-comparison trap the
		// migration guards for v_qualname. Dropping the NOT NULL survived the suite.
		var notNull bool
		if err := pool.QueryRow(ctx, `
			SELECT attnotnull FROM pg_attribute
			 WHERE attrelid = 'public.position_state'::regclass AND attname = 'projection'`).Scan(&notNull); err != nil {
			t.Fatal(err)
		}
		if !notNull {
			t.Error("position_state.projection is nullable; a NULL there disables the ownership check")
		}
	})

	t.Run("the materializer pins timescaledb.enable_tiered_reads", func(t *testing.T) {
		// Asserted on the mechanism because it is not behaviourally killable here: tiered storage is a
		// Timescale Cloud primitive, so no CI engine can produce a tiered chunk to read. Without the pin,
		// every read in the function answers "what is new / what drifted / who owns this" over local
		// chunks only once the tiering policy has fired -- and it fires on the first historical backfill,
		// because the partition column is on-chain time.
		var cfg []string
		if err := pool.QueryRow(ctx,
			`SELECT proconfig FROM pg_proc WHERE proname = 'materialize_position_projection'`).Scan(&cfg); err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, c := range cfg {
			if strings.HasPrefix(c, "timescaledb.enable_tiered_reads=") {
				found = true
				if !strings.HasSuffix(c, "=on") {
					t.Errorf("proconfig has %q; want it set to on", c)
				}
			}
		}
		if !found {
			t.Errorf("proconfig = %v; want a timescaledb.enable_tiered_reads=on entry", cfg)
		}
	})

	t.Run("return value counts rows inserted, and nothing else", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vrc AS `+valuesOf(row("irc", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0), row("irc", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 6, "LOAN", 110, 0, 0))); err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vrc'::regclass)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("first run inserted %d; want 2", n)
		}
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vrc'::regclass)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("identical rerun changed %d; want 0", n)
		}
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vrc AS `+valuesOf(row("irc", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0), row("irc", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 99, "LOAN", 110, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vrc'::regclass)`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("a same-key changed quantity reported %d inserts; want 0 (append-only: kept-stored + warned)", n)
		}
		var q int
		if err := pool.QueryRow(ctx, `SELECT quantity::int FROM position_state WHERE position_id = position_id(1,10,'irc','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa') AND block_number = 110`).Scan(&q); err != nil {
			t.Fatal(err)
		}
		if q != 6 {
			t.Errorf("stored quantity = %d; want 6 kept (the drifted 99 must not be applied)", q)
		}
	})

	t.Run("two views in one transaction both materialize (temp reuse + no self-deadlock)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vt1 AS `+valuesOf(row("it1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vt2 AS `+valuesOf(row("it2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 6, "BORROW", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		// Second call's DROP TABLE IF EXISTS pg_temp._mpp_src must clear the first call's temp table (its
		// ON COMMIT DROP has not fired mid-transaction), and a single caller acquiring two per-view locks
		// must not self-deadlock.
		if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('vt1'::regclass)`); err != nil {
			t.Fatalf("first view: %v", err)
		}
		if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('vt2'::regclass)`); err != nil {
			t.Fatalf("second view (temp reuse / lock): %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("concurrent same-view runs serialize on the advisory lock", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vlock AS `+valuesOf(row("ilk2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		// conn A holds the per-view xact advisory lock (open transaction, uncommitted).
		connA, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connA.Release()
		txA, err := connA.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txA.Rollback(ctx)
		if _, err := txA.Exec(ctx, `SELECT materialize_position_projection('vlock'::regclass)`); err != nil {
			t.Fatalf("conn A: %v", err)
		}
		// conn B on the same view must block on the advisory lock; a transaction-scoped statement_timeout
		// interrupts the wait, proving B was excluded (a broken/absent lock would let B finish instantly).
		// SET LOCAL + the deferred rollback keep the timeout scoped to this tx, so it can't leak back to the
		// pool the way a session-level SET + ignored RESET could.
		// Assert WHAT B is blocked on (review finding: a bare statement-timeout is satisfiable by the
		// speculative-insert XactLockTableWait even with the advisory lock deleted). A third connection
		// polls pg_locks for a NOT-granted advisory lock while B is waiting.
		done := make(chan error, 1)
		go func() {
			_, err := pool.Exec(ctx, `SELECT materialize_position_projection('vlock'::regclass)`)
			done <- err
		}()
		sawAdvisoryWait := false
		for i := 0; i < 100; i++ {
			var waiting int
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted`).Scan(&waiting); err != nil {
				t.Fatal(err)
			}
			if waiting > 0 {
				sawAdvisoryWait = true
				break
			}
			select {
			case err := <-done:
				t.Fatalf("conn B completed (err=%v) while conn A held the lock; the per-view lock did not serialize", err)
			default:
			}
			if _, err := pool.Exec(ctx, `SELECT pg_sleep(0.05)`); err != nil {
				t.Fatal(err)
			}
		}
		if !sawAdvisoryWait {
			t.Fatal("never observed B waiting on an ungranted ADVISORY lock; the block may be PK speculative-insert, not the per-view lock")
		}
		if err := txA.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("conn B failed after A released: %v", err)
		}
	})
}

// psTestAtomicityNotnullObservationColumns covers: atomicity / NOT-NULL observation columns
func psTestAtomicityNotnullObservationColumns(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("a within-write failure leaves existing rows untouched (atomicity)", func(t *testing.T) {
		// Seed a position, then run a view emitting a new observation for it AND a second position whose
		// holder violates the hex CHECK at write time (the pre-write checks do not look at holder format). The failure must roll back the WHOLE statement: the seeded row is
		// untouched and the poison position wrote nothing.
		mpp(t, "vat", valuesOf(row("iat", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0)), "seed")
		poison := valuesOf(row("iat", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 50, "LOAN", 200, 0, 0), row("iat2", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", 7, "LOAN", 100, 0, 0))
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vat AS `+poison); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection('vat'::regclass)`); err == nil || !strings.Contains(err.Error(), "position_state_holder_hex_chk") {
			t.Fatalf("poison run: got %v; want the holder hex CHECK failure (proves the failure happened mid-write)", err)
		}
		var unchanged bool
		if err := pool.QueryRow(ctx, `SELECT quantity = 5 FROM position_state WHERE position_id = position_id(1,10,'iat','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&unchanged); err != nil {
			t.Fatal(err)
		}
		if !unchanged {
			t.Error("existing quantity changed after a rolled-back run; want 5 (no partial write)")
		}
		var poisonRows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE position_id = position_id(1,10,'iat2','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&poisonRows); err != nil {
			t.Fatal(err)
		}
		if poisonRows != 0 {
			t.Errorf("poison position wrote %d rows; want 0 (all-or-nothing)", poisonRows)
		}
	})

	t.Run("null block_number rejected (NOT NULL observation column)", func(t *testing.T) {
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ibn'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,NULL::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		// Named by the NULL-ness pre-flight now, before any write; the column's own NOT NULL is
		// proven independently by "NOT NULL holds on every column a direct INSERT can reach".
		mppErr(t, "vbn", body, "bn", "block_number=NULL")
	})

	t.Run("null block_timestamp rejected (NOT NULL partition column)", func(t *testing.T) {
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ibt'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,NULL::timestamptz)) ` + mppCols
		mppErr(t, "vbt", body, "bt", "block_timestamp=NULL")
	})
}

// psTestCoordinateStructuralidChecksRound covers: coordinate / structural-id CHECKs (round 5; each fails on the pre-fix code, which stored these)
func psTestCoordinateStructuralidChecksRound(t *testing.T, f *psFixture) {
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("negative block_number rejected (coordinate CHECK)", func(t *testing.T) {
		// Pre-fix this was stored silently and — the guard orders on these columns — became the
		// position's "oldest" observation, skewing classification recency.
		mppErr(t, "vnbn", valuesOf(row("inbn", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", -42, 0, 0)), "nbn", "position_state_coord_nonneg_chk")
	})

	t.Run("negative block_version and processing_version rejected", func(t *testing.T) {
		mppErr(t, "vnbv", valuesOf(row("inbv", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, -1, 0)), "nbv", "position_state_coord_nonneg_chk")
		mppErr(t, "vnpv", valuesOf(row("inpv", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, -1)), "npv", "position_state_coord_nonneg_chk")
	})

	t.Run("zero or negative chain_id / protocol_id rejected (NULL stays legal)", func(t *testing.T) {
		// Registry ids are strictly positive; 0 is an upstream zero-value default, and because the value
		// feeds the position_id hash a wrong-but-accepted id forks the position permanently.
		body := `SELECT * FROM (VALUES (0::int,10::bigint,'izc'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vzc", body, "zc", "chain_pos")
		body = `SELECT * FROM (VALUES (1::int,0::bigint,'izp'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vzp", body, "zp", "protocol_pos")
	})

	t.Run("two positions at one block may carry different timestamps (event-time sources)", func(t *testing.T) {
		// block_timestamp is each source's observation time, invariant only per logical key (pre-flight
		// check 3), NOT a table-wide function of block_number: Sky prime_debt uses synced_at, so two
		// positions at the same block legitimately differ. Pins the Sky-enabling semantics so a future
		// "consistency" check doesn't silently break the #627 materializer.
		body := `SELECT * FROM (VALUES ` +
			`(1::int,10::bigint,'idv1'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,777::bigint,0::int,0::int,'2026-01-01'::timestamptz),` +
			`(1::int,10::bigint,'idv2'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,6::numeric,'BORROW'::text,777::bigint,0::int,0::int,'2026-02-01'::timestamptz)) ` + mppCols
		mpp(t, "vdv", body, "eventtime")
	})

	t.Run("pre-blockchain block_timestamp rejected (epoch-corruption guard)", func(t *testing.T) {
		// A hex-parse bug writing epoch-zero would otherwise silently create a 1970 chunk on the
		// partition column and poison time-ordered reads.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'iep'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'1970-01-01+00'::timestamptz)) ` + mppCols
		mppErr(t, "vep", body, "ep", "ts_sane")
	})
}

// psTestNarrowedGrantsRound5 covers: narrowed grants (round 5): append-only must hold against history REWRITES, not just DELETE
func psTestNarrowedGrantsRound5(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool

	t.Run("narrowed grants: writer path intact, history rewrite denied (stl_readwrite)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW aclv AS `+valuesOf(row("iaclr", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `GRANT SELECT ON aclv TO stl_readwrite`); err != nil {
			t.Fatal(err)
		}
		// asRole runs stmts in one transaction under SET LOCAL ROLE (role scope ends with the tx, so
		// nothing leaks back to the pooled connection), returning the first error.
		asRole := func(stmts ...string) error {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, `SET LOCAL ROLE stl_readwrite`); err != nil {
				t.Fatal(err)
			}
			for _, s := range stmts {
				if _, err := tx.Exec(ctx, s); err != nil {
					return err
				}
			}
			return tx.Commit(ctx)
		}
		// The one sanctioned writer path — the full materializer — must run under the narrowed grants.
		if err := asRole(`SELECT materialize_position_projection('aclv'::regclass)`); err != nil {
			t.Fatalf("materializer as stl_readwrite failed under narrowed grants: %v", err)
		}
		// EVERY rewrite must be denied, quantity included: there is no sanctioned update channel on this
		// table. (An earlier version of this comment said quantity stayed sanctioned, which the assertion
		// immediately below contradicts.)
		denied := [][2]string{
			{"position_state identity", `UPDATE position_state SET holder_id = 'bb' WHERE false`},
			{"position_state coordinates", `UPDATE position_state SET block_number = 1 WHERE false`},
			{"position_state delete", `DELETE FROM position_state WHERE false`},
		}
		for _, d := range denied {
			if err := asRole(d[1]); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("%s: want permission denied, got %v", d[0], err)
			}
		}
		if err := asRole(`UPDATE position_state SET quantity = quantity WHERE false`); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("quantity update: want permission denied (the update channel was removed; append-only default), got %v", err)
		}
		// Owner-side REVOKE (#737: a stray fix-migration must fail loudly; nothing FKs position_state,
		// so the ref-table FK/KEY SHARE caveat does not apply). Asserted via the catalogue because the
		// harness superuser bypasses ACLs.
		// The owner-side REVOKE is guarded by role existence: stl_migrator comes from the infra
		// bootstrap and no migration creates it, so it is ABSENT under this harness and the REVOKE never
		// executes here. That means CI does not cover it -- log the skip rather than passing silently,
		// because a green run must not read as evidence the owner-side revoke works.
		var migratorExists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'stl_migrator')`).Scan(&migratorExists); err != nil {
			t.Fatal(err)
		}
		if migratorExists {
			var ownUpd, ownDel bool
			if err := pool.QueryRow(ctx,
				`SELECT has_table_privilege('stl_migrator','position_state','UPDATE'), has_table_privilege('stl_migrator','position_state','DELETE')`).Scan(&ownUpd, &ownDel); err != nil {
				t.Fatal(err)
			}
			if ownUpd || ownDel {
				t.Errorf("owner-side privileges present (UPDATE=%v DELETE=%v); want both revoked", ownUpd, ownDel)
			}
		} else {
			t.Log("stl_migrator absent: the owner-side REVOKE was NOT executed or asserted in this run. " +
				"Deleting it from the migration would not fail here -- it is covered only where the infra " +
				"bootstrap has created the role.")
		}
		// A second run at a higher processing_version, as the login role: proves the whole materializer
		// path works under the narrowed grants, not just the first insert. Reuses the SAME view, because
		// one projection owns a position and a correction is a new processing_version from that view.
		// (An earlier version of this comment claimed it forced a position_classification DO UPDATE arm
		// and protected a column-scoped UPDATE grant. This migration writes no classification and grants
		// nothing on it, so there was no such arm and no such grant line to delete.)
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW aclv AS `+valuesOf(row("iaclr", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "COLLATERAL", 100, 0, 1))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `GRANT SELECT ON aclv TO stl_readwrite`); err != nil {
			t.Fatal(err)
		}
		if err := asRole(`SELECT materialize_position_projection('aclv'::regclass)`); err != nil {
			t.Fatalf("a correction run (higher processing_version) as stl_readwrite failed: %v", err)
		}
	})
}

// psTestPortedInputMatrixPreviously covers: ported input matrix (previously scratch-only)
func psTestPortedInputMatrixPreviously(t *testing.T, f *psFixture) {
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("input matrix: boundary and encoding cases", func(t *testing.T) {
		// Table-driven port of the scratch matrix cases the hand-written subtests above don't cover:
		// boundary numerics, encoding edges, and vocabulary-miss shapes. want == "" expects success.
		// Fields are SQL expressions: chain/proto/ik/holder/qty get their casts appended by the
		// template; code carries its own cast (so NULL and '' shapes stay expressible).
		cases := []struct {
			name   string
			chain  string
			proto  string
			ik     string
			holder string
			qty    string
			code   string
			bn, bv string
			want   string
		}{
			{"holder 0x prefix rejected", "1", "10", "'m1'", "'0xabc1'", "5", "'LOAN'::text", "100", "0", "holder_hex"},
			{"holder non-hex g rejected", "1", "10", "'m2'", "'gg11'", "5", "'LOAN'::text", "100", "0", "holder_hex"},
			{"whitespace-only instrument rejected", "1", "10", "'   '", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "0", "instrument_key is required"},
			{"whitespace-only holder rejected", "1", "10", "'m3'", "'  '", "5", "'LOAN'::text", "100", "0", "holder_id is required"},
			{"uppercase instrument is legal", "1", "10", "'REGISTRY:ILK-A'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "0", ""},
			// Was pinned as legal, which asserted the wrong thing: the bridge's PK btree caps an index
			// entry at 2704 bytes, so a key that long can never have a bridge row and resolves to
			// nothing, forever. Now capped at 2,000 characters and rejected here.
			{"10k-char instrument is rejected (never resolvable via the bridge)", "1", "10", "repeat('x',10000)", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "0", "position_state_instrument_key_len_chk"},
			{"a 2000-char instrument is still legal (the cap itself)", "1", "10", "repeat('y',2000)", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "0", ""},
			{"unicode instrument is legal", "1", "10", "'ilk-Ω-θ'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "0", ""},
			{"quantity 1e30 is legal", "1", "10", "'m4'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "1e30", "'LOAN'::text", "100", "0", ""},
			{"quantity 1e-18 dust is legal", "1", "10", "'m5'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "0.000000000000000001", "'LOAN'::text", "100", "0", ""},
			{"chain_id int32 max is legal", "2147483647", "10", "'m8'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "0", ""},
			{"protocol_id int64 max is legal", "1", "9223372036854775807", "'m9'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "0", ""},
			{"block_number 2^62 is legal", "1", "10", "'m10'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "4611686018427387904", "0", ""},
			{"block_version int32 max is legal", "1", "10", "'m11'", "'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'", "5", "'LOAN'::text", "100", "2147483647", ""},
		}
		for i, tc := range cases {
			body := fmt.Sprintf(
				`SELECT * FROM (VALUES (%s::int,%s::bigint,%s::text,%s::text,%s::numeric,%s,%s::bigint,%s::int,0::int,'2026-01-01'::timestamptz)) `+mppCols,
				tc.chain, tc.proto, tc.ik, tc.holder, tc.qty, tc.code, tc.bn, tc.bv)
			view := "vmat" + strconv.Itoa(i)
			if tc.want == "" {
				mpp(t, view, body, "matrix")
			} else {
				mppErr(t, view, body, "matrix", tc.want)
			}
		}
	})
}

// psTestAclCompletionsAndSemantic covers: ACL completions and semantic pins
func psTestAclCompletionsAndSemantic(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool

	t.Run("TRUNCATE denied and stl_readonly is read-only", func(t *testing.T) {
		asRoleOne := func(role, stmt string) error {
			conn, err := pool.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Release()
			tx, err := conn.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, `SET LOCAL ROLE `+role); err != nil {
				t.Fatal(err)
			}
			_, err = tx.Exec(ctx, stmt)
			return err
		}
		// TRUNCATE is a privilege of its own; the DELETE denial does not imply it.
		for _, tbl := range []string{"position_state"} {
			if err := asRoleOne("stl_readwrite", `TRUNCATE `+tbl); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("TRUNCATE %s as stl_readwrite: want permission denied, got %v", tbl, err)
			}
		}
		if err := asRoleOne("stl_readonly", `SELECT count(*) FROM position_state`); err != nil {
			t.Errorf("SELECT as stl_readonly failed: %v", err)
		}
		if err := asRoleOne("stl_readonly", `INSERT INTO position_state (position_id, chain_id, protocol_id,
			instrument_key, holder_id, quantity, block_number, block_version, processing_version,
			block_timestamp, projection) VALUES (position_id(1,10,'iro','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'),1,10,'iro','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',1,1,0,0,'2026-01-01','v')`); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("INSERT as stl_readonly: want permission denied, got %v", err)
		}
	})

	t.Run("1-byte position_id rejected by the width CHECK (direct insert)", func(t *testing.T) {
		// Unreachable through the function (position_id() always emits 32 bytes); pin the table CHECK.
		_, err := pool.Exec(ctx, `INSERT INTO position_state (position_id, instrument_key, holder_id, quantity, block_number, block_timestamp, projection)
			VALUES ('\x00'::bytea, 'x', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 1, 100, '2026-01-01', 'direct-test')`)
		if err == nil || !strings.Contains(err.Error(), "id_len") {
			t.Errorf("1-byte position_id: want id_len_chk violation, got %v", err)
		}
	})

	t.Run("temp snapshot is not left behind after a run", func(t *testing.T) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, `CREATE OR REPLACE VIEW vtl AS `+valuesOf(row("itl", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `SELECT materialize_position_projection('vtl'::regclass)`); err != nil {
			t.Fatal(err)
		}
		var leaked bool
		if err := conn.QueryRow(ctx, `SELECT to_regclass('pg_temp._mpp_src') IS NOT NULL`).Scan(&leaked); err != nil {
			t.Fatal(err)
		}
		if leaked {
			t.Error("pg_temp._mpp_src survived the call in the same session")
		}
	})

	t.Run("different views on disjoint positions run concurrently without blocking", func(t *testing.T) {
		// The per-view advisory lock must serialize same-view runs ONLY: a second view touching a
		// disjoint position must complete while the first holds its transaction open.
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vcna AS `+valuesOf(row("icna", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vcnb AS `+valuesOf(row("icnb", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "BORROW", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		connA, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connA.Release()
		txA, err := connA.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txA.Rollback(ctx)
		if _, err := txA.Exec(ctx, `SELECT materialize_position_projection('vcna'::regclass)`); err != nil {
			t.Fatal(err)
		}
		connB, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer connB.Release()
		txB, err := connB.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer txB.Rollback(ctx)
		if _, err := txB.Exec(ctx, `SET LOCAL statement_timeout = '3s'`); err != nil {
			t.Fatal(err)
		}
		if _, err := txB.Exec(ctx, `SELECT materialize_position_projection('vcnb'::regclass)`); err != nil {
			t.Fatalf("a different view on a disjoint position blocked behind the first run: %v", err)
		}
		if err := txB.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})
}

// psTestRound9FixesEachReproduced covers: round-9 fixes (each reproduced fail-first on the pre-fix code)
func psTestRound9FixesEachReproduced(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("cross-view ownership is enforced: a second view on the same position raises (:193)", func(t *testing.T) {
		mpp(t, "vown1", valuesOf(row("iown", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0)), "owner")
		body := valuesOf(row("iown", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 7, "BORROW", 110, 0, 0))
		mppErr(t, "vown2", body, "thief", "owned by another projection")
		// and the owning view keeps working
		mpp(t, "vown1", valuesOf(row("iown", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0), row("iown", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 6, "LOAN", 120, 0, 0)), "owner-again")
	})

	t.Run("a re-emitted key with a changed block_timestamp is kept-stored, not a wedge (:296)", func(t *testing.T) {
		// At-least-once wall-clock sources (Sky's synced_at on an SQS retry) legitimately re-emit a
		// stored key with a new timestamp. Pre-fix this raised on every run, forever, with no repair
		// path. Now: warn, keep the stored row, insert nothing, and the run keeps succeeding.
		mpp(t, "vtsd", valuesOf(row("itsd", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0)), "seed")
		drift := `SELECT * FROM (VALUES (1::int,10::bigint,'itsd'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01 00:00:07+00'::timestamptz)) ` + mppCols
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vtsd AS `+drift); err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vtsd'::regclass)`).Scan(&n); err != nil {
			t.Fatalf("timestamp drift wedged the run: %v", err)
		}
		if n != 0 {
			t.Errorf("drift run inserted %d rows; want 0 (kept-stored)", n)
		}
		var rows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE position_id = position_id(1,10,'itsd','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&rows); err != nil {
			t.Fatal(err)
		}
		if rows != 1 {
			t.Errorf("stored rows = %d; want 1 (no duplicate under the 5-column PK)", rows)
		}
	})

	t.Run("pg_temp shadows of the permanent tables are defeated (:312)", func(t *testing.T) {
		// PostgreSQL searches the session temp schema FIRST for relation names, so a pre-created
		// pg_temp shadow must not absorb the writes. Empirically: SET search_path FROM CURRENT alone
		// did NOT defeat this (pg_temp is implicitly first when not explicitly listed) — the
		// schema-qualification in the function is what does.
		//
		// Everything runs inside ONE transaction with ON COMMIT DROP temp tables and a rollback, so
		// the shadows cannot outlive this subtest. Temp tables live for the whole SESSION, and this
		// connection goes back to the pool: a leaked shadow would silently redirect every later
		// test's unqualified reads to an empty table (that exact leak turned this suite red once).
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		for _, ddl := range []string{
			`CREATE TEMP TABLE position_state (LIKE public.position_state INCLUDING ALL) ON COMMIT DROP`,
			// position_classification and ref_deal_type were shadowed here too. This migration
			// references neither, so those two decoys asserted nothing; the catalog decoys that DO
			// bite are exercised in the subtest below.
		} {
			if _, err := tx.Exec(ctx, ddl); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := tx.Exec(ctx, `CREATE OR REPLACE VIEW vshadow AS `+valuesOf(row("ishadow", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('vshadow'::regclass)`); err != nil {
			t.Fatalf("run under shadow tables: %v", err)
		}
		var realRows, shadowRows int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM public.position_state WHERE position_id = position_id(1,10,'ishadow','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa')`).Scan(&realRows); err != nil {
			t.Fatal(err)
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM pg_temp.position_state`).Scan(&shadowRows); err != nil {
			t.Fatal(err)
		}
		if realRows != 1 || shadowRows != 0 {
			t.Errorf("shadow absorbed the write: public=%d shadow=%d; want 1/0", realRows, shadowRows)
		}
		// Rolled back by the deferred Rollback: the shadows and the probe row both disappear, so the
		// pooled connection is returned clean.
	})
}

// psTestModelbasedFuzz covers: model-based fuzz
func psTestModelbasedFuzz(t *testing.T, f *psFixture) {

	// The oracle below is a second implementation of the invariant by the same author, not an
	// independent one: across the 32-mutation sweep it never killed a mutation the hand-written
	// subtests missed, so it is a cross-check on the state space, not evidence on its own.
}

// psTestGuardsADataAssertion covers: guards a data assertion cannot see: asserted on the mechanism
func psTestGuardsADataAssertion(t *testing.T, f *psFixture) {
	ctx, pool := f.ctx, f.pool
	mpp, mppErr := f.mpp, f.mppErr

	t.Run("block_timestamp and quantity drift each raise a WARNING (kept-stored is not silent)", func(t *testing.T) {
		// The drift checks keep the stored row and warn (:296), so every data assertion survives a
		// mutation that downgrades the RAISE or reads a pg_temp shadow: the warning is the observable.
		cfg, err := pgx.ParseConfig(pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var notices []string
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
			mu.Lock()
			defer mu.Unlock()
			notices = append(notices, n.SeverityUnlocalized+": "+n.Message)
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)

		run := func(body, label string) {
			t.Helper()
			if _, err := conn.Exec(ctx, `CREATE OR REPLACE VIEW vwarn AS `+body); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `SELECT materialize_position_projection('vwarn'::regclass)`); err != nil {
				t.Fatalf("run %s: %v", label, err)
			}
		}
		warned := func(want string) bool {
			mu.Lock()
			defer mu.Unlock()
			for _, n := range notices {
				if strings.HasPrefix(n, "WARNING") && strings.Contains(n, want) {
					return true
				}
			}
			return false
		}
		run(valuesOf(row("iwarn", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 5, "LOAN", 100, 0, 0)), "store")
		if warned("changed block_timestamp") || warned("changed quantity") {
			t.Fatalf("the first store already warned; fixture is not clean: %v", notices)
		}
		run(`SELECT * FROM (VALUES (1::int,10::bigint,'iwarn'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-09-09'::timestamptz)) `+mppCols, "ts-drift")
		if !warned("changed block_timestamp") {
			t.Errorf("no WARNING for block_timestamp drift; notices = %v", notices)
		}
		run(valuesOf(row("iwarn", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 7, "LOAN", 100, 0, 0)), "qty-drift")
		if !warned("changed quantity") {
			t.Errorf("no WARNING for quantity drift; notices = %v", notices)
		}
	})

	t.Run("a drift check does not fire on a sibling row at the same block", func(t *testing.T) {
		// Both drift checks join the snapshot to the history on the FULL logical key. Dropping a leg
		// widens the join to sibling rows, and because the message is built from the snapshot side a
		// widened join emits the same text twice rather than different text -- so content cannot
		// discriminate. What does: a SPURIOUS warning. Re-emit a row unchanged while a sibling at the same
		// block carries a different value, and a widened join warns when nothing drifted.
		cfg, err := pgx.ParseConfig(pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var notices []string
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
			mu.Lock()
			defer mu.Unlock()
			notices = append(notices, n.SeverityUnlocalized+": "+n.Message)
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		run := func(body, label string) {
			t.Helper()
			if _, err := conn.Exec(ctx, `CREATE OR REPLACE VIEW vsib AS `+body); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `SELECT materialize_position_projection('vsib'::regclass)`); err != nil {
				t.Fatalf("run %s: %v", label, err)
			}
		}
		warnedSince := func(from int, want string) bool {
			mu.Lock()
			defer mu.Unlock()
			for _, n := range notices[from:] {
				if strings.HasPrefix(n, "WARNING") && strings.Contains(n, want) {
					return true
				}
			}
			return false
		}
		obs := func(ik string, qty, bn, bv, pv int, ts string) string {
			return `SELECT * FROM (VALUES (1::int,10::bigint,'` + ik + `'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,` +
				strconv.Itoa(qty) + `::numeric,'LOAN'::text,` + strconv.Itoa(bn) + `::bigint,` +
				strconv.Itoa(bv) + `::int,` + strconv.Itoa(pv) + `::int,'` + ts + `'::timestamptz)) ` + mppCols
		}

		// block_timestamp drift, processing_version leg. Two logical keys at the same block and
		// block_version, differing only in pv, with DIFFERENT timestamps -- legal, since block_timestamp
		// is invariant per logical key and these are two keys.
		run(obs("isibts", 5, 200, 0, 0, "2026-01-01"), "sibling pv=0")
		run(obs("isibts", 5, 200, 0, 1, "2026-02-02"), "sibling pv=1")
		mu.Lock()
		mark := len(notices)
		mu.Unlock()
		run(obs("isibts", 5, 200, 0, 1, "2026-02-02"), "re-emit pv=1 unchanged")
		if warnedSince(mark, "changed block_timestamp") {
			t.Errorf("re-emitting pv=1 unchanged warned about block_timestamp drift; the join is matching the pv=0 sibling. notices = %v", notices[mark:])
		}

		// quantity drift, block_version leg. Two keys at the same block and pv with the same timestamp,
		// differing in block_version and in quantity -- a reorg that changed the value.
		run(obs("isibqty", 5, 300, 0, 0, "2026-03-03"), "sibling bv=0")
		run(obs("isibqty", 9, 300, 1, 0, "2026-03-03"), "sibling bv=1")
		mu.Lock()
		mark = len(notices)
		mu.Unlock()
		run(obs("isibqty", 9, 300, 1, 0, "2026-03-03"), "re-emit bv=1 unchanged")
		if warnedSince(mark, "changed quantity") {
			t.Errorf("re-emitting bv=1 unchanged warned about quantity drift; the join is matching the bv=0 sibling. notices = %v", notices[mark:])
		}
	})

	// No subtest covers the `attnum > 0` and `NOT attisdropped` legs of check (1)'s pg_attribute join,
	// because neither leg is load-bearing: `attname = e.col` already excludes everything they exclude.
	// Postgres renames a dropped column to "........pg.dropped.N........", and it rejects a user column
	// named after a system column outright ("column name \"ctid\" conflicts with a system column name"),
	// so no relation can present a matching name from either class. Measured on 2.25.1-pg17 against a
	// table holding two dropped columns and all six system columns: removing either leg, or both, leaves
	// the verdict unchanged both when the contract is satisfied and when a column is genuinely missing.
	// They are equivalent mutations -- kept as cheap defence-in-depth, not a coverage hole.

	t.Run("a pg_temp decoy catalog cannot redirect the qualname lookup or the contract check", func(t *testing.T) {
		// The behavioural half of the qualification fix. pg_temp precedes pg_catalog for relation
		// lookup, so before pg_catalog.-qualifying them, a session pre-creating pg_temp.pg_class chose
		// v_qualname -- the advisory-lock key AND the ownership stamp -- and a pg_temp.pg_attribute
		// neutered the column contract. Empty decoys make the difference observable: unqualified, the
		// qualname lookup finds nothing and the function raises; qualified, it ignores them entirely.
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		for _, ddl := range []string{
			`CREATE TEMP TABLE pg_class (oid oid, relname name, relnamespace oid) ON COMMIT DROP`,
			`CREATE TEMP TABLE pg_namespace (oid oid, nspname name) ON COMMIT DROP`,
			`CREATE TEMP TABLE pg_attribute (attrelid oid, attname name, atttypid oid,
				atttypmod integer, attnum smallint, attisdropped boolean) ON COMMIT DROP`,
		} {
			if _, err := tx.Exec(ctx, ddl); err != nil {
				t.Fatalf("create decoy: %v", err)
			}
		}
		if _, err := tx.Exec(ctx, `CREATE OR REPLACE VIEW vdecoy AS `+
			valuesOf(row("idecoy", strings.Repeat("a", 40), 5, "LOAN", 800, 0, 0))); err != nil {
			t.Fatal(err)
		}
		var inserted int64
		if err := tx.QueryRow(ctx, `SELECT materialize_position_projection('vdecoy'::regclass)`).Scan(&inserted); err != nil {
			t.Fatalf("the decoy catalogs redirected the materializer: %v", err)
		}
		if inserted != 1 {
			t.Errorf("inserted %d rows under decoy catalogs; want 1", inserted)
		}
	})

	t.Run("a NULL in any NOT NULL column is named before the write, not raised as a bare 23502", func(t *testing.T) {
		// prime_debt.processing_version is nullable_exempt, so a live projection can emit a NULL version.
		// It passes the name+type contract, then matches nothing in the drift joins or the anti-join
		// (three-valued logic) and dies mid-INSERT on the column's NOT NULL -- a 23502 naming no view, no
		// row, no position, on every subsequent run. Each column gets its own case: a single NULL-pv
		// fixture would leave the other eight legs of the pre-flight untested.
		// chain_id and protocol_id are excluded: both are nullable on this table by convention, so a
		// NULL there is legal (asserted by "null chain_id is legal ..." below).
		cols := []string{"instrument_key", "holder_id", "quantity",
			"block_number", "block_version", "processing_version", "block_timestamp"}
		typed := map[string]string{"chain_id": "int", "protocol_id": "bigint", "instrument_key": "text",
			"holder_id": "text", "quantity": "numeric", "block_number": "bigint", "block_version": "int",
			"processing_version": "int", "block_timestamp": "timestamptz"}
		for _, col := range cols {
			t.Run(col, func(t *testing.T) {
				vals := map[string]string{
					"chain_id": "1::int", "protocol_id": "10::bigint",
					"instrument_key": "'inull'::text", "holder_id": "'" + strings.Repeat("a", 40) + "'::text",
					"quantity": "5::numeric", "deal_type": "'LOAN'::text",
					"block_number": "900::bigint", "block_version": "0::int",
					"processing_version": "0::int", "block_timestamp": "'2026-01-01'::timestamptz",
				}
				vals[col] = "NULL::" + typed[col]
				order := []string{"chain_id", "protocol_id", "instrument_key", "holder_id", "quantity",
					"deal_type", "block_number", "block_version", "processing_version", "block_timestamp"}
				parts := make([]string, 0, len(order))
				for _, k := range order {
					parts = append(parts, vals[k])
				}
				body := `SELECT * FROM (VALUES (` + strings.Join(parts, ",") + `)) ` + mppCols
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vnull AS `+body); err != nil {
					t.Fatal(err)
				}
				_, err := pool.Exec(ctx, `SELECT materialize_position_projection('vnull'::regclass)`)
				if err == nil {
					t.Fatalf("a NULL %s was accepted", col)
				}
				msg := err.Error()
				// identity columns die inside position_id() first, which is also a named, pre-write
				// failure; everything else must be named by the NULL-ness pre-flight.
				if strings.Contains(msg, "is required") {
					return
				}
				if !strings.Contains(msg, "emits NULL in a NOT NULL position_state column") {
					t.Errorf("NULL %s was not named before the write: %v", col, err)
				}
				if !strings.Contains(msg, col+"=NULL") {
					t.Errorf("the message does not name the offending column %q: %v", col, err)
				}
			})
		}
	})

	t.Run("an explicit NULL p_build_id is rejected up front, not on the first inserting run", func(t *testing.T) {
		// An explicit NULL bypasses the DEFAULT 0 and passes every check in the body: build_id >= 0
		// evaluates to NULL rather than false, and the column's NOT NULL only fires when a row is
		// actually inserted. Without the guard a caller passing NULL succeeds on any run that inserts
		// nothing and dies with a bare 23502 on the first that does.
		view := valuesOf(row("ibid", strings.Repeat("a", 40), 5, "LOAN", 700, 0, 0))
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vbid AS `+view); err != nil {
			t.Fatal(err)
		}
		// The no-op run first: it inserts nothing, which is exactly where the old behaviour passed.
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection('vbid'::regclass, NULL)`); err == nil {
			t.Error("a NULL p_build_id was accepted on a run that inserts nothing; the guard is missing")
		} else if !strings.Contains(err.Error(), "p_build_id must not be NULL") {
			t.Errorf("rejected, but not by the guard: %v", err)
		}
		// And omitting the argument still takes the default.
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection('vbid'::regclass)`); err != nil {
			t.Fatalf("omitting p_build_id must take the DEFAULT: %v", err)
		}
	})

	t.Run("a key whose timestamp AND quantity both drifted warns about both, naming the position", func(t *testing.T) {
		// As two separate queries the quantity arm carried `p.block_timestamp = s.block_timestamp`, so a
		// row that drifted on both axes reported only the timestamp -- the quantity disagreement was
		// silently unreported, on the one path whose whole job is to say "data disagreed and was
		// discarded". Both messages must also name the position: bn/bv/pv are identical for two
		// different positions drifting at the same block.
		holder := strings.Repeat("a", 40)
		cfg, err := pgx.ParseConfig(pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var notices []string
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
			mu.Lock()
			defer mu.Unlock()
			notices = append(notices, n.Message)
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)

		emit := func(qty int, ts, label string) {
			t.Helper()
			body := `SELECT * FROM (VALUES (1::int,10::bigint,'iboth'::text,'` + holder + `'::text,` +
				strconv.Itoa(qty) + `::numeric,'LOAN'::text,650::bigint,0::int,0::int,'` + ts + `'::timestamptz)) ` + mppCols
			if _, err := conn.Exec(ctx, `CREATE OR REPLACE VIEW vboth AS `+body); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `SELECT materialize_position_projection('vboth'::regclass)`); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
		}
		emit(5, "2026-01-01", "store the observation")
		mu.Lock()
		notices = nil
		mu.Unlock()
		emit(9, "2026-06-06", "re-emit with BOTH axes drifted")

		var wantPos string
		if err := pool.QueryRow(ctx,
			`SELECT encode(position_id(1,10,'iboth','`+holder+`'),'hex')`).Scan(&wantPos); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		for _, want := range []string{"changed block_timestamp", "changed quantity"} {
			var got string
			for _, n := range notices {
				if strings.Contains(n, want) {
					got = n
				}
			}
			if got == "" {
				t.Errorf("no %q warning: a both-axes drift must report both. notices = %v", want, notices)
				continue
			}
			if !strings.Contains(got, "pos="+wantPos) {
				t.Errorf("the %q warning does not name the position (want pos=%s): %s", want, wantPos, got)
			}
		}
		// And nothing was rewritten: the stored row keeps its original values.
		var qty int
		var ts string
		if err := pool.QueryRow(ctx, `SELECT quantity, block_timestamp::date::text FROM position_state
			 WHERE position_id = position_id(1,10,'iboth','`+holder+`')`).Scan(&qty, &ts); err != nil {
			t.Fatal(err)
		}
		if qty != 5 || ts != "2026-01-01" {
			t.Errorf("stored row was modified: quantity=%d ts=%s; want 5 / 2026-01-01", qty, ts)
		}
	})

	t.Run("a lossy typmod on a value column is rejected, a lossless one passes", func(t *testing.T) {
		// Check (1) strips typmod on purpose so a wider compatible column passes -- which also let a
		// NARROWING one through: numeric(10,0) rounds every fractional quantity and timestamptz(0)
		// truncates the instant to the second, silently, and nothing downstream can detect it because the
		// value stores clean and no UPDATE grant exists to repair it.
		holder := strings.Repeat("a", 40)
		// A distinct instrument_key per case: one projection owns a position, so reusing a key would
		// trip cross-view disjointness instead of exercising the type contract.
		body := func(ik, qtyType, tsType string) string {
			return `SELECT 1::int chain_id,10::bigint protocol_id,'` + ik + `'::text instrument_key,'` + holder +
				`'::text holder_id,0.5::` + qtyType + ` quantity,'LOAN'::text deal_type,` +
				`600::bigint block_number,0::int block_version,0::int processing_version,` +
				`'2026-01-01 00:00:00.123456+00'::` + tsType + ` block_timestamp`
		}
		for i, tc := range []struct {
			name, qty, ts string
			wantErr       string
		}{
			{"a rounding quantity", "numeric(10,0)", "timestamptz", "quantity is numeric(10,0)"},
			{"a truncating block_timestamp", "numeric", "timestamptz(0)", "block_timestamp is timestamp(0) with time zone"},
			{"both", "numeric(10,0)", "timestamptz(0)", "quantity is numeric(10,0)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				tag := "tm" + strconv.Itoa(i)
				mppErr(t, "v"+tag, body("i"+tag, tc.qty, tc.ts), tc.name, tc.wantErr)
			})
		}
		// The lossless forms must still pass, or the check is just a narrower contract.
		for i, tc := range []struct{ name, qty, ts string }{
			{"unconstrained", "numeric", "timestamptz"},
			{"numeric(30,18) and explicit timestamptz(6)", "numeric(30,18)", "timestamptz(6)"},
		} {
			t.Run("lossless: "+tc.name, func(t *testing.T) {
				tag := "tmok" + strconv.Itoa(i)
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW v`+tag+` AS `+
					body("i"+tag, tc.qty, tc.ts)); err != nil {
					t.Fatal(err)
				}
				if _, err := pool.Exec(ctx,
					`SELECT materialize_position_projection($1::regclass)`, "v"+tag); err != nil {
					t.Errorf("%s must be accepted as lossless: %v", tc.name, err)
				}
			})
		}
	})

	t.Run("a foreign row that does not sort first is still caught as an ownership violation", func(t *testing.T) {
		// The old probe read each position's min-PK row only, so a row stamped by ANOTHER projection was
		// invisible unless it happened to sort first -- ownership was decided by PK sort order rather
		// than by which writer got there first, and the interleaving this check exists to surface went
		// unreported. It also forced a read of the position's OLDEST observation, its coldest chunk.
		// This builds exactly the state the first-write race produces: our row sorts first, the
		// foreign one does not.
		holder := strings.Repeat("a", 40)
		if n := mpp(t, "vownsort", valuesOf(row("iownsort", holder, 5, "LOAN", 500, 0, 0)), "establish ownership"); n != 1 {
			t.Fatalf("setup inserted %d rows; want 1", n)
		}
		// A later-sorting row stamped by a different projection, written directly: the materializer
		// cannot produce this state itself, which is precisely why the check has to detect it.
		if _, err := pool.Exec(ctx, `
			INSERT INTO position_state
			  (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
			   block_number, block_version, processing_version, block_timestamp, projection, build_id)
			VALUES (position_id(1,10,'iownsort',$1), 1, 10, 'iownsort', $1, 7,
			        600, 0, 0, '2026-01-02'::timestamptz, 'public.vintruder', 0)`, holder); err != nil {
			t.Fatal(err)
		}
		// Sanity: our row really does sort first, so a min-PK probe would have seen only ours.
		var firstProjection string
		if err := pool.QueryRow(ctx, `
			SELECT projection FROM position_state WHERE position_id = position_id(1,10,'iownsort',$1)
			 ORDER BY position_id, block_number, block_version, processing_version, block_timestamp
			 LIMIT 1`, holder).Scan(&firstProjection); err != nil {
			t.Fatal(err)
		}
		if firstProjection != "public.vownsort" {
			t.Fatalf("the min-PK row is %q, so this fixture does not reproduce the blind spot", firstProjection)
		}
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection('vownsort'::regclass)`)
		if err == nil {
			t.Fatal("a foreign row on the position was not detected; the probe only looks at one row")
		}
		if !strings.Contains(err.Error(), "public.vintruder") {
			t.Errorf("raised, but not naming the other projection: %v", err)
		}
	})

	t.Run("an in-flight run blocks a concurrent rename of its projection view", func(t *testing.T) {
		// Scope, stated precisely because the obvious reading overclaims: this asserts the view is held
		// for the run's transaction, which it already was -- building the snapshot reads the view and
		// that read takes AccessShareLock to commit. Removing the explicit LOCK TABLE added upstream does
		// NOT fail this subtest (measured). That lock closes a narrower window -- between the pg_class
		// name lookup and the first read of the view, where a committed rename would leave this session
		// keyed on the old name while a later session keys on the new one -- and hitting that window
		// deterministically from outside the function is not possible, so it stays belt-and-braces, like
		// the search_path pin below. What IS asserted here: the name cannot move under a run in flight.
		holder := strings.Repeat("a", 40)
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vlockname AS `+
			valuesOf(row("ilockname", holder, 5, "LOAN", 950, 0, 0))); err != nil {
			t.Fatal(err)
		}
		runner, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer runner.Release()
		tx, err := runner.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('vlockname'::regclass)`); err != nil {
			t.Fatal(err)
		}

		// A second session tries to rename the view while that transaction is still open.
		renamer, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer renamer.Release()
		if _, err := renamer.Exec(ctx, `SET lock_timeout = '2s'`); err != nil {
			t.Fatal(err)
		}
		_, err = renamer.Exec(ctx, `ALTER VIEW vlockname RENAME TO vlockname_moved`)
		if err == nil {
			t.Error("the rename succeeded while a run was in flight; the view is not being held, so two " +
				"sessions can derive different canonical names for it")
			// put it back so later subtests are unaffected
			if _, e := renamer.Exec(ctx, `ALTER VIEW vlockname_moved RENAME TO vlockname`); e != nil {
				t.Fatal(e)
			}
			return
		}
		if !strings.Contains(err.Error(), "lock timeout") && !strings.Contains(err.Error(), "57014") {
			t.Errorf("the rename failed for the wrong reason (want a lock timeout): %v", err)
		}
	})

	t.Run("each diagnostic message is capped at five coordinate sets", func(t *testing.T) {
		// The three LIMIT 5 clauses bound how many coordinate sets a message names. Removing any of them
		// makes an error or warning grow with the size of the offending projection -- a 200,000-row bad
		// view would emit a 200,000-item string into the log. Nothing exercised the caps, so all three
		// removals survived. Six offenders per site, asserting the message names exactly five.
		count := func(msg string) int { return strings.Count(msg, "bn=") }

		// Check (2), the double-emit hard failure: six logical keys emitted twice each.
		var dupRows []string
		for i := range 6 {
			r := row("icap", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 7, "LOAN", 900+i, 0, 0)
			dupRows = append(dupRows, r, r)
		}
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vcap AS `+valuesOf(dupRows...)); err != nil {
			t.Fatal(err)
		}
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection('vcap'::regclass)`)
		if err == nil {
			t.Fatal("six double-emitted keys did not raise")
		}
		if n := count(err.Error()); n != 5 {
			t.Errorf("the double-emit message names %d coordinate sets; want exactly 5 (the LIMIT 5 cap): %v", n, err)
		}

		// The two drift WARNINGs: store six observations, then re-emit all six with a drifted timestamp
		// and again with a drifted quantity.
		cfg, err := pgx.ParseConfig(pool.Config().ConnString())
		if err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		var notices []string
		cfg.OnNotice = func(_ *pgconn.PgConn, n *pgconn.Notice) {
			mu.Lock()
			defer mu.Unlock()
			notices = append(notices, n.Message)
		}
		conn, err := pgx.ConnectConfig(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(ctx)
		emit := func(qty int, ts string, label string) {
			t.Helper()
			var rows []string
			for i := range 6 {
				rows = append(rows, `(1::int,10::bigint,'icapd'::text,'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'::text,`+strconv.Itoa(qty)+
					`::numeric,'LOAN'::text,`+strconv.Itoa(800+i)+`::bigint,0::int,0::int,'`+ts+`'::timestamptz)`)
			}
			if _, err := conn.Exec(ctx, `CREATE OR REPLACE VIEW vcapd AS SELECT * FROM (VALUES `+
				strings.Join(rows, ",")+`) `+mppCols); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `SELECT materialize_position_projection('vcapd'::regclass)`); err != nil {
				t.Fatalf("%s: %v", label, err)
			}
		}
		emit(5, "2026-01-01", "store six")
		emit(5, "2026-06-06", "ts drift on all six")
		emit(9, "2026-01-01", "qty drift on all six")

		mu.Lock()
		defer mu.Unlock()
		for _, want := range []string{"changed block_timestamp", "changed quantity"} {
			found := false
			for _, n := range notices {
				if !strings.Contains(n, want) {
					continue
				}
				found = true
				if got := count(n); got != 5 {
					t.Errorf("the %q warning names %d coordinate sets; want exactly 5 (the LIMIT 5 cap)", want, got)
				}
			}
			if !found {
				t.Errorf("no %q warning was raised, so its cap is untested; notices = %v", want, notices)
			}
		}
	})

	t.Run("every relation and the identity call are schema-qualified (the class, not an instance)", func(t *testing.T) {
		// Round 8 closed pg_temp shadowing one read at a time and two unqualified references survived,
		// so the class is asserted from the catalogue rather than case by case. Round 9 widened it twice
		// more: the CATALOG reads (pg_class/pg_namespace in the qualname lookup, pg_attribute in check
		// (1)) sat outside the relation list, and pg_temp precedes pg_catalog for relation lookup -- a
		// session pre-creating pg_temp.pg_class chooses the advisory-lock key and the ownership stamp,
		// and a pg_temp.pg_attribute neuters the contract check. And position_id() was the one
		// unqualified USER-function call: the pinned path is the symbolic `"$user", public`, re-resolved
		// per CALLER for a SECURITY INVOKER function, so any role-named schema shadows the identity hash.
		lineComment := regexp.MustCompile(`(?m)--.*$`)
		// Covers every statement that names a permanent, temp or catalog relation, not just the four
		// keywords the earliest version matched -- ANALYZE and DROP TABLE sat outside the "class" this
		// subtest claims to assert, so unqualifying either survived.
		unqualifiedRel := regexp.MustCompile(`(?i)\b(?:FROM|JOIN|INTO|UPDATE|ANALYZE|DROP\s+TABLE(?:\s+IF\s+EXISTS)?)\s+` +
			`(position_state|_mpp_src|pg_class|pg_namespace|pg_attribute|pg_proc|pg_trigger)\b`)
		// A call, not a mention: the paren is required, and a leading dot excludes public.position_id(.
		unqualifiedFn := regexp.MustCompile(`(?i)(?:^|[^.\w])(position_id|position_key)\s*\(`)
		const fn = "materialize_position_projection"
		var src string
		if err := pool.QueryRow(ctx, `SELECT prosrc FROM pg_proc WHERE proname = $1`, fn).Scan(&src); err != nil {
			t.Fatalf("%s: %v", fn, err)
		}
		body := lineComment.ReplaceAllString(src, "")
		if bad := unqualifiedRel.FindAllString(body, -1); len(bad) > 0 {
			t.Errorf("%s has unqualified relation references: %q", fn, bad)
		}
		if bad := unqualifiedFn.FindAllString(body, -1); len(bad) > 0 {
			t.Errorf("%s has unqualified identity-function calls (a role-named schema shadows them): %q", fn, bad)
		}
	})

	t.Run("the materializer pins search_path", func(t *testing.T) {
		// The pin overrides a hostile caller search_path before the body runs, which is what makes the
		// qualification above belt-and-braces and its own removal invisible to any behavioural case.
		for _, fn := range []string{"materialize_position_projection"} {
			var settings []string
			if err := pool.QueryRow(ctx,
				`SELECT coalesce(proconfig, '{}') FROM pg_proc WHERE proname = $1`, fn).Scan(&settings); err != nil {
				t.Fatalf("%s: %v", fn, err)
			}
			pinned := false
			for _, s := range settings {
				if strings.HasPrefix(s, "search_path=") {
					pinned = true
				}
			}
			if !pinned {
				t.Errorf("%s: proconfig = %v; want a pinned search_path", fn, settings)
			}
		}
	})
}

// psTestDealTypeCode covers: deal_type carry, type gate, vocabulary gate (VEC-401).
// Every case fails on the pre-fix code it names -- the pre-fix materializer read the view's column
// only when it was exactly `text`, stored whatever string it found, and validated nothing.
func psTestDealTypeCode(t *testing.T, f *psFixture) {
	// dtRow is row() with the deal_type slot under the caller's control: an arbitrary SQL
	// expression, or "" to omit the column entirely (the MISSING-contract case).
	dtRow := func(ik, expr string) string {
		body := "(1::int,10::bigint,'" + ik + "'::text,'" + strings.Repeat("a", 40) + "'::text,5::numeric,"
		cols := mppCols
		if expr == "" {
			cols = `v(chain_id,protocol_id,instrument_key,holder_id,quantity,block_number,` +
				`block_version,processing_version,block_timestamp)`
		} else {
			body += expr + ","
		}
		body += "100::bigint,0::int,0::int,'2026-01-01'::timestamptz)"
		return `SELECT * FROM (VALUES ` + body + `) ` + cols
	}
	stored := func(t *testing.T, ik string) *string {
		t.Helper()
		var got *string
		if err := f.pool.QueryRow(f.ctx,
			`SELECT deal_type FROM position_state WHERE instrument_key = $1`, ik).Scan(&got); err != nil {
			t.Fatalf("read back %s: %v", ik, err)
		}
		return got
	}

	// deal_type is a required contract column typed exactly text. A narrower string type would truncate
	// on cast, and the truncation can land on ANOTHER valid code ('CUSTODY_COLLATERAL'::varchar(7) is
	// 'CUSTODY'), so the exact-type match refuses every non-text declaration up front.
	t.Run("declared text is carried; any other string type is a contract violation", func(t *testing.T) {
		ik := "dt-carry-text"
		if n := f.mppN(t, "pv_dt_carry_text", dtRow(ik, "'LOAN'::text"), "carry"); n != 1 {
			t.Fatalf("text: inserted %d rows, want 1", n)
		}
		if got := stored(t, ik); got == nil || *got != "LOAN" {
			t.Errorf("text: deal_type = %v, want LOAN", got)
		}
		for _, c := range []struct{ name, expr, want string }{
			{"varchar_63", "'BORROW'::varchar(63)", "deal_type (is character varying(63))"},
			{"varchar_unbounded", "'COLLATERAL'::varchar", "deal_type (is character varying)"},
			{"varchar_7_would_truncate_to_another_code", "'CUSTODY_COLLATERAL'::varchar(7)", "deal_type (is character varying(7))"},
			{"bpchar_63", "'LOAN'::char(63)", "deal_type (is character(63))"},
		} {
			f.mppErr(t, "pv_dt_type_"+c.name, dtRow("dt-type-"+c.name, c.expr), "contract", c.want)
		}
	})

	// A non-string deal_type is refused by the same exact-type contract match.
	t.Run("non-string type is a contract violation", func(t *testing.T) {
		f.mppErr(t, "pv_dt_int", dtRow("dt-int", "7::int"), "wrong type", "deal_type (is integer)")
	})

	// The materializer CANNOT apply a changed deal_type to a stored observation: the insert is
	// suppressed on the 4-column key and UPDATE is revoked. It records the drift and continues; raising
	// instead wedged the whole projection on every later run, since nothing could clear the stored row.
	t.Run("re-emitting a stored key with a different deal type is recorded, kept-stored, and never a wedge", func(t *testing.T) {
		const ik = "dt-reemit"
		body := dtRow(ik, "'LOAN'::text")
		if n := f.mppN(t, "pv_dt_reemit", body, "first insert"); n != 1 {
			t.Fatalf("first insert put %d rows, want 1", n)
		}
		for run := 1; run <= 2; run++ {
			if n := f.mppN(t, "pv_dt_reemit", dtRow(ik, "'BORROW'::text"), "drift run"); n != 0 {
				t.Errorf("drift run %d inserted %d rows, want 0 (stored row kept)", run, n)
			}
		}
		var stored string
		var recorded int
		if err := f.pool.QueryRow(f.ctx, `SELECT
			(SELECT deal_type FROM position_state WHERE instrument_key = $1),
			(SELECT count(*) FROM position_projection_refusal WHERE reason = 'deal_type_drift' AND detail LIKE 'ik=' || $1 || ' %')`,
			ik).Scan(&stored, &recorded); err != nil {
			t.Fatal(err)
		}
		if stored != "LOAN" {
			t.Errorf("stored deal_type=%s after a BORROW re-emit; want the original LOAN kept", stored)
		}
		if recorded != 1 {
			t.Errorf("%d deal_type_drift refusal rows across two runs; want exactly 1", recorded)
		}

		// NULL -> a value is the migration case: the column was added after the rows were stored. Just
		// as unrepairable, so it is recorded the same way rather than silently returning 0.
		const ik2 = "dt-reemit-null"
		if n := f.mppN(t, "pv_dt_reemit_null", dtRow(ik2, "NULL::text"), "first insert, silent"); n != 1 {
			t.Fatalf("first insert put %d rows, want 1", n)
		}
		if n := f.mppN(t, "pv_dt_reemit_null", dtRow(ik2, "'LOAN'::text"), "NULL->value drift"); n != 0 {
			t.Errorf("NULL->value drift inserted %d rows, want 0", n)
		}
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_projection_refusal
			WHERE reason = 'deal_type_drift' AND detail LIKE 'ik=' || $1 || ' %' AND detail LIKE '%stored ts=% dt=NULL;%'`, ik2).Scan(&recorded); err != nil {
			t.Fatal(err)
		}
		if recorded != 1 {
			t.Errorf("NULL->value drift recorded %d rows naming the stored NULL; want 1", recorded)
		}

		// Negative control: re-emitting the SAME deal type must stay a clean no-op, or the arm above
		// would make every idempotent re-run fail.
		if n := f.mppN(t, "pv_dt_reemit", dtRow(ik, "'LOAN'::text"), "idempotent re-run"); n != 0 {
			t.Errorf("idempotent re-run inserted %d rows, want 0", n)
		}
	})

	// Off-vocabulary codes must fail hard: UPDATE is revoked on position_state, so a bad code written
	// once is unfixable without a superuser. Pre-fix all three of these were stored verbatim.
	t.Run("off-vocabulary fails hard", func(t *testing.T) {
		for _, c := range []struct{ name, expr string }{
			{"wrong_case", "'loan'::text"},
			{"unknown_code", "'NOT_A_DEAL_TYPE'::text"},
			{"empty_string", "''::text"},
		} {
			body := dtRow("dt-bad-"+c.name, c.expr)
			f.mppErr(t, "pv_dt_bad_"+c.name, body, "vocabulary gate", "position_state_deal_type_fkey")
		}
	})

	// A projection with nothing to say emits NULL::text; it may not omit the column, or a silently
	// stored NULL would be indistinguishable from a projection that forgot the direction.
	t.Run("an explicit NULL inserts, an omitted column is refused as MISSING", func(t *testing.T) {
		ik := "dt-silent-null"
		if n := f.mppN(t, "pv_dt_silent_null", dtRow(ik, "NULL::text"), "explicit null"); n != 1 {
			t.Fatalf("explicit NULL: inserted %d rows, want 1", n)
		}
		if got := stored(t, ik); got != nil {
			t.Errorf("explicit NULL: deal_type = %q, want NULL", *got)
		}
		f.mppErr(t, "pv_dt_silent_omitted", dtRow("dt-silent-omitted", ""), "omitted", "deal_type (MISSING)")
	})

	// Every seeded code must round-trip, or the gate is too strict for the vocabulary it enforces.
	t.Run("every seeded code round-trips", func(t *testing.T) {
		codes := f.refDealTypeCodes(t)
		for i, code := range codes {
			ik := "dt-rt-" + strconv.Itoa(i)
			body := dtRow(ik, "'"+code+"'::text")
			if n := f.mppN(t, "pv_dt_rt_"+strconv.Itoa(i), body, "round-trip"); n != 1 {
				t.Fatalf("%s: inserted %d rows, want 1", code, n)
			}
			if got := stored(t, ik); got == nil || *got != code {
				t.Errorf("%s: deal_type = %v, want %q", code, got, code)
			}
		}
	})

	// The gate must read ref_deal_type rather than a copied IN-list, or the vocabulary drifts the moment
	// a code is added. Asserted on the mechanism, and ordered reject-then-seed because the reference
	// tables are append-only (DELETE is trigger-blocked), so the code cannot be withdrawn afterwards.
	t.Run("vocabulary comes from ref_deal_type", func(t *testing.T) {
		const code = "ZZ_TEST_ONLY_DEAL_TYPE"

		body := dtRow("dt-refdriven-before", "'"+code+"'::text")
		f.mppErr(t, "pv_dt_refdriven_before", body, "ref-driven", "position_state_deal_type_fkey")

		if _, err := f.pool.Exec(f.ctx,
			`INSERT INTO ref_deal_type (deal_type, direction, description)
			 VALUES ($1, 'LONG', 'test-only; added by psTestDealTypeCode')`, code); err != nil {
			t.Fatalf("seed %s: %v", code, err)
		}

		ik := "dt-refdriven-after"
		body2 := dtRow(ik, "'"+code+"'::text")
		if n := f.mppN(t, "pv_dt_refdriven_after", body2, "ref-driven"); n != 1 {
			t.Fatalf("seeded code: inserted %d rows, want 1", n)
		}
		if got := stored(t, ik); got == nil || *got != code {
			t.Errorf("seeded code: deal_type = %v, want %q", got, code)
		}
	})
}

// psTestDealTypeCodeDirectInsert covers the writer the materializer's gates do not bind: stl_readwrite
// holds INSERT on position_state directly. Measured as stl_readwrite -- as the owning test role every
// case is accepted regardless, so a superuser connection would prove nothing.
//
// Fails on the pre-CHECK code, which accepted 'loan', ”, '   ', a newline and a 1MB string through
// this path, none of which UPDATE or DELETE can then repair.
func psTestDealTypeCodeDirectInsert(t *testing.T, f *psFixture) {
	rw, doneRW := f.asReadWrite(t)
	defer doneRW()

	ins := func(t *testing.T, code any, bn int) error {
		t.Helper()
		_, err := rw.Exec(f.ctx,
			`INSERT INTO position_state
			   (position_id, chain_id, protocol_id, instrument_key, holder_id, quantity,
			    block_number, block_version, processing_version, block_timestamp, projection,
			    build_id, deal_type)
			 VALUES (decode(repeat('cd',32),'hex'), 1, 10, 'dt-direct', repeat('a',40), 5,
			         $1, 0, 0, '2026-01-01'::timestamptz, 'dt.direct.view', 0, $2)`, bn, code)
		return err
	}

	t.Run("direct INSERT rejects malformed codes", func(t *testing.T) {
		for i, c := range []struct{ name, code string }{
			{"wrong_case", "loan"},
			{"empty", ""},
			{"whitespace", "   "},
			{"leading_space", " LOAN"},
			{"trailing_newline", "LOAN\n"},
			{"embedded_newline", "LOAN\n-- rubbish"},
			{"lowercase_tail", "LOAn"},
			{"leading_digit", "1LOAN"},
			{"one_megabyte", strings.Repeat("X", 1<<20)},
		} {
			if err := ins(t, c.code, 500+i); err == nil {
				t.Errorf("%s: direct INSERT accepted %.40q, want rejection", c.name, c.code)
			} else if !strings.Contains(err.Error(), "position_state_deal_type_fkey") {
				t.Errorf("%s: rejected by %v, want the ref_deal_type FK", c.name, firstLineOf(err.Error()))
			}
		}
	})

	// Negative controls: the CHECK must not have made the column unusable or effectively NOT NULL.
	t.Run("direct INSERT accepts a real code and NULL", func(t *testing.T) {
		if err := ins(t, "LOAN", 600); err != nil {
			t.Errorf("direct INSERT of 'LOAN': %v", err)
		}
		if err := ins(t, nil, 601); err != nil {
			t.Errorf("direct INSERT of NULL: %v", err)
		}
		if err := ins(t, "COLLATERAL", 602); err != nil {
			t.Errorf("direct INSERT of 'COLLATERAL': %v", err)
		}
	})

	// stl_readwrite's table-level GRANT INSERT must reach the new column, and every code in the
	// vocabulary must be storable through the role that writes it.
	t.Run("the app role can store every ref_deal_type code", func(t *testing.T) {
		var ok bool
		if err := f.pool.QueryRow(f.ctx,
			`SELECT has_column_privilege('stl_readwrite','position_state','deal_type','INSERT')`).Scan(&ok); err != nil {
			t.Fatalf("check column privilege: %v", err)
		}
		if !ok {
			t.Error("stl_readwrite has no INSERT privilege on position_state.deal_type")
		}
		codes := f.refDealTypeCodes(t)
		for i, code := range codes {
			if err := ins(t, code, 700+i); err != nil {
				t.Errorf("ref_deal_type code %q is not storable: %v", code, firstLineOf(err.Error()))
			}
		}
	})
}

// asReadWrite returns a pool connected as stl_readwrite. The fixture's own pool owns the schema, and
// an owner bypasses the ACLs these subtests are about.
func (f *psFixture) asReadWrite(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	// Roles are cluster-scoped, not per-database, and this fixture's container is shared across the
	// whole package -- so the LOGIN has to come back off, or every later test in this container runs
	// against a stl_readwrite that can connect.
	if _, err := f.pool.Exec(f.ctx, `ALTER ROLE stl_readwrite WITH LOGIN PASSWORD 'psfixture'`); err != nil {
		t.Fatalf("give stl_readwrite a login: %v", err)
	}

	cfg := f.pool.Config().Copy()
	cfg.ConnConfig.User = "stl_readwrite"
	cfg.ConnConfig.Password = "psfixture"
	pool, err := pgxpool.NewWithConfig(f.ctx, cfg)
	if err != nil {
		t.Fatalf("connect as stl_readwrite: %v", err)
	}
	// Returned rather than registered with t.Cleanup: the fixture closes its own pool with a plain
	// defer, and defer runs BEFORE t.Cleanup, so a t.Cleanup revoke reaches a closed pool.
	done := func() {
		pool.Close()
		if _, err := f.pool.Exec(f.ctx, `ALTER ROLE stl_readwrite WITH NOLOGIN PASSWORD NULL`); err != nil {
			t.Errorf("revoke the stl_readwrite login: %v", err)
		}
	}
	var who string
	if err := pool.QueryRow(f.ctx, `SELECT current_user`).Scan(&who); err != nil {
		t.Fatalf("confirm role: %v", err)
	}
	if who != "stl_readwrite" {
		t.Fatalf("connected as %q, want stl_readwrite", who)
	}
	return pool, done
}

func firstLineOf(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}

// refDealTypeCodes lists the seeded vocabulary. One helper because two subtests need it and the loop
// is easy to write without checking rows.Err(), which would silently truncate the list and let an
// "every code" assertion pass over part of it.
func (f *psFixture) refDealTypeCodes(t *testing.T) []string {
	t.Helper()
	rows, err := f.pool.Query(f.ctx, `SELECT deal_type FROM ref_deal_type ORDER BY deal_type`)
	if err != nil {
		t.Fatalf("list ref_deal_type: %v", err)
	}
	defer rows.Close()
	var codes []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatalf("scan ref_deal_type: %v", err)
		}
		codes = append(codes, c)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate ref_deal_type: %v", err)
	}
	if len(codes) < 5 {
		t.Fatalf("ref_deal_type has %d codes, expected the seeded vocabulary", len(codes))
	}
	return codes
}

// A restore, a manual apply, or a partial-apply recovery re-runs the file. Every statement in it is
// guarded except ADD CONSTRAINT, which Postgres offers no IF NOT EXISTS for -- so it failed with
// SQLSTATE 42710 until the DO block went in. #752 carries the same test for the same reason.
func psTestDealTypeCodeMigrationIsReRunnable(t *testing.T, f *psFixture) {
	apply := func(t *testing.T, name string) {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(getMigrationsPath(), name))
		if err != nil {
			t.Fatalf("read migration: %v", err)
		}
		if _, err := f.pool.Exec(f.ctx, string(raw)); err != nil {
			t.Errorf("re-applying %s failed: %v", name, err)
		}
	}

	t.Run("the migration re-applies cleanly", func(t *testing.T) {
		apply(t, "20260818_140000_add_position_state_deal_type.sql")
	})

	// Re-running one old file leaves the schema at that file's state, so it restores an earlier spine
	// overload. Both then exist and every call that omits the later arguments is ambiguous, which is why a
	// partial re-apply has to be followed by every later file -- the operator's own order.
	t.Run("re-applying the later files restores the current spine, and the old overload is gone", func(t *testing.T) {
		apply(t, "20260818_150000_add_run_id_to_position_stack.sql")
		apply(t, "20260909_160000_fix_offchain_check_before_closure.sql")
		apply(t, "20260911_120000_projection_window.sql")
		var n int
		if err := f.pool.QueryRow(f.ctx, `
			SELECT count(*) FROM pg_proc WHERE proname = 'materialize_position_projection'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("materialize_position_projection has %d definitions after the re-apply, want 1", n)
		}
	})
}

// psTestBlockTimeMonotonicPerPosition: the caches order by block_number and date by block_timestamp, so a
// higher block with an earlier instant makes position_current and position_daily disagree about the
// newest observation. On-chain that pairing is impossible; as input it is an error, and the design
// harness produced the disagreement on 24/24 seeds before this check existed.
func psTestBlockTimeMonotonicPerPosition(t *testing.T, f *psFixture) {
	one := func(ik string, bn int, ts string) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + strings.Repeat("a", 40) + "'::text,5::numeric,'LOAN'::text," +
			strconv.Itoa(bn) + "::bigint,0::int,0::int,'" + ts + "'::timestamptz)"
	}
	refusals := func(t *testing.T, ik string) int {
		t.Helper()
		var n int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_projection_refusal
			WHERE reason = 'block_time_inverts_height' AND detail LIKE 'ik=' || $1 || ' %'`, ik).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	stored := func(t *testing.T, ik string) int {
		t.Helper()
		var n int
		if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM position_state WHERE instrument_key = $1`, ik).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	// Refused per POSITION, recorded, and the run continues: aborting instead refused the whole
	// projection on every later run, since nothing can clear a stored row.
	t.Run("within one batch: the position is withheld and recorded while its peer lands", func(t *testing.T) {
		body := `SELECT * FROM (VALUES ` + one("mono-batch", 100, "2026-03-02T00:00:00Z") + "," +
			one("mono-batch", 200, "2026-03-01T00:00:00Z") + "," +
			one("mono-peer", 150, "2026-03-01T12:00:00Z") + `) ` + mppCols
		if n := f.mppN(t, "pv_mono_batch", body, "inverted pair plus a peer"); n != 1 {
			t.Errorf("inserted %d, want 1: the peer lands and the inverted position is withheld", n)
		}
		if got := stored(t, "mono-batch"); got != 0 {
			t.Errorf("the inverted position stored %d rows, want 0", got)
		}
		if got := refusals(t, "mono-batch"); got != 2 {
			t.Errorf("recorded %d refusals for the inverted position, want both withheld observations", got)
		}
	})
	t.Run("against stored history, in either direction: withheld, recorded once, never a wedge", func(t *testing.T) {
		if n := f.mppN(t, "pv_mono_hist", `SELECT * FROM (VALUES `+one("mono-hist", 200, "2026-03-02T00:00:00Z")+`) `+mppCols, "seed"); n != 1 {
			t.Fatalf("seed inserted %d, want 1", n)
		}
		lower := `SELECT * FROM (VALUES ` + one("mono-hist", 100, "2026-03-03T00:00:00Z") + `) ` + mppCols
		higher := `SELECT * FROM (VALUES ` + one("mono-hist", 300, "2026-03-01T00:00:00Z") + `) ` + mppCols
		for i, body := range []string{lower, higher, higher} {
			if n := f.mppN(t, "pv_mono_hist", body, "inverted against history"); n != 0 {
				t.Errorf("run %d inserted %d, want 0 (withheld)", i+1, n)
			}
		}
		if got := stored(t, "mono-hist"); got != 1 {
			t.Errorf("stored %d rows, want the seed alone", got)
		}
		if got := refusals(t, "mono-hist"); got != 2 {
			t.Errorf("recorded %d refusals, want 2: one per withheld observation, not one per run", got)
		}
	})
	// Negative controls: equal instants across blocks and same-block corrections must still insert, and
	// two positions are independent -- or the check would reject legitimate input.
	t.Run("monotonic, same-instant and cross-position cases still insert", func(t *testing.T) {
		body := `SELECT * FROM (VALUES ` + one("mono-ok", 100, "2026-03-01T00:00:00Z") + "," +
			one("mono-ok", 200, "2026-03-01T00:00:00Z") + "," +
			one("mono-ok", 300, "2026-03-02T00:00:00Z") + `) ` + mppCols
		if n := f.mppN(t, "pv_mono_ok", body, "monotonic ok"); n != 3 {
			t.Errorf("monotonic history inserted %d, want 3", n)
		}
		body2 := `SELECT * FROM (VALUES ` + one("mono-a", 100, "2026-03-05T00:00:00Z") + "," +
			one("mono-b", 200, "2026-03-04T00:00:00Z") + `) ` + mppCols
		if n := f.mppN(t, "pv_mono_two", body2, "two positions"); n != 2 {
			t.Errorf("two independent positions inserted %d, want 2", n)
		}
	})
}

// psTestCompletedRunIsRecorded: a position that stops being observed keeps its last quantity forever,
// and only block_timestamp age hinted at it. A run record per projection makes it a fact: swept up to
// block N and not seen. Written inside the materializer's transaction, so a failed run leaves nothing.
func psTestCompletedRunIsRecorded(t *testing.T, f *psFixture) {
	runs := func(t *testing.T, view string) (n int, latest *time.Time) {
		t.Helper()
		if err := f.pool.QueryRow(f.ctx,
			`SELECT count(*), max(block_timestamp) FROM position_projection_run WHERE projection = 'public.'||$1`,
			view).Scan(&n, &latest); err != nil {
			t.Fatalf("read runs for %s: %v", view, err)
		}
		return
	}
	row := func(ik string, bn int) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + strings.Repeat("a", 40) + "'::text,5::numeric,'LOAN'::text," +
			strconv.Itoa(bn) + "::bigint,0::int,0::int,'2026-03-0" + strconv.Itoa(1+bn%9) + "T00:00:00Z'::timestamptz)"
	}
	day := func(d int) time.Time { return time.Date(2026, 3, d, 0, 0, 0, 0, time.UTC) }

	t.Run("one row per completed run, carrying the run's latest block_timestamp", func(t *testing.T) {
		if n := f.mppN(t, "pv_run_a", `SELECT * FROM (VALUES `+row("run-a", 100)+","+row("run-a", 250)+`) `+mppCols, "first"); n != 2 {
			t.Fatalf("first run inserted %d, want 2", n)
		}
		n, latest := runs(t, "pv_run_a")
		if n != 1 || latest == nil || !latest.Equal(day(8)) {
			t.Errorf("after one run: %d rows, latest %v; want 1 row at 2026-03-08 (block 250's instant)", n, latest)
		}
		// an idempotent re-run inserts nothing but is still a completed sweep
		if k := f.mppN(t, "pv_run_a", `SELECT * FROM (VALUES `+row("run-a", 100)+","+row("run-a", 250)+`) `+mppCols, "rerun"); k != 0 {
			t.Fatalf("re-run inserted %d, want 0", k)
		}
		if n, _ := runs(t, "pv_run_a"); n != 2 {
			t.Errorf("after a no-op re-run: %d run rows, want 2 -- a sweep that saw nothing new still completed", n)
		}
	})

	t.Run("two runs of one view inside one transaction are two rows", func(t *testing.T) {
		if _, err := f.pool.Exec(f.ctx, `CREATE OR REPLACE VIEW pv_run_tx AS SELECT * FROM (VALUES `+row("run-tx", 100)+`) `+mppCols); err != nil {
			t.Fatal(err)
		}
		tx, err := f.pool.Begin(f.ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(f.ctx) }()
		for i := 0; i < 2; i++ {
			if _, err := tx.Exec(f.ctx, `SELECT materialize_position_projection('pv_run_tx'::regclass)`); err != nil {
				t.Fatalf("run %d in one transaction: %v (created_at must be clock time, not the transaction time)", i+1, err)
			}
		}
		if err := tx.Commit(f.ctx); err != nil {
			t.Fatal(err)
		}
		if n, _ := runs(t, "pv_run_tx"); n != 2 {
			t.Errorf("two runs in one transaction recorded %d rows, want 2", n)
		}
	})

	t.Run("a failed run records nothing", func(t *testing.T) {
		// A double-emitted key fails the run before its inserts; the run row must roll back with it.
		body := `SELECT * FROM (VALUES ` + row("run-fail", 100) + "," + row("run-fail", 100) + `) ` + mppCols
		f.mppErr(t, "pv_run_fail", body, "must fail", "double-emits")
		if n, _ := runs(t, "pv_run_fail"); n != 0 {
			t.Errorf("a failed run left %d run rows, want 0", n)
		}
	})

	t.Run("an empty projection still records a completed sweep, with no instant", func(t *testing.T) {
		body := `SELECT * FROM (VALUES ` + row("run-empty", 1) + `) ` + mppCols + ` WHERE false`
		if n := f.mppN(t, "pv_run_empty", body, "empty"); n != 0 {
			t.Fatalf("empty projection inserted %d, want 0", n)
		}
		n, latest := runs(t, "pv_run_empty")
		if n != 1 || latest != nil {
			t.Errorf("empty sweep: %d rows, latest %v; want 1 row with NULL block_timestamp", n, latest)
		}
	})

	t.Run("a mixed on-chain and off-chain projection records a comparable instant", func(t *testing.T) {
		// An off-chain row's block_number is epoch seconds (~1.7e9); max(block_number) would have made
		// every on-chain position in the projection look stale forever. The instant is comparable.
		body := `SELECT * FROM (VALUES ` + row("run-mixed-on", 100) + `,` +
			`(NULL::int,NULL::bigint,'run-mixed-off'::text,'` + strings.Repeat("b", 40) + `'::text,5::numeric,'CUSTODY'::text,` +
			`1772409600::bigint,0::int,0::int,'2026-03-02T00:00:00Z'::timestamptz)) ` + mppCols
		if n := f.mppN(t, "pv_run_mixed", body, "mixed"); n != 2 {
			t.Fatalf("mixed projection inserted %d, want 2", n)
		}
		_, latest := runs(t, "pv_run_mixed")
		if latest == nil || !latest.Equal(day(2)) {
			t.Errorf("mixed sweep latest = %v, want 2026-03-02 (the later of the two instants)", latest)
		}
	})
}

// psTestClosure: the materializer decides which emitted rows are observations. Leading zeros and
// repeated zeros are not; the first zero after a positive is the close, and a zero at the same block as
// its predecessor (a reorg or reprocess of the close) is kept so the canonical version survives.
func psTestClosure(t *testing.T, f *psFixture) {
	row := func(ik string, qty, bn, bv, pv int, ts string) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + strings.Repeat("c", 40) + "'::text," + strconv.Itoa(qty) + "::numeric,'LOAN'::text," +
			strconv.Itoa(bn) + "::bigint," + strconv.Itoa(bv) + "::int," + strconv.Itoa(pv) + "::int,'" + ts + "'::timestamptz)"
	}
	stored := func(t *testing.T, ik string) []string {
		t.Helper()
		var got []string
		if err := f.pool.QueryRow(f.ctx, `
			SELECT coalesce(array_agg(block_number::text || 'v' || block_version::text || 'p' || processing_version::text || '=' || quantity::text
			                          ORDER BY block_number, block_version, processing_version), '{}')
			FROM position_state WHERE instrument_key = $1`, ik).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", ik, err)
		}
		return got
	}
	for _, c := range []struct {
		name string
		rows string
		want []string
	}{
		{"leading zeros are dropped, the first positive opens", row("cl-lead", 0, 100, 0, 0, "2026-05-01") + "," + row("cl-lead", 0, 200, 0, 0, "2026-05-02") + "," + row("cl-lead", 7, 300, 0, 0, "2026-05-03"), []string{"300v0p0=7"}},
		{"the first zero after a positive is the close, repeated zeros are dropped", row("cl-close", 5, 100, 0, 0, "2026-05-01") + "," + row("cl-close", 0, 200, 0, 0, "2026-05-02") + "," + row("cl-close", 0, 300, 0, 0, "2026-05-03"), []string{"100v0p0=5", "200v0p0=0"}},
		{"a reorg sibling of the closing block is kept", row("cl-reorg", 5, 100, 0, 0, "2026-05-01") + "," + row("cl-reorg", 0, 200, 0, 0, "2026-05-02") + "," + row("cl-reorg", 0, 200, 1, 0, "2026-05-02T00:00:12Z"), []string{"100v0p0=5", "200v0p0=0", "200v1p0=0"}},
		{"a reprocess sibling of the closing block is kept", row("cl-repro", 5, 100, 0, 0, "2026-05-01") + "," + row("cl-repro", 0, 200, 0, 0, "2026-05-02") + "," + row("cl-repro", 0, 200, 0, 1, "2026-05-02"), []string{"100v0p0=5", "200v0p0=0", "200v0p1=0"}},
		{"a never-open position with a reorged leading zero emits nothing", row("cl-never", 0, 100, 0, 0, "2026-05-01") + "," + row("cl-never", 0, 100, 1, 0, "2026-05-01T00:00:12Z"), []string{}},
		{"a re-open after a close survives", row("cl-reopen", 5, 100, 0, 0, "2026-05-01") + "," + row("cl-reopen", 0, 200, 0, 0, "2026-05-02") + "," + row("cl-reopen", 7, 300, 0, 0, "2026-05-03"), []string{"100v0p0=5", "200v0p0=0", "300v0p0=7"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			f.mppN(t, "pv_"+strings.ReplaceAll(strings.Fields(c.name)[0]+strings.Fields(c.name)[1], "-", ""), `SELECT * FROM (VALUES `+c.rows+`) `+mppCols, c.name)
			ik := strings.SplitN(strings.TrimPrefix(c.rows, "(1::int,10::bigint,'"), "'", 2)[0]
			if got := stored(t, ik); strings.Join(got, ",") != strings.Join(c.want, ",") {
				t.Errorf("stored %v, want %v", got, c.want)
			}
		})
	}

	t.Run("an out-of-order arrival: a zero seen first is a leading zero, and becomes the close once the positive lands", func(t *testing.T) {
		const ik = "cl-ooo"
		if n := f.mppN(t, "pv_clooo", `SELECT * FROM (VALUES `+row(ik, 0, 200, 0, 0, "2026-05-02")+`) `+mppCols, "zero first"); n != 0 {
			t.Fatalf("a lone zero inserted %d rows, want 0", n)
		}
		if n := f.mppN(t, "pv_clooo", `SELECT * FROM (VALUES `+row(ik, 5, 100, 0, 0, "2026-05-01")+","+row(ik, 0, 200, 0, 0, "2026-05-02")+`) `+mppCols, "history complete"); n != 2 {
			t.Fatalf("the completed history inserted %d rows, want 2 (the open and the close)", n)
		}
		if got := stored(t, ik); strings.Join(got, ",") != "100v0p0=5,200v0p0=0" {
			t.Errorf("stored %v, want the open and the close", got)
		}
	})
}

// psTestOffChainObservations: a custody snapshot has no block. The observation key is (position, block,
// versions), so distinct snapshots need distinct blocks; off-chain rows carry chain_id NULL and
// block_number = floor(epoch seconds of block_timestamp), which the materializer enforces.
func psTestOffChainObservations(t *testing.T, f *psFixture) {
	epoch := func(ts string) int64 {
		tm, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			t.Fatal(err)
		}
		return tm.Unix()
	}
	snap := func(ik string, qty int, ts string, bn int64) string {
		return "(NULL::int,NULL::bigint,'" + ik + "'::text,'" + strings.Repeat("c", 40) + "'::text," + strconv.Itoa(qty) +
			"::numeric,'CUSTODY'::text," + strconv.FormatInt(bn, 10) + "::bigint,0::int,0::int,'" + ts + "'::timestamptz)"
	}
	onchain := func(ik string, bn int, ts string) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + strings.Repeat("c", 40) + "'::text,5::numeric,'LOAN'::text," +
			strconv.Itoa(bn) + "::bigint,0::int,0::int,'" + ts + "'::timestamptz)"
	}
	t1, t2, t3 := "2026-05-01T00:00:00Z", "2026-05-02T00:00:00Z", "2026-05-03T00:00:00Z"

	t.Run("snapshots are distinct observations and newer-wins follows the instant", func(t *testing.T) {
		body := `SELECT * FROM (VALUES ` + snap("cust-a", 10, t1, epoch(t1)) + "," + snap("cust-a", 20, t2, epoch(t2)) + "," +
			snap("cust-a", 15, t3, epoch(t3)) + `) ` + mppCols
		if n := f.mppN(t, "pv_cust_a", body, "snapshots"); n != 3 {
			t.Fatalf("three snapshots inserted %d rows, want 3", n)
		}
		var newest int
		if err := f.pool.QueryRow(f.ctx, `SELECT quantity FROM position_state WHERE instrument_key = 'cust-a'
			 ORDER BY block_number DESC, block_version DESC, processing_version DESC, block_timestamp DESC LIMIT 1`).Scan(&newest); err != nil {
			t.Fatal(err)
		}
		if newest != 15 {
			t.Errorf("newest off-chain observation has quantity %d, want 15", newest)
		}
	})

	t.Run("an off-chain row whose block_number is not its instant is refused", func(t *testing.T) {
		f.mppErr(t, "pv_cust_bad", `SELECT * FROM (VALUES `+snap("cust-bad", 10, t1, 0)+`) `+mppCols, "block 0", "not floor(epoch")
		f.mppErr(t, "pv_cust_bad2", `SELECT * FROM (VALUES `+snap("cust-bad2", 10, t1, epoch(t1)+1)+`) `+mppCols, "off by one", "not floor(epoch")
	})

	// Negative controls: the epoch rule does not touch on-chain rows, and one projection may carry
	// off-chain and on-chain POSITIONS side by side.
	t.Run("on-chain rows are untouched and the two kinds may share a projection", func(t *testing.T) {
		body := `SELECT * FROM (VALUES ` + snap("cust-b", 10, t1, epoch(t1)) + "," + onchain("chain-b", 100, t1) + `) ` + mppCols
		if n := f.mppN(t, "pv_cust_both", body, "two positions"); n != 2 {
			t.Errorf("two positions inserted %d, want 2", n)
		}
	})
}
