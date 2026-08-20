//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// mppCols is the aliased column list the projection-view contract requires.
const mppCols = `v(chain_id,protocol_id,instrument_key,holder_id,quantity,deal_type_code,block_number,block_version,processing_version,block_timestamp)`

// TestPositionState drives the real materialize_position_projection through one shared schema (a single
// ApplyAll) across the adversarial cases from this PR's review history — every subtest fails on the
// pre-fix code it guards. Subtests use distinct instrument_keys so they share state safely; setup is
// shared per stl-verify/AGENTS.md ("Share setup, don't repeat it").
func TestPositionState(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations: %v", err)
	}

	// mpp (re)creates a projection view from a VALUES body and runs the materializer, expecting success.
	mpp := func(t *testing.T, name, valuesBody, reason string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+name+` AS `+valuesBody); err != nil {
			t.Fatalf("create view %s: %v", name, err)
		}
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass, $2)`, name, reason); err != nil {
			t.Fatalf("materialize %s: %v", name, err)
		}
	}
	// mppErr runs the materializer expecting an error whose text contains want.
	mppErr := func(t *testing.T, name, valuesBody, reason, want string) {
		t.Helper()
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+name+` AS `+valuesBody); err != nil {
			t.Fatalf("create view %s: %v", name, err)
		}
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass, $2)`, name, reason)
		if err == nil {
			t.Fatalf("%s: expected an error containing %q, got none", name, want)
		}
		if want != "" && !strings.Contains(err.Error(), want) {
			t.Errorf("%s: error = %v, want it to contain %q", name, err, want)
		}
	}
	// classOf returns the current deal_type_code for position (1,10,ik,holder), or "" if unclassified.
	classOf := func(t *testing.T, ik, holder string) string {
		t.Helper()
		var dt string
		err := pool.QueryRow(ctx,
			`SELECT deal_type_code FROM position_classification WHERE position_id = position_id(1, 10, $1, $2)`,
			ik, holder).Scan(&dt)
		if err != nil {
			if strings.Contains(err.Error(), "no rows") {
				return ""
			}
			t.Fatalf("classOf(%s, %s): %v", ik, holder, err)
		}
		return dt
	}
	row := func(ik, holder string, qty int, code string, bn, bv, pv int) string {
		return "(1::int,10::bigint,'" + ik + "'::text,'" + holder + "'::text," + strconv.Itoa(qty) + "::numeric,'" + code + "'::text," +
			strconv.Itoa(bn) + "::bigint," + strconv.Itoa(bv) + "::int," + strconv.Itoa(pv) + "::int,'2026-01-01'::timestamptz)"
	}
	valuesOf := func(rows ...string) string {
		return `SELECT * FROM (VALUES ` + strings.Join(rows, ",") + `) ` + mppCols
	}

	// Mutation audit — every guard in 20260818_130000_create_position_state.sql mapped to the
	// subtest that fails if that guard is deleted or weakened. Reviewers: this table is the
	// coverage claim; each row was verified by reproducing the mutation on a scratch build.
	//
	//   guard / behavior                          killed by subtest
	//   ----------------------------------------  ------------------------------------------------
	//   position_state_qty_nonneg_chk             negative/NaN/Infinity quantity rejected
	//   position_state_coord_nonneg_chk           negative block_number / bv+pv rejected
	//   position_state_chain_pos_chk              zero or negative chain_id rejected
	//   position_state_protocol_pos_chk           zero or negative chain_id/protocol_id rejected
	//   position_state_ts_sane_chk                pre-blockchain block_timestamp rejected
	//   position_state_id_len_chk                 1-byte position_id rejected (direct insert)
	//   position_state_holder_hex_chk             holder case/prefix/non-hex cases (input matrix)
	//   pre-flight (1) column contract            float8 fails / missing column MISSING
	//   pre-flight (2) double-emit                double-emitted logical key raises
	//   pre-flight (3) changed block_timestamp    re-emitted observation with changed ts raises
	//   pre-flight (4) NULL code on non-zero      null deal_type_code on non-zero raises
	//   p_reason / p_view argument guards         null/blank p_reason; NULL p_view subtests
	//   recency guard bn component                older-block backfill does not regress
	//   recency guard bv component                same-height older version does not regress
	//   recency guard pv component (deny)         older processing_version does not regress
	//   recency guard lexicographic order         block_version bump outranks higher stored pv
	//   merged-canonical (fix a, reorg drop)      reorg-wedge reclassifies down (:279)
	//   merged-canonical (fix b, run visibility)  self-snapshot correction reclassifies (:278)
	//   canonicality join (round 7)               stale re-emission cannot rewrite / cannot mint
	//   cls FK fail-hard (LEFT JOIN + raw code)   unseeded deal_type_code fails the run
	//   cls no-op guard (IS DISTINCT FROM)        no-op rerun preserves change_reason
	//   direction frozen from ref_deal_type       no-op rerun preserves change_reason (direction)
	//   valid_from UTC stamp                      classification stamps valid_from as UTC today
	//   quantity-only upsert (ins arm)            qty rewrite leaves created_at untouched
	//   column-scoped UPDATE grants               narrowed grants: rewrite denied suite
	//   TRUNCATE / readonly / collateral locks    TRUNCATE denied and stl_readonly is read-only
	//   advisory lock exists + key stability      advisory lock key is search_path-independent
	//   advisory lock serializes same view        concurrent same-view runs serialize
	//   advisory lock does NOT over-serialize     different views run concurrently
	//   pg_temp-qualified reads (round 7)         poisoned permanent _mpp_src is never read
	//   pg_temp DROP qualification                temp-table drop spares a permanent _mpp_src
	//   temp lifecycle                            temp snapshot is not left behind after a run
	//   hypertable + 1-day chunks                 position_state is a hypertable with 1-day chunks
	//   no default ts index                       no default block_timestamp index
	//   cross-chunk correctness                   12-day chunk reorg / model fuzz
	//   whole-run atomicity                       within-write failure leaves rows untouched
	//   return-count contract                     return value counts rows inserted or changed
	//   state-space beyond hand-picked cases      model fuzz (seeded, oracle-mirrored)

	// --- recency guard ---------------------------------------------------------------------------

	t.Run("same-height older version does not regress (CodeRabbit)", func(t *testing.T) {
		mpp(t, "vg", valuesOf(row("ig", "aa", 5, "COLLATERAL", 100, 1, 0)), "store")
		mpp(t, "vg", valuesOf(row("ig", "aa", 5, "LOAN", 100, 0, 0)), "older")
		if got := classOf(t, "ig", "aa"); got != "COLLATERAL" {
			t.Errorf("older same-height version regressed to %q; want COLLATERAL (full-tuple recency guard)", got)
		}
	})

	t.Run("reorg-wedge reclassifies down (finding :279)", func(t *testing.T) {
		mpp(t, "vf1", valuesOf(row("if1", "aa", 7, "BORROW", 90, 0, 0), row("if1", "aa", 5, "LOAN", 100, 0, 0)), "store")
		mpp(t, "vf1", valuesOf(row("if1", "aa", 7, "BORROW", 90, 0, 0), row("if1", "aa", 0, "LOAN", 100, 1, 0)), "reorg")
		if got := classOf(t, "if1", "aa"); got != "BORROW" {
			t.Errorf("after a reorg zeroed block 100, class = %q; want BORROW (canonicalised high-water must drop the reorged-out row)", got)
		}
	})

	t.Run("self-snapshot correction reclassifies (finding :278)", func(t *testing.T) {
		mpp(t, "vf2", valuesOf(row("if2", "ab", 7, "BORROW", 140, 0, 0), row("if2", "ab", 10, "LOAN", 150, 0, 0)), "store")
		mpp(t, "vf2", valuesOf(row("if2", "ab", 7, "BORROW", 140, 0, 0), row("if2", "ab", 0, "LOAN", 150, 0, 0)), "zero150")
		if got := classOf(t, "if2", "ab"); got != "BORROW" {
			t.Errorf("a run that zeroed its own stored latest did not reclassify: class = %q; want BORROW (high-water must see the run's own writes)", got)
		}
	})

	t.Run("newer processing_version reclassifies", func(t *testing.T) {
		mpp(t, "vn", valuesOf(row("in", "aa", 5, "COLLATERAL", 100, 1, 0)), "store")
		mpp(t, "vn", valuesOf(row("in", "aa", 5, "BORROW", 100, 1, 1)), "newerpv")
		if got := classOf(t, "in", "aa"); got != "BORROW" {
			t.Errorf("newer processing_version did not reclassify: class = %q; want BORROW", got)
		}
	})

	t.Run("older processing_version at same coordinates does not regress", func(t *testing.T) {
		// Deny-side pv coverage: the guard tuple is (bn, bv, pv); a mutation dropping pv from the
		// comparison would let an older-pv re-emission regress the classification.
		mpp(t, "vpd", valuesOf(row("ipd", "aa", 5, "COLLATERAL", 100, 0, 1)), "store")
		mpp(t, "vpd", valuesOf(row("ipd", "aa", 5, "LOAN", 100, 0, 0)), "olderpv")
		if got := classOf(t, "ipd", "aa"); got != "COLLATERAL" {
			t.Errorf("older-pv re-emission regressed to %q; want COLLATERAL (pv is part of the guard tuple)", got)
		}
	})

	t.Run("a block_version bump outranks a higher stored processing_version (lexicographic guard)", func(t *testing.T) {
		// Accept-side: a reorg (bv bump) resets pv; lexicographic >= must admit (100,1,0) over (100,0,5).
		// A wrong all-components->= implementation would wedge every post-reorg reclassification.
		mpp(t, "vbo", valuesOf(row("ibo", "aa", 5, "COLLATERAL", 100, 0, 5)), "store")
		mpp(t, "vbo", valuesOf(row("ibo", "aa", 7, "BORROW", 100, 1, 0)), "reorg")
		if got := classOf(t, "ibo", "aa"); got != "BORROW" {
			t.Errorf("bv bump with pv reset did not reclassify: %q; want BORROW", got)
		}
	})

	t.Run("stale re-emission of a reorged-out row cannot rewrite the classification", func(t *testing.T) {
		// Round-7 fix: latest is canonicalized over the RUN's rows only, so a run re-emitting just a stale
		// lower-version row (raw pruned of the superseding version) passed the recency guard whenever the
		// superseding stored row was a ZERO (hwm drops that block; COALESCE(-1) admits anything) and
		// silently rewrote the classification from a reorged-out observation. The canonicality join
		// (latest row must BE the merged-canonical row at its block) closes it. Fails on pre-fix code.
		mpp(t, "vsr", valuesOf(row("isr", "aa", 5, "LOAN", 100, 0, 0)), "seed")
		mpp(t, "vsr", valuesOf(row("isr", "aa", 5, "LOAN", 100, 0, 0), row("isr", "aa", 0, "LOAN", 100, 1, 0)), "reorg-close")
		mpp(t, "vsr", valuesOf(row("isr", "aa", 5, "BORROW", 100, 0, 0)), "stale-reemit")
		if got := classOf(t, "isr", "aa"); got != "LOAN" {
			t.Errorf("stale reorged-out re-emission rewrote the classification to %q; want LOAN held", got)
		}
	})

	t.Run("stale re-emission cannot mint a classification for an all-zero position", func(t *testing.T) {
		mpp(t, "vsm", valuesOf(row("ism", "aa", 5, "LOAN", 100, 0, 0), row("ism", "aa", 0, "LOAN", 100, 1, 0)), "seed-closed")
		if got := classOf(t, "ism", "aa"); got != "" {
			t.Fatalf("all-zero canonical history classified as %q at seed", got)
		}
		mpp(t, "vsm", valuesOf(row("ism", "aa", 5, "BORROW", 100, 0, 0)), "stale-reemit")
		if got := classOf(t, "ism", "aa"); got != "" {
			t.Errorf("stale re-emission minted classification %q for a canonically all-zero position; want unclassified", got)
		}
	})

	t.Run("closed position reclassifies (finding :269)", func(t *testing.T) {
		mpp(t, "vc", valuesOf(row("ic", "ac", 5, "LOAN", 100, 0, 0), row("ic", "ac", 0, "LOAN", 110, 0, 0)), "init")
		mpp(t, "vc", valuesOf(row("ic", "ac", 5, "COLLATERAL", 100, 0, 1)), "correct")
		if got := classOf(t, "ic", "ac"); got != "COLLATERAL" {
			t.Errorf("closed position did not reclassify: class = %q; want COLLATERAL", got)
		}
	})

	t.Run("older-block backfill does not regress", func(t *testing.T) {
		mpp(t, "vb", valuesOf(row("ib", "ad", 100, "BORROW", 5000, 0, 0)), "cur")
		mpp(t, "vb", valuesOf(row("ib", "ad", 50, "LOAN", 1500, 0, 0)), "backfill")
		if got := classOf(t, "ib", "ad"); got != "BORROW" {
			t.Errorf("older-block backfill regressed to %q; want BORROW", got)
		}
	})

	t.Run("all-zero history stays unclassified", func(t *testing.T) {
		mpp(t, "vz", valuesOf(row("iz", "af", 0, "LOAN", 100, 0, 0)), "z")
		if got := classOf(t, "iz", "af"); got != "" {
			t.Errorf("all-zero history classified as %q; want unclassified", got)
		}
	})

	t.Run("two positions in one run stay isolated", func(t *testing.T) {
		mpp(t, "vmp", valuesOf(row("imA", "aa", 5, "COLLATERAL", 100, 1, 0), row("imB", "ab", 5, "BORROW", 5000, 0, 0)), "store")
		mpp(t, "vmp", valuesOf(row("imA", "aa", 5, "LOAN", 100, 0, 0), row("imB", "ab", 5, "LOAN", 1500, 0, 0)), "run")
		if got := classOf(t, "imA", "aa"); got != "COLLATERAL" {
			t.Errorf("position A regressed to %q; want COLLATERAL", got)
		}
		if got := classOf(t, "imB", "ab"); got != "BORROW" {
			t.Errorf("position B regressed to %q; want BORROW", got)
		}
	})

	t.Run("un-zeroing reopens a closed position", func(t *testing.T) {
		mpp(t, "vuz", valuesOf(row("iuz", "aa", 0, "BORROW", 100, 0, 0)), "closed")
		if got := classOf(t, "iuz", "aa"); got != "" {
			t.Errorf("all-zero position classified as %q; want unclassified", got)
		}
		mpp(t, "vuz", valuesOf(row("iuz", "aa", 5, "BORROW", 100, 0, 0)), "reopen")
		if got := classOf(t, "iuz", "aa"); got != "BORROW" {
			t.Errorf("reopened position = %q; want BORROW", got)
		}
	})

	t.Run("deep reorg moves the classification down two blocks", func(t *testing.T) {
		mpp(t, "vdr", valuesOf(row("idr", "aa", 7, "BORROW", 10, 0, 0), row("idr", "aa", 6, "LOAN", 20, 0, 0), row("idr", "aa", 5, "COLLATERAL", 30, 0, 0)), "store")
		if got := classOf(t, "idr", "aa"); got != "COLLATERAL" {
			t.Errorf("store class = %q; want COLLATERAL@30", got)
		}
		mpp(t, "vdr", valuesOf(row("idr", "aa", 7, "BORROW", 10, 0, 0), row("idr", "aa", 6, "LOAN", 20, 0, 0), row("idr", "aa", 0, "COLLATERAL", 30, 1, 0)), "reorg")
		if got := classOf(t, "idr", "aa"); got != "LOAN" {
			t.Errorf("after reorg zeroing block 30, class = %q; want LOAN@20", got)
		}
	})

	t.Run("full close keeps the prior classification (existing semantics)", func(t *testing.T) {
		mpp(t, "vfc", valuesOf(row("ifc", "aa", 5, "BORROW", 100, 0, 0)), "open")
		mpp(t, "vfc", valuesOf(row("ifc", "aa", 0, "BORROW", 110, 0, 0)), "close")
		if got := classOf(t, "ifc", "aa"); got != "BORROW" {
			t.Errorf("fully-closed position class = %q; want BORROW (last classification kept)", got)
		}
	})

	// --- input robustness ------------------------------------------------------------------------

	t.Run("null deal_type_code on a non-zero observation raises (finding :283)", func(t *testing.T) {
		mppErr(t, "vnull",
			`SELECT * FROM (VALUES (1::int,10::bigint,'inl'::text,'aa'::text,5::numeric,NULL::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) `+mppCols,
			"nullcode", "NULL deal_type_code")
	})

	t.Run("null deal_type_code on a zero row is allowed; position unclassified", func(t *testing.T) {
		mpp(t, "vnz", `SELECT * FROM (VALUES (1::int,10::bigint,'inz'::text,'aa'::text,0::numeric,NULL::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) `+mppCols, "zeronull")
		if got := classOf(t, "inz", "aa"); got != "" {
			t.Errorf("zero-row NULL-coded position classified as %q; want unclassified", got)
		}
	})

	t.Run("mixed batch: NULL-code closing zero row alongside a coded non-zero row", func(t *testing.T) {
		// Every reorg fixture's zero row carries a code; the realistic close is uncoded. The NULL-code
		// zero row must be allowed in a mixed batch and the classification must come from the coded row.
		body := `SELECT * FROM (VALUES ` +
			`(1::int,10::bigint,'imx'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz),` +
			`(1::int,10::bigint,'imx'::text,'aa'::text,0::numeric,NULL::text,110::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mpp(t, "vmx", body, "mixed")
		if got := classOf(t, "imx", "aa"); got != "LOAN" {
			t.Errorf("mixed batch class = %q; want LOAN from the coded non-zero row", got)
		}
	})

	t.Run("no-op rerun preserves change_reason (guarded classification update)", func(t *testing.T) {
		mpp(t, "vnp", valuesOf(row("inp", "aa", 5, "LOAN", 100, 0, 0)), "first-reason")
		mpp(t, "vnp", valuesOf(row("inp", "aa", 5, "LOAN", 100, 0, 0)), "second-reason")
		var reason, direction string
		if err := pool.QueryRow(ctx,
			`SELECT change_reason, direction FROM position_classification WHERE position_id = position_id(1,10,'inp','aa')`).Scan(&reason, &direction); err != nil {
			t.Fatal(err)
		}
		if reason != "first-reason" {
			t.Errorf("no-op rerun overwrote change_reason to %q; want first-reason preserved (IS DISTINCT FROM guard)", reason)
		}
		if direction != "LONG" {
			t.Errorf("direction = %q; want LONG frozen from ref_deal_type", direction)
		}
	})

	t.Run("null and blank p_reason are rejected (finding :286)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vpr AS `+valuesOf(row("ipr", "aa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		want := "p_reason must be a non-empty change_reason"
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection('vpr'::regclass, $1)`, nil); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("NULL p_reason: got %v; want the p_reason raise", err)
		}
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection('vpr'::regclass, '   ')`); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("blank p_reason: got %v; want the p_reason raise", err)
		}
	})

	t.Run("numeric(30,18) quantity passes the contract (finding :197)", func(t *testing.T) {
		mpp(t, "vtm", `SELECT 1::int chain_id,10::bigint protocol_id,'itm'::text instrument_key,'aa'::text holder_id,`+
			`5::numeric(30,18) quantity,'LOAN'::text deal_type_code,100::bigint block_number,0::int block_version,`+
			`0::int processing_version,'2026-01-01'::timestamptz block_timestamp`, "typmod")
		if got := classOf(t, "itm", "aa"); got != "LOAN" {
			t.Errorf("numeric(30,18) view: class = %q; want LOAN (contract must compare base type, not typmod)", got)
		}
	})

	t.Run("float8 quantity fails the contract", func(t *testing.T) {
		mppErr(t, "vfl", `SELECT 1::int chain_id,10::bigint protocol_id,'ifl'::text instrument_key,'aa'::text holder_id,`+
			`1.5::float8 quantity,'LOAN'::text deal_type_code,1::bigint block_number,0::int block_version,`+
			`0::int processing_version,'2026-01-01'::timestamptz block_timestamp`, "flt", "column contract")
	})

	t.Run("double-emitted logical key raises", func(t *testing.T) {
		mppErr(t, "vde", valuesOf(row("ide", "aa", 7, "LOAN", 600, 0, 0), row("ide", "aa", 9, "LOAN", 600, 0, 0)), "de", "double-emit")
	})

	t.Run("re-emitted observation with a changed block_timestamp raises", func(t *testing.T) {
		mpp(t, "vts", valuesOf(row("its", "aa", 5, "LOAN", 100, 0, 0)), "store")
		mppErr(t, "vts", `SELECT * FROM (VALUES (1::int,10::bigint,'its'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-09-09'::timestamptz)) `+mppCols, "shift", "changed block_timestamp")
	})

	t.Run("uppercase holder rejected by the hex CHECK", func(t *testing.T) {
		mppErr(t, "vuh", `SELECT * FROM (VALUES (1::int,10::bigint,'iuh'::text,'0xAB'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) `+mppCols, "uh", "position_state_holder_hex_chk")
	})

	t.Run("Infinity quantity rejected by the CHECK", func(t *testing.T) {
		mppErr(t, "vinf", `SELECT * FROM (VALUES (1::int,10::bigint,'iinf'::text,'aa'::text,'Infinity'::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) `+mppCols, "inf", "qty_nonneg")
	})

	// --- structure / physical -------------------------------------------------------------------

	t.Run("happy path fills spine + classification; rerun is a no-op", func(t *testing.T) {
		mpp(t, "vh", valuesOf(row("ih", "aa", 100, "BORROW", 500, 0, 0), row("ih", "aa", 0, "BORROW", 600, 0, 0)), "happy")
		if got := classOf(t, "ih", "aa"); got != "BORROW" {
			t.Errorf("happy class = %q; want BORROW", got)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vh'::regclass, 'happy')`).Scan(&n); err != nil {
			t.Fatalf("rerun: %v", err)
		}
		if n != 0 {
			t.Errorf("rerun changed %d rows; want 0 (idempotent)", n)
		}
	})

	t.Run("temp-table drop spares a permanent _mpp_src (finding :206)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE TABLE public._mpp_src (sentinel int)`); err != nil {
			t.Fatalf("create permanent _mpp_src: %v", err)
		}
		defer pool.Exec(ctx, `DROP TABLE IF EXISTS public._mpp_src`)
		if _, err := pool.Exec(ctx, `INSERT INTO public._mpp_src VALUES (777)`); err != nil {
			t.Fatal(err)
		}
		mpp(t, "vp", valuesOf(row("ip", "aa", 5, "LOAN", 100, 0, 0)), "perm")
		var sentinel int
		if err := pool.QueryRow(ctx, `SELECT sentinel FROM public._mpp_src`).Scan(&sentinel); err != nil {
			t.Fatalf("the materializer dropped the caller's permanent _mpp_src: %v", err)
		}
		if sentinel != 777 {
			t.Errorf("permanent _mpp_src sentinel = %d; want 777", sentinel)
		}
	})

	t.Run("poisoned permanent _mpp_src is never read (pg_temp-qualified reads)", func(t *testing.T) {
		// A caller listing pg_temp LAST in search_path would previously have every pre-flight check and
		// both writes read a permanent _mpp_src instead of the just-built temp snapshot. Reads are now
		// pg_temp-qualified; the poison rows must not land in position_state. Fails on pre-fix code.
		if _, err := pool.Exec(ctx, `CREATE TABLE public._mpp_src (chain_id int, protocol_id bigint, instrument_key text, holder_id text, quantity numeric, deal_type_code text, block_number bigint, block_version int, processing_version int, block_timestamp timestamptz)`); err != nil {
			t.Fatal(err)
		}
		defer pool.Exec(ctx, `DROP TABLE IF EXISTS public._mpp_src`)
		if _, err := pool.Exec(ctx, `INSERT INTO public._mpp_src VALUES (1,10,'ipsn','aa',999,'LOAN',100,0,0,'2026-01-01')`); err != nil {
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
		if _, err := tx.Exec(ctx, `SET LOCAL search_path = public, pg_temp`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `CREATE OR REPLACE VIEW vps AS `+valuesOf(row("ips", "aa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('vps'::regclass, 'poisoned-path')`); err != nil {
			t.Fatalf("run under hostile search_path: %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		var poison int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE position_id = position_id(1,10,'ipsn','aa')`).Scan(&poison); err != nil {
			t.Fatal(err)
		}
		if poison != 0 {
			t.Errorf("permanent _mpp_src poison rows written: %d; want 0 (reads must be pg_temp-qualified)", poison)
		}
		if got := classOf(t, "ips", "aa"); got != "LOAN" {
			t.Errorf("legitimate row under hostile search_path classified %q; want LOAN", got)
		}
	})

	t.Run("NULL p_view raises instead of silently skipping the lock", func(t *testing.T) {
		_, err := pool.Exec(ctx, `SELECT materialize_position_projection(NULL::regclass, 'r')`)
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

	t.Run("reorg across chunk boundaries reclassifies (12 observations, 12 daily chunks)", func(t *testing.T) {
		// Every other fixture uses one timestamp, so the guard's stored∪run merge had never been
		// exercised across real chunk boundaries. Twelve observations, one per day (one per 1-day
		// chunk): blocks 100..111, all LOAN except block 108 (COLLATERAL). A reorg then zeroes the
		// top three blocks at bv=1 — the canonical latest non-zero moves down three chunks to 108
		// and the classification must follow it.
		obs := func(qty int, code string, bn, bv int) string {
			day := strconv.Itoa(bn - 100 + 1)
			return "(1::int,10::bigint,'ixc'::text,'aa'::text," + strconv.Itoa(qty) + "::numeric," + code + "::text," +
				strconv.Itoa(bn) + "::bigint," + strconv.Itoa(bv) + "::int,0::int,timestamptz '2026-03-01 12:00+00' + interval '" + day + " days')"
		}
		var seed, reorg []string
		for bn := 100; bn <= 111; bn++ {
			code := "'LOAN'"
			if bn == 108 {
				code = "'COLLATERAL'"
			}
			seed = append(seed, obs(5, code, bn, 0))
			if bn >= 109 {
				reorg = append(reorg, obs(0, code, bn, 1))
			} else {
				reorg = append(reorg, obs(5, code, bn, 0))
			}
		}
		mpp(t, "vxc", `SELECT * FROM (VALUES `+strings.Join(seed, ",")+`) `+mppCols, "seed")
		if got := classOf(t, "ixc", "aa"); got != "LOAN" {
			t.Fatalf("seed class = %q; want LOAN (latest non-zero is block 111)", got)
		}
		mpp(t, "vxc", `SELECT * FROM (VALUES `+strings.Join(reorg, ",")+`) `+mppCols, "reorg-top-three")
		if got := classOf(t, "ixc", "aa"); got != "COLLATERAL" {
			t.Errorf("after zeroing blocks 109-111 across three chunks, class = %q; want COLLATERAL from block 108", got)
		}
		var chunks int
		if err := pool.QueryRow(ctx, `SELECT count(DISTINCT chunk_name) FROM timescaledb_information.chunks WHERE hypertable_name = 'position_state'`).Scan(&chunks); err != nil {
			t.Fatal(err)
		}
		if chunks < 12 {
			t.Errorf("fixtures landed in %d chunk(s); the multi-chunk premise requires >= 12", chunks)
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
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW lockprobe AS `+valuesOf(row("ilk", "aa", 1, "LOAN", 1, 0, 0))); err != nil {
			t.Fatal(err)
		}
		// Call the REAL function inside a transaction under a given search_path, then read the advisory
		// lock it is holding (xact lock, still held pre-commit). Comparing the held (classid,objid) across
		// two search_paths pins the actual key — a hardcoded copy of the expression could not.
		heldLock := func(searchPath string) (int64, int64) {
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
			if _, err := tx.Exec(ctx, `SET LOCAL search_path = `+searchPath); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('public.lockprobe'::regclass, 'lock')`); err != nil {
				t.Fatalf("materialize under %s: %v", searchPath, err)
			}
			var classid, objid int64
			if err := tx.QueryRow(ctx,
				`SELECT classid::bigint, objid::bigint FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() ORDER BY classid, objid LIMIT 1`).Scan(&classid, &objid); err != nil {
				t.Fatalf("read advisory lock under %s: %v", searchPath, err)
			}
			return classid, objid
		}
		c1, o1 := heldLock("public")
		c2, o2 := heldLock("pg_catalog, public")
		if c1 != c2 || o1 != o2 {
			t.Errorf("advisory lock key differs across search_path: public=(%d,%d) pg_catalog=(%d,%d); the per-view lock would not serialize concurrent runs", c1, o1, c2, o2)
		}
	})

	// --- identity integrity (position_key contract, not exercised above) ------------------------

	t.Run("blank holder_id rejected even on a zero-quantity row", func(t *testing.T) {
		// Identity fields feed position_id() for EVERY row, unlike deal_type_code (pre-flight (4) checks
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
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'a;b'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vsemi", body, "semi", "delimiter")
	})

	t.Run("null chain_id is legal and does not collide with a set chain_id", func(t *testing.T) {
		// chain_id/protocol_id are nullable structural fields (render empty in position_key). A NULL-chain
		// position must materialize AND hash distinctly from the same instrument/holder at chain 1.
		body := `SELECT * FROM (VALUES ` +
			`(NULL::int,10::bigint,'inc'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz),` +
			`(1::int,10::bigint,'inc'::text,'aa'::text,9::numeric,'BORROW'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mpp(t, "vnc", body, "nullchain")
		var nullClass, setClass string
		if err := pool.QueryRow(ctx, `SELECT deal_type_code FROM position_classification WHERE position_id = position_id(NULL, 10, 'inc', 'aa')`).Scan(&nullClass); err != nil {
			t.Fatalf("null-chain position not classified: %v", err)
		}
		if err := pool.QueryRow(ctx, `SELECT deal_type_code FROM position_classification WHERE position_id = position_id(1, 10, 'inc', 'aa')`).Scan(&setClass); err != nil {
			t.Fatalf("set-chain position not classified: %v", err)
		}
		if nullClass != "LOAN" || setClass != "BORROW" {
			t.Errorf("null vs set chain collided: null=%q set=%q; want LOAN and BORROW (distinct ids)", nullClass, setClass)
		}
	})

	// --- contract edges (missing / extra column, FK) --------------------------------------------

	t.Run("unseeded deal_type_code fails the run (FK, not a silent drop)", func(t *testing.T) {
		// The cls insert uses a LEFT JOIN + the raw code, so an unseeded/typo'd code must hit the
		// deal_type_code FK (23503) and fail the run rather than be dropped by an inner join.
		mppErr(t, "vfk", valuesOf(row("ifk", "aa", 5, "NOPE_NOT_A_CODE", 100, 0, 0)), "fk", "deal_type_fkey")
	})

	t.Run("view missing a contract column is rejected as MISSING", func(t *testing.T) {
		// Drop deal_type_code from the emitted columns; the contract check must name it MISSING before any write.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'imz'::text,'aa'::text,5::numeric,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) v(chain_id,protocol_id,instrument_key,holder_id,quantity,block_number,block_version,processing_version,block_timestamp)`
		mppErr(t, "vmz", body, "missing", "MISSING")
	})

	t.Run("extra column in the view is tolerated", func(t *testing.T) {
		// The temp projection selects contract columns by name, so an unrelated extra column is ignored.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ix'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz,'ignored'::text)) v(chain_id,protocol_id,instrument_key,holder_id,quantity,deal_type_code,block_number,block_version,processing_version,block_timestamp,extra)`
		mpp(t, "vx", body, "extra")
		if got := classOf(t, "ix", "aa"); got != "LOAN" {
			t.Errorf("extra-column view class = %q; want LOAN (extra columns ignored)", got)
		}
	})

	// --- recency guard edges (equal tuple, pv tiebreak) -----------------------------------------

	t.Run("same-coordinate re-emit with a changed code reclassifies", func(t *testing.T) {
		mpp(t, "vso", valuesOf(row("iso", "aa", 5, "LOAN", 100, 0, 0)), "store")
		// Identical (bn,bv,pv) and quantity, only the code changes: the quantity upsert is a no-op but the
		// classification must still move — the guard admits the equal high-water tuple (>=), and cls runs
		// independently of the position_state insert.
		mpp(t, "vso", valuesOf(row("iso", "aa", 5, "COLLATERAL", 100, 0, 0)), "reclassify")
		if got := classOf(t, "iso", "aa"); got != "COLLATERAL" {
			t.Errorf("same-coordinate re-emit did not reclassify: class = %q; want COLLATERAL", got)
		}
	})

	t.Run("processing_version tiebreak within a block closes on the higher pv", func(t *testing.T) {
		mpp(t, "vpv", valuesOf(row("ipv", "aa", 7, "BORROW", 90, 0, 0), row("ipv", "aa", 5, "LOAN", 100, 0, 0)), "store")
		// A higher pv at block 100 zeros it; canonical(block 100) must pick pv=1 (zero), so the latest
		// non-zero observation falls back to block 90 BORROW.
		mpp(t, "vpv", valuesOf(row("ipv", "aa", 7, "BORROW", 90, 0, 0), row("ipv", "aa", 0, "LOAN", 100, 0, 1)), "pvzero")
		if got := classOf(t, "ipv", "aa"); got != "BORROW" {
			t.Errorf("higher-pv zero at block 100 did not close it: class = %q; want BORROW", got)
		}
	})

	// --- quantity CHECK (negative, NaN) ---------------------------------------------------------

	t.Run("negative quantity rejected by the CHECK", func(t *testing.T) {
		mppErr(t, "vneg", valuesOf(row("ineg", "aa", -5, "LOAN", 100, 0, 0)), "neg", "qty_nonneg")
	})

	t.Run("NaN quantity rejected by the CHECK", func(t *testing.T) {
		// NaN sorts above every finite numeric, so it clears the quantity > 0 filter (the pre-flight sees a
		// non-null code and passes); the CHECK must still reject it before it poisons downstream SUMs.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'inan'::text,'aa'::text,'NaN'::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vnan", body, "nan", "qty_nonneg")
	})

	// --- empty projection, return count, multi-view txn, concurrency ----------------------------

	t.Run("empty projection is a no-op and preserves the stored classification", func(t *testing.T) {
		mpp(t, "vempty", valuesOf(row("iempty", "aa", 5, "LOAN", 100, 0, 0)), "seed")
		if got := classOf(t, "iempty", "aa"); got != "LOAN" {
			t.Fatalf("seed class = %q; want LOAN", got)
		}
		// A view that emits zero rows (typed by the VALUES, filtered by WHERE false) must not error and must
		// not delete the stored classification.
		empty := valuesOf(row("iempty", "aa", 5, "LOAN", 100, 0, 0)) + ` WHERE false`
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vempty AS `+empty); err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vempty'::regclass, 'empty')`).Scan(&n); err != nil {
			t.Fatalf("empty projection errored: %v", err)
		}
		if n != 0 {
			t.Errorf("empty projection changed %d rows; want 0", n)
		}
		if got := classOf(t, "iempty", "aa"); got != "LOAN" {
			t.Errorf("empty projection wiped classification to %q; want LOAN preserved", got)
		}
	})

	t.Run("return value counts rows inserted or changed", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vrc AS `+valuesOf(row("irc", "aa", 5, "LOAN", 100, 0, 0), row("irc", "aa", 6, "LOAN", 110, 0, 0))); err != nil {
			t.Fatal(err)
		}
		var n int64
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vrc'::regclass, 'rc')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("first run inserted %d; want 2", n)
		}
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vrc'::regclass, 'rc')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("identical rerun changed %d; want 0", n)
		}
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vrc AS `+valuesOf(row("irc", "aa", 5, "LOAN", 100, 0, 0), row("irc", "aa", 99, "LOAN", 110, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx, `SELECT materialize_position_projection('vrc'::regclass, 'rc')`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Errorf("one changed quantity reported %d; want 1", n)
		}
	})

	t.Run("two views in one transaction both materialize (temp reuse + no self-deadlock)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vt1 AS `+valuesOf(row("it1", "aa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vt2 AS `+valuesOf(row("it2", "aa", 6, "BORROW", 100, 0, 0))); err != nil {
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
		if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('vt1'::regclass, 'multi')`); err != nil {
			t.Fatalf("first view: %v", err)
		}
		if _, err := tx.Exec(ctx, `SELECT materialize_position_projection('vt2'::regclass, 'multi')`); err != nil {
			t.Fatalf("second view (temp reuse / lock): %v", err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if got := classOf(t, "it1", "aa"); got != "LOAN" {
			t.Errorf("view 1 class = %q; want LOAN", got)
		}
		if got := classOf(t, "it2", "aa"); got != "BORROW" {
			t.Errorf("view 2 class = %q; want BORROW", got)
		}
	})

	t.Run("concurrent same-view runs serialize on the advisory lock", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vlock AS `+valuesOf(row("ilk2", "aa", 5, "LOAN", 100, 0, 0))); err != nil {
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
		if _, err := txA.Exec(ctx, `SELECT materialize_position_projection('vlock'::regclass, 'A')`); err != nil {
			t.Fatalf("conn A: %v", err)
		}
		// conn B on the same view must block on the advisory lock; a transaction-scoped statement_timeout
		// interrupts the wait, proving B was excluded (a broken/absent lock would let B finish instantly).
		// SET LOCAL + the deferred rollback keep the timeout scoped to this tx, so it can't leak back to the
		// pool the way a session-level SET + ignored RESET could.
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
		if _, err := txB.Exec(ctx, `SET LOCAL statement_timeout = '2s'`); err != nil {
			t.Fatal(err)
		}
		_, errB := txB.Exec(ctx, `SELECT materialize_position_projection('vlock'::regclass, 'B')`)
		if errB == nil {
			t.Error("conn B completed while conn A held the lock; the per-view lock did not serialize")
		} else if !strings.Contains(errB.Error(), "statement timeout") {
			t.Errorf("conn B failed with %v; want a statement timeout (blocked on the advisory lock)", errB)
		}
	})

	// --- atomicity / NOT-NULL observation columns ----------------------------------------------

	t.Run("a within-write failure leaves existing rows untouched (atomicity)", func(t *testing.T) {
		// Seed a position, then run a view that updates it AND emits a second position with an unseeded code.
		// The FK failure during the classification write must roll back the WHOLE statement: the seeded
		// row's quantity and classification unchanged, and the poison position must have written nothing.
		mpp(t, "vat", valuesOf(row("iat", "aa", 5, "LOAN", 100, 0, 0)), "seed")
		poison := valuesOf(row("iat", "aa", 50, "LOAN", 100, 0, 0), row("iat2", "aa", 7, "NOPE_CODE", 100, 0, 0))
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vat AS `+poison); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `SELECT materialize_position_projection('vat'::regclass, 'poison')`); err == nil || !strings.Contains(err.Error(), "deal_type_fkey") {
			t.Fatalf("poison run: got %v; want the classification FK failure (proves the failure happened mid-write)", err)
		}
		var unchanged bool
		if err := pool.QueryRow(ctx, `SELECT quantity = 5 FROM position_state WHERE position_id = position_id(1,10,'iat','aa')`).Scan(&unchanged); err != nil {
			t.Fatal(err)
		}
		if !unchanged {
			t.Error("existing quantity changed after a rolled-back run; want 5 (no partial write)")
		}
		if got := classOf(t, "iat", "aa"); got != "LOAN" {
			t.Errorf("existing classification = %q after a rolled-back run; want LOAN", got)
		}
		var poisonRows int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM position_state WHERE position_id = position_id(1,10,'iat2','aa')`).Scan(&poisonRows); err != nil {
			t.Fatal(err)
		}
		if poisonRows != 0 {
			t.Errorf("poison position wrote %d rows; want 0 (all-or-nothing)", poisonRows)
		}
	})

	t.Run("null block_number rejected (NOT NULL observation column)", func(t *testing.T) {
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ibn'::text,'aa'::text,5::numeric,'LOAN'::text,NULL::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vbn", body, "bn", `column "block_number"`)
	})

	t.Run("null block_timestamp rejected (NOT NULL partition column)", func(t *testing.T) {
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'ibt'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,NULL::timestamptz)) ` + mppCols
		mppErr(t, "vbt", body, "bt", `column "block_timestamp"`)
	})

	// --- coordinate / structural-id CHECKs (round 5; each fails on the pre-fix code, which stored these) --

	t.Run("negative block_number rejected (coordinate CHECK)", func(t *testing.T) {
		// Pre-fix this was stored silently and — the guard orders on these columns — became the
		// position's "oldest" observation, skewing classification recency.
		mppErr(t, "vnbn", valuesOf(row("inbn", "aa", 5, "LOAN", -42, 0, 0)), "nbn", "coord_nonneg")
	})

	t.Run("negative block_version and processing_version rejected", func(t *testing.T) {
		mppErr(t, "vnbv", valuesOf(row("inbv", "aa", 5, "LOAN", 100, -1, 0)), "nbv", "coord_nonneg")
		mppErr(t, "vnpv", valuesOf(row("inpv", "aa", 5, "LOAN", 100, 0, -1)), "npv", "coord_nonneg")
	})

	t.Run("zero or negative chain_id / protocol_id rejected (NULL stays legal)", func(t *testing.T) {
		// Registry ids are strictly positive; 0 is an upstream zero-value default, and because the value
		// feeds the position_id hash a wrong-but-accepted id forks the position permanently.
		body := `SELECT * FROM (VALUES (0::int,10::bigint,'izc'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vzc", body, "zc", "chain_pos")
		body = `SELECT * FROM (VALUES (1::int,0::bigint,'izp'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'2026-01-01'::timestamptz)) ` + mppCols
		mppErr(t, "vzp", body, "zp", "protocol_pos")
	})

	t.Run("two positions at one block may carry different timestamps (event-time sources)", func(t *testing.T) {
		// block_timestamp is each source's observation time, invariant only per logical key (pre-flight
		// check 3), NOT a table-wide function of block_number: Sky prime_debt uses synced_at, so two
		// positions at the same block legitimately differ. Pins the Sky-enabling semantics so a future
		// "consistency" check doesn't silently break the #627 materializer.
		body := `SELECT * FROM (VALUES ` +
			`(1::int,10::bigint,'idv1'::text,'aa'::text,5::numeric,'LOAN'::text,777::bigint,0::int,0::int,'2026-01-01'::timestamptz),` +
			`(1::int,10::bigint,'idv2'::text,'aa'::text,6::numeric,'BORROW'::text,777::bigint,0::int,0::int,'2026-02-01'::timestamptz)) ` + mppCols
		mpp(t, "vdv", body, "eventtime")
		if got := classOf(t, "idv1", "aa"); got != "LOAN" {
			t.Errorf("position 1 class = %q; want LOAN", got)
		}
		if got := classOf(t, "idv2", "aa"); got != "BORROW" {
			t.Errorf("position 2 class = %q; want BORROW", got)
		}
	})

	t.Run("pre-blockchain block_timestamp rejected (epoch-corruption guard)", func(t *testing.T) {
		// A hex-parse bug writing epoch-zero would otherwise silently create a 1970 chunk on the
		// partition column and poison time-ordered reads.
		body := `SELECT * FROM (VALUES (1::int,10::bigint,'iep'::text,'aa'::text,5::numeric,'LOAN'::text,100::bigint,0::int,0::int,'1970-01-01+00'::timestamptz)) ` + mppCols
		mppErr(t, "vep", body, "ep", "ts_sane")
	})

	// --- narrowed grants (round 5): append-only must hold against history REWRITES, not just DELETE ---

	t.Run("narrowed grants: writer path intact, history rewrite denied (stl_readwrite)", func(t *testing.T) {
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW aclv AS `+valuesOf(row("iaclr", "aa", 5, "LOAN", 100, 0, 0))); err != nil {
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
		if err := asRole(`SELECT materialize_position_projection('aclv'::regclass, 'role-run')`); err != nil {
			t.Fatalf("materializer as stl_readwrite failed under narrowed grants: %v", err)
		}
		if got := classOf(t, "iaclr", "aa"); got != "LOAN" {
			t.Errorf("role-run classification = %q; want LOAN", got)
		}
		// Rewrites of identity/coordinate/history columns must be denied; quantity stays sanctioned.
		denied := [][2]string{
			{"position_state identity", `UPDATE position_state SET holder_id = 'bb' WHERE false`},
			{"position_state coordinates", `UPDATE position_state SET block_number = 1 WHERE false`},
			{"position_state delete", `DELETE FROM position_state WHERE false`},
			{"classification identity", `UPDATE position_classification SET position_id = '\x00'::bytea WHERE false`},
			{"classification collateral_status", `UPDATE position_classification SET collateral_status = 'PLEDGED' WHERE false`},
			{"classification delete", `DELETE FROM position_classification WHERE false`},
		}
		for _, d := range denied {
			if err := asRole(d[1]); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("%s: want permission denied, got %v", d[0], err)
			}
		}
		if err := asRole(`UPDATE position_state SET quantity = quantity WHERE false`); err != nil {
			t.Errorf("quantity update (the sanctioned column) denied: %v", err)
		}
		// Reclassify the same position as the role: forces the cls ON CONFLICT DO UPDATE arm, which
		// needs the column-scoped UPDATE grant on position_classification (deleting that grant line
		// must fail here, not weeks later on the first production reclassification).
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW aclv2 AS `+valuesOf(row("iaclr", "aa", 5, "COLLATERAL", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `GRANT SELECT ON aclv2 TO stl_readwrite`); err != nil {
			t.Fatal(err)
		}
		if err := asRole(`SELECT materialize_position_projection('aclv2'::regclass, 'role-reclassify')`); err != nil {
			t.Fatalf("reclassification (cls UPDATE arm) as stl_readwrite failed: %v", err)
		}
		if got := classOf(t, "iaclr", "aa"); got != "COLLATERAL" {
			t.Errorf("role reclassification = %q; want COLLATERAL", got)
		}
	})

	// --- ported input matrix (previously scratch-only) ---------------------------------------------

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
			{"whitespace-only instrument rejected", "1", "10", "'   '", "'aa'", "5", "'LOAN'::text", "100", "0", "instrument_key is required"},
			{"whitespace-only holder rejected", "1", "10", "'m3'", "'  '", "5", "'LOAN'::text", "100", "0", "holder_id is required"},
			{"uppercase instrument is legal", "1", "10", "'REGISTRY:ILK-A'", "'aa'", "5", "'LOAN'::text", "100", "0", ""},
			{"10k-char instrument is legal", "1", "10", "repeat('x',10000)", "'aa'", "5", "'LOAN'::text", "100", "0", ""},
			{"unicode instrument is legal", "1", "10", "'ilk-Ω-θ'", "'aa'", "5", "'LOAN'::text", "100", "0", ""},
			{"quantity 1e30 is legal", "1", "10", "'m4'", "'aa'", "1e30", "'LOAN'::text", "100", "0", ""},
			{"quantity 1e-18 dust is legal", "1", "10", "'m5'", "'aa'", "0.000000000000000001", "'LOAN'::text", "100", "0", ""},
			{"empty-string code hits the FK", "1", "10", "'m6'", "'aa'", "5", "''::text", "100", "0", "deal_type_fkey"},
			{"lowercase code hits the FK", "1", "10", "'m7'", "'aa'", "5", "'loan'::text", "100", "0", "deal_type_fkey"},
			{"chain_id int32 max is legal", "2147483647", "10", "'m8'", "'aa'", "5", "'LOAN'::text", "100", "0", ""},
			{"protocol_id int64 max is legal", "1", "9223372036854775807", "'m9'", "'aa'", "5", "'LOAN'::text", "100", "0", ""},
			{"block_number 2^62 is legal", "1", "10", "'m10'", "'aa'", "5", "'LOAN'::text", "4611686018427387904", "0", ""},
			{"block_version int32 max is legal", "1", "10", "'m11'", "'aa'", "5", "'LOAN'::text", "100", "2147483647", ""},
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

	// --- ACL completions and semantic pins ----------------------------------------------------------

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
		for _, tbl := range []string{"position_state", "position_classification"} {
			if err := asRoleOne("stl_readwrite", `TRUNCATE `+tbl); err == nil || !strings.Contains(err.Error(), "permission denied") {
				t.Errorf("TRUNCATE %s as stl_readwrite: want permission denied, got %v", tbl, err)
			}
		}
		if err := asRoleOne("stl_readonly", `SELECT count(*) FROM position_state`); err != nil {
			t.Errorf("SELECT as stl_readonly failed: %v", err)
		}
		if err := asRoleOne("stl_readonly", `INSERT INTO position_classification (position_id, deal_type_code) VALUES ('\x00'::bytea, 'LOAN')`); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Errorf("INSERT as stl_readonly: want permission denied, got %v", err)
		}
	})

	t.Run("1-byte position_id rejected by the width CHECK (direct insert)", func(t *testing.T) {
		// Unreachable through the function (position_id() always emits 32 bytes); pin the table CHECK.
		_, err := pool.Exec(ctx, `INSERT INTO position_state (position_id, instrument_key, holder_id, quantity, block_number, block_timestamp)
			VALUES ('\x00'::bytea, 'x', 'aa', 1, 100, '2026-01-01')`)
		if err == nil || !strings.Contains(err.Error(), "id_len") {
			t.Errorf("1-byte position_id: want id_len_chk violation, got %v", err)
		}
	})

	t.Run("classification stamps valid_from as UTC today; qty rewrite leaves created_at untouched", func(t *testing.T) {
		mpp(t, "vsp1", valuesOf(row("isp1", "aa", 5, "LOAN", 100, 0, 0)), "stamp")
		var utcToday bool
		if err := pool.QueryRow(ctx,
			`SELECT valid_from = (now() AT TIME ZONE 'utc')::date FROM position_classification WHERE position_id = position_id(1,10,'isp1','aa')`).Scan(&utcToday); err != nil {
			t.Fatal(err)
		}
		if !utcToday {
			t.Error("valid_from is not UTC today on first classification")
		}
		// The sanctioned same-key quantity rewrite (pv-less sources) leaves created_at untouched —
		// pinning the documented residual: the rewrite carries no audit trail.
		var before string
		if err := pool.QueryRow(ctx, `SELECT created_at::text FROM position_state WHERE position_id = position_id(1,10,'isp1','aa')`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		mpp(t, "vsp1", valuesOf(row("isp1", "aa", 9, "LOAN", 100, 0, 0)), "rewrite")
		var after string
		var q int
		if err := pool.QueryRow(ctx, `SELECT created_at::text, quantity::int FROM position_state WHERE position_id = position_id(1,10,'isp1','aa')`).Scan(&after, &q); err != nil {
			t.Fatal(err)
		}
		if q != 9 {
			t.Errorf("rewrite quantity = %d; want 9", q)
		}
		if before != after {
			t.Errorf("created_at changed on a quantity rewrite (%s -> %s); the residual is pinned as no-audit", before, after)
		}
	})

	t.Run("temp snapshot is not left behind after a run", func(t *testing.T) {
		conn, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		if _, err := conn.Exec(ctx, `CREATE OR REPLACE VIEW vtl AS `+valuesOf(row("itl", "aa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Exec(ctx, `SELECT materialize_position_projection('vtl'::regclass, 'tmp')`); err != nil {
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
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vcna AS `+valuesOf(row("icna", "aa", 5, "LOAN", 100, 0, 0))); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW vcnb AS `+valuesOf(row("icnb", "aa", 5, "BORROW", 100, 0, 0))); err != nil {
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
		if _, err := txA.Exec(ctx, `SELECT materialize_position_projection('vcna'::regclass, 'A')`); err != nil {
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
		if _, err := txB.Exec(ctx, `SELECT materialize_position_projection('vcnb'::regclass, 'B')`); err != nil {
			t.Fatalf("a different view on a disjoint position blocked behind the first run: %v", err)
		}
		if err := txB.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if got := classOf(t, "icnb", "aa"); got != "BORROW" {
			t.Errorf("concurrent B classification = %q; want BORROW", got)
		}
	})

	// --- model-based fuzz --------------------------------------------------------------------------

	t.Run("model fuzz: seeded operation sequences vs an independent oracle", func(t *testing.T) {
		// Every other subtest is a hand-picked scenario; this drives seeded-random operation
		// sequences and checks four invariants after every step: idempotent rerun, exact
		// stored-state conservation, no minted codes, and a full mirror of the guard semantics
		// (merged-canonical, canonicality join, hwm). Two profiles: "core" (single-position
		// batches, the op mix that finds the round-7 bug class automatically) and "batch-shapes"
		// (NULL-code zero closes and multi-position views — shapes the core profile never emits).
		// Seeds are fixed so CI is deterministic.
		type okey struct{ bn, bv, pv int }
		type oobs struct {
			qty  int
			code string // "" means NULL
		}
		type opos struct {
			ik       string
			stored   map[okey]oobs
			expected string // "" means unclassified
			seen     map[string]bool
		}
		type canon struct {
			bv, pv, qty int
			code        string
		}
		codes := []string{"LOAN", "BORROW", "COLLATERAL"}
		sortedKeys := func(m map[okey]oobs) []okey {
			ks := make([]okey, 0, len(m))
			for k := range m {
				ks = append(ks, k)
			}
			sort.Slice(ks, func(a, b int) bool {
				if ks[a].bn != ks[b].bn {
					return ks[a].bn < ks[b].bn
				}
				if ks[a].bv != ks[b].bv {
					return ks[a].bv < ks[b].bv
				}
				return ks[a].pv < ks[b].pv
			})
			return ks
		}
		tupleSQL := func(ik string, k okey, o oobs) string {
			c := "NULL::text"
			if o.code != "" {
				c = "'" + o.code + "'::text"
			}
			return fmt.Sprintf("(1::int,10::bigint,'%s'::text,'aa'::text,%d::numeric,%s,%d::bigint,%d::int,%d::int,timestamptz '2026-06-01 12:00+00' + %d*interval '1 day')",
				ik, o.qty, c, k.bn, k.bv, k.pv, k.bn%40)
		}
		canonical := func(p *opos, extra map[okey]oobs) map[int]canon {
			type cand struct {
				bv, pv, src, qty int
				code             string
			}
			best := map[int]cand{}
			consider := func(k okey, o oobs, src int) {
				c, seen := best[k.bn]
				n := cand{k.bv, k.pv, src, o.qty, o.code}
				if !seen || n.bv > c.bv || (n.bv == c.bv && (n.pv > c.pv || (n.pv == c.pv && n.src > c.src))) {
					best[k.bn] = n
				}
			}
			for k, o := range p.stored {
				consider(k, o, 0)
			}
			for k, o := range extra {
				consider(k, o, 1)
			}
			out := map[int]canon{}
			for bn, c := range best {
				out[bn] = canon{c.bv, c.pv, c.qty, c.code}
			}
			return out
		}
		oracleApply := func(p *opos, batch map[okey]oobs) {
			merged := canonical(p, batch)
			runCanon := map[int]canon{}
			for k, o := range batch {
				c, seen := runCanon[k.bn]
				if !seen || k.bv > c.bv || (k.bv == c.bv && k.pv > c.pv) {
					runCanon[k.bn] = canon{k.bv, k.pv, o.qty, o.code}
				}
			}
			latestBn := -1
			for bn, c := range runCanon {
				if c.qty > 0 && bn > latestBn {
					latestBn = bn
				}
			}
			for k, o := range batch {
				p.stored[k] = o
				if o.qty > 0 && o.code != "" {
					p.seen[o.code] = true
				}
			}
			if latestBn < 0 {
				return
			}
			l := runCanon[latestBn]
			mc, okmc := merged[latestBn]
			if !okmc || mc.bv != l.bv || mc.pv != l.pv {
				return // canonicality join blocks it
			}
			hbn := -1
			var h canon
			for bn, c := range merged {
				if c.qty > 0 && bn > hbn {
					hbn, h = bn, c
				}
			}
			if hbn >= 0 {
				if latestBn < hbn || (latestBn == hbn && (l.bv < h.bv || (l.bv == h.bv && l.pv < h.pv))) {
					return // recency guard blocks it
				}
			}
			p.expected = l.code
		}
		dbRows := func(p *opos) map[okey]int {
			out := map[okey]int{}
			rs, err := pool.Query(ctx,
				`SELECT block_number, block_version, processing_version, quantity::int
				   FROM position_state WHERE position_id = position_id(1, 10, $1, 'aa')`, p.ik)
			if err != nil {
				t.Fatal(err)
			}
			defer rs.Close()
			for rs.Next() {
				var bn, bv, pv, q int
				if err := rs.Scan(&bn, &bv, &pv, &q); err != nil {
					t.Fatal(err)
				}
				out[okey{bn, bv, pv}] = q
			}
			return out
		}
		checkPos := func(step int, p *opos) {
			got := dbRows(p)
			if len(got) != len(p.stored) {
				t.Fatalf("step %d %s: db has %d rows, oracle %d", step, p.ik, len(got), len(p.stored))
			}
			for k, o := range p.stored {
				if got[k] != o.qty {
					t.Fatalf("step %d %s: row %+v qty db=%d oracle=%d", step, p.ik, k, got[k], o.qty)
				}
			}
			cls := classOf(t, p.ik, "aa")
			if cls != p.expected {
				t.Fatalf("step %d %s: classification db=%q oracle=%q", step, p.ik, cls, p.expected)
			}
			if cls != "" && !p.seen[cls] {
				t.Fatalf("step %d %s: minted code %q never emitted on a non-zero row", step, p.ik, cls)
			}
		}
		buildOp := func(rng *rand.Rand, p *opos, nullCloses bool) map[okey]oobs {
			batch := map[okey]oobs{}
			keys := sortedKeys(p.stored)
			var nz []okey
			for _, k := range keys {
				if p.stored[k].qty > 0 {
					nz = append(nz, k)
				}
			}
			op := rng.Intn(7)
			switch {
			case op <= 1 || len(keys) == 0: // new block
				maxBn := 99
				for _, k := range keys {
					if k.bn > maxBn {
						maxBn = k.bn
					}
				}
				batch[okey{maxBn + 1 + rng.Intn(3), 0, 0}] = oobs{1 + rng.Intn(9), codes[rng.Intn(3)]}
			case op == 2: // reorg-zero the top block; batch-shapes profile closes with a NULL code
				top := keys[len(keys)-1]
				code := codes[rng.Intn(3)]
				if nullCloses && rng.Intn(2) == 0 {
					code = ""
				}
				batch[okey{top.bn, top.bv + 1, 0}] = oobs{0, code}
			case op == 3: // pv correction of a random stored key
				k := keys[rng.Intn(len(keys))]
				batch[okey{k.bn, k.bv, k.pv + 1}] = oobs{1 + rng.Intn(9), codes[rng.Intn(3)]}
			case op == 4 && len(nz) > 0: // stale partial re-emission, code changed (the round-7 shape)
				k := nz[rng.Intn(len(nz))]
				batch[k] = oobs{p.stored[k].qty, codes[rng.Intn(3)]}
			case op == 5 && len(nz) > 0: // quantity rewrite at the same key (the sanctioned D1 channel)
				k := nz[rng.Intn(len(nz))]
				batch[k] = oobs{1 + rng.Intn(9), p.stored[k].code}
			default: // full re-projection of everything stored
				for k, o := range p.stored {
					batch[k] = o
				}
			}
			return batch
		}
		profiles := []struct {
			name       string
			seed       int64
			steps      int
			multi      bool
			nullCloses bool
		}{
			{"core", 20260819, 200, false, false},
			{"batch-shapes", 424242, 140, true, true},
		}
		for pi, prof := range profiles {
			rng := rand.New(rand.NewSource(prof.seed))
			positions := make([]*opos, 3)
			for i := range positions {
				positions[i] = &opos{ik: fmt.Sprintf("fzp%d_%d", pi, i), stored: map[okey]oobs{}, seen: map[string]bool{}}
			}
			view := fmt.Sprintf("vfzp%d", pi)
			for step := 1; step <= prof.steps; step++ {
				primary := positions[rng.Intn(len(positions))]
				batches := map[*opos]map[okey]oobs{primary: buildOp(rng, primary, prof.nullCloses)}
				if prof.multi && rng.Intn(3) == 0 { // one view emitting rows for a second position too
					other := positions[rng.Intn(len(positions))]
					if other != primary {
						batches[other] = buildOp(rng, other, prof.nullCloses)
					}
				}
				var rows []string
				for _, p := range positions { // deterministic order across the map
					if b, okb := batches[p]; okb {
						for _, k := range sortedKeys(b) {
							rows = append(rows, tupleSQL(p.ik, k, b[k]))
						}
					}
				}
				if len(rows) == 0 {
					continue
				}
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+view+` AS `+valuesOf(rows...)); err != nil {
					t.Fatalf("fuzz view %s step %d: %v", view, step, err)
				}
				var n1, n2 int64
				if err := pool.QueryRow(ctx, `SELECT materialize_position_projection($1::regclass, 'fuzz')`, view).Scan(&n1); err != nil {
					t.Fatalf("fuzz run %s step %d: %v", view, step, err)
				}
				if err := pool.QueryRow(ctx, `SELECT materialize_position_projection($1::regclass, 'fuzz-rerun')`, view).Scan(&n2); err != nil {
					t.Fatalf("fuzz rerun %s step %d: %v", view, step, err)
				}
				if n2 != 0 {
					t.Fatalf("step %d: identical rerun changed %d rows; want 0", step, n2)
				}
				for p, b := range batches {
					oracleApply(p, b)
				}
				for p := range batches {
					checkPos(step, p)
				}
			}
		}
	})
}
