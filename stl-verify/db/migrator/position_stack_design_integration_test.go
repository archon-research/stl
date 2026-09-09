//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"
)

// Design invariants of the position spine, checked on randomised histories: several block versions at
// one block, corrections, deal-type flips, zero quantities, off-chain holders, and 2-5 arrival batches
// in random order so a later batch carries an older observation. The cache invariants (position_current,
// position_daily) live with the migrations that create those tables. Every violation is a test failure.
func TestPositionStackDesignInvariants(t *testing.T) {
	const seeds = 24
	for seed := 1; seed <= seeds; seed++ {
		t.Run(fmt.Sprintf("seed-%02d", seed), func(t *testing.T) {
			ctx := context.Background()
			pool, cleanup := setupMigratedPostgres(ctx, t)
			defer cleanup()

			rng := rand.New(rand.NewSource(int64(seed) * 7919))
			rows := generateHistory(rng)
			view := fmt.Sprintf("pv_design_%d", seed)

			// The whole history and its closure as views, for the oracle comparisons in I0 and I5b.
			if _, err := pool.Exec(ctx, `CREATE VIEW `+view+`_all AS `+valuesBody(rows)); err != nil {
				t.Fatalf("create history view: %v", err)
			}
			if _, err := pool.Exec(ctx, `CREATE VIEW `+view+`_want AS `+valuesBody(closure(rows))); err != nil {
				t.Fatalf("create closure view: %v", err)
			}

			// Each run re-projects everything that has arrived so far, as the runner does; arrival order is
			// random, so a later run carries observations older than ones already stored.
			var arrived []obsRow
			for bi, batch := range splitBatches(rng, rows) {
				arrived = append(arrived, batch...)
				if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+view+` AS `+valuesBody(arrived)); err != nil {
					t.Fatalf("create view (batch %d): %v", bi, err)
				}
				if _, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass)`, view); err != nil {
					t.Fatalf("I0: the materializer refused a legal history at batch %d: %v", bi, err)
				}
			}
			// I0: the spine holds every observation the closure of the FULL history keeps, nothing that is not
			// in the history, and no position starts with a zero. Batches arrive out of order, so a zero that
			// was a close when it arrived may be a repeated zero in the final history; those may remain.
			want := closure(rows)
			var missing, foreign, leadingZero int
			if err := pool.QueryRow(ctx, `
				WITH hist AS (SELECT * FROM `+view+`_all),
				     want AS (SELECT * FROM `+view+`_want),
				     spine AS (SELECT holder_id, chain_id, instrument_key, block_number, block_version, processing_version, quantity FROM position_state)
				SELECT (SELECT count(*) FROM (SELECT holder_id, chain_id, instrument_key, block_number, block_version, processing_version, quantity FROM want
				                              EXCEPT SELECT * FROM spine) x),
				       (SELECT count(*) FROM (SELECT * FROM spine
				                              EXCEPT SELECT holder_id, chain_id, instrument_key, block_number, block_version, processing_version, quantity FROM hist) y),
				       (SELECT count(*) FROM (SELECT DISTINCT ON (position_id) quantity FROM position_state
				                              ORDER BY position_id, block_number, block_version, processing_version) z WHERE quantity = 0)`).
				Scan(&missing, &foreign, &leadingZero); err != nil {
				t.Fatalf("I0: %v", err)
			}
			if missing != 0 || foreign != 0 || leadingZero != 0 {
				t.Errorf("I0 closure: %d closure rows missing from the spine, %d spine rows not in the history, %d positions opening with a zero (history %d rows, closure %d)",
					missing, foreign, leadingZero, len(rows), len(want))
			}

			// I5: deal_type reaches the spine for exactly the observations that emitted one.
			var nullInSpine, total int
			if err := pool.QueryRow(ctx,
				`SELECT count(*) FILTER (WHERE deal_type IS NULL), count(*) FROM position_state`).Scan(&nullInSpine, &total); err != nil {
				t.Fatalf("I5: %v", err)
			}
			wantNull := 0
			for _, r := range want {
				if r.dealType == "" {
					wantNull++
				}
			}
			if nullInSpine < wantNull || total < len(want) {
				t.Errorf("I5 deal_type round-trips: %d NULL stored of %d rows, want at least %d NULL of %d", nullInSpine, total, wantNull, len(want))
			}

			// I5b: every stored deal_type is the one its observation emitted (not just the NULL count).
			var mismatched int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM position_state p JOIN `+view+`_all v
				  ON v.holder_id = p.holder_id AND v.block_number = p.block_number
				 AND v.block_version = p.block_version AND v.processing_version = p.processing_version
				 AND (v.chain_id IS NOT DISTINCT FROM p.chain_id)
				 WHERE v.deal_type IS DISTINCT FROM p.deal_type`).Scan(&mismatched); err != nil {
				t.Fatalf("I5b: %v", err)
			}
			if mismatched != 0 {
				t.Errorf("I5b %d stored observations carry a deal_type other than the one emitted", mismatched)
			}

			// I7: nothing anywhere still carries the pre-rename column name.
			var stale int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid
				 JOIN pg_namespace n ON n.oid = c.relnamespace
				 WHERE n.nspname = 'public' AND c.relkind IN ('r','v','m')
				   AND a.attname = 'deal_type_code' AND a.attnum > 0 AND NOT a.attisdropped`).Scan(&stale); err != nil {
				t.Fatalf("I7: %v", err)
			}
			if stale != 0 {
				t.Errorf("I7 %d relation columns still use deal_type_code", stale)
			}

			// I9: a higher block with an earlier instant is refused for THAT position and recorded, while
			// the run continues and every other position lands. Aborting instead refused the whole
			// projection on every later run, with no repair path on an append-only spine.
			bad := fmt.Sprintf("pv_design_bad_%d", seed)
			nonmono := `SELECT * FROM (VALUES (1::int, 10::bigint, 'design-inst'::text, '%s'::text, 5::numeric, ` +
				`%d::bigint, 0::int, 0::int, '%s'::timestamptz, 'LOAN'::text)) ` +
				`v(chain_id,protocol_id,instrument_key,holder_id,quantity,block_number,block_version,processing_version,block_timestamp,deal_type)`
			h := fmt.Sprintf("%040x", 99)
			peer := fmt.Sprintf("%040x", 98)
			if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+bad+` AS `+fmt.Sprintf(nonmono, h, 900, "2026-04-20T00:00:00Z")); err != nil {
				t.Fatalf("I9 view: %v", err)
			}
			if _, err := pool.Exec(ctx, `SELECT materialize_position_projection($1::regclass)`, bad); err != nil {
				t.Errorf("I9 a monotonic first row must be accepted: %v", err)
			}
			// The inverted row for h, and a healthy peer in the same batch.
			if _, err := pool.Exec(ctx, `CREATE OR REPLACE VIEW `+bad+` AS `+fmt.Sprintf(nonmono, h, 950, "2026-04-10T00:00:00Z")+
				` UNION ALL `+fmt.Sprintf(nonmono, peer, 960, "2026-04-21T00:00:00Z")); err != nil {
				t.Fatalf("I9 view: %v", err)
			}
			for run := 1; run <= 2; run++ {
				var n int64
				if err := pool.QueryRow(ctx, `SELECT materialize_position_projection($1::regclass)`, bad).Scan(&n); err != nil {
					t.Errorf("I9 run %d must continue past one position's inverted pair, got %v", run, err)
				}
				if run == 1 && n != 1 {
					t.Errorf("I9 run 1 appended %d rows; want exactly the healthy peer", n)
				}
				if run == 2 && n != 0 {
					t.Errorf("I9 run 2 appended %d rows; want 0", n)
				}
			}
			var hRows, peerRows, refusals, runsRefusing int
			if err := pool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM position_state WHERE holder_id = $1),
				(SELECT count(*) FROM position_state WHERE holder_id = $2),
				(SELECT count(*) FROM position_projection_refusal WHERE reason = 'block_time_inverts_height' AND detail LIKE '%holder=' || $1 || '%'),
				(SELECT count(*) FROM position_projection_run WHERE projection = 'public.' || $3 AND positions_refused = 1)`,
				h, peer, bad).Scan(&hRows, &peerRows, &refusals, &runsRefusing); err != nil {
				t.Fatalf("I9: %v", err)
			}
			if hRows != 1 || peerRows != 1 {
				t.Errorf("I9 stored rows: offender=%d peer=%d; want the offender frozen at its first row and the peer landed", hRows, peerRows)
			}
			if refusals != 1 {
				t.Errorf("I9 %d refusal rows for the offender across two runs; want exactly 1 (keyed on the observation, not the run)", refusals)
			}
			if runsRefusing != 2 {
				t.Errorf("I9 %d runs recorded positions_refused=1; want both", runsRefusing)
			}

			// I8: one position_id may span more than one deal type over time. A property, not a defect:
			// deal type is not in the position_id hash, so a holder flipping net supplier/borrower keeps
			// one identity, which is why deal type is an attribute of the observation.
			var flipped int
			if err := pool.QueryRow(ctx, `
				SELECT count(*) FROM (SELECT position_id FROM position_state WHERE deal_type IS NOT NULL
				  GROUP BY position_id HAVING count(DISTINCT deal_type) > 1) z`).Scan(&flipped); err != nil {
				t.Fatalf("I8: %v", err)
			}
			if flipped > 0 {
				t.Logf("I8: %d position_ids span more than one deal type", flipped)
			}
		})
	}
}

type obsRow struct {
	offChain          bool
	holder            string
	qty               int
	block, bver, pver int
	ts                time.Time
	dealType          string // "" means emit NULL
}

// generateHistory produces a legal but adversarial history: several block_versions at one block,
// corrections, deal-type flips, zero quantities, dates spanning position_daily's 7-day chunk boundary,
// and block_timestamp monotonic in block_number per position -- which the materializer now enforces.
func generateHistory(rng *rand.Rand) []obsRow {
	base := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	var rows []obsRow
	seen := map[string]bool{}
	for h := 0; h < 1+rng.Intn(3); h++ {
		holder := fmt.Sprintf("%040x", h+10)
		// Roughly one holder in three is OFF-CHAIN: chain_id NULL, one snapshot a day, and block_number is
		// the instant in epoch seconds, as the materializer requires.
		offChain := rng.Intn(3) == 0
		for i := 0; i < 2+rng.Intn(6); i++ {
			block := 100 + rng.Intn(400)
			r := obsRow{
				holder: holder,
				qty:    []int{0, 1, 30, 150, 999}[rng.Intn(5)],
				block:  block,
				bver:   rng.Intn(2),
				pver:   rng.Intn(2),
				// Monotonic in block_number, as on-chain time is: one hour per block plus sub-hour
				// jitter that cannot reorder distinct blocks. The non-monotonic case is now an
				// input the materializer refuses, tested separately below.
				ts: base.Add(time.Duration(block) * time.Hour).
					Add(time.Duration(rng.Intn(60)) * time.Minute),
			}
			if offChain {
				r.ts = base.Add(time.Duration(i) * 24 * time.Hour).Add(time.Duration(rng.Intn(60)) * time.Minute)
				r.block = int(r.ts.Unix())
				r.bver, r.pver = 0, 0
				r.dealType = "CUSTODY"
				r.offChain = true
			}
			switch rng.Intn(4) {
			case 0:
				r.dealType = ""
			case 1, 2:
				r.dealType = "LOAN"
			default:
				r.dealType = "BORROW"
			}
			k := fmt.Sprintf("%v|%s|%d|%d|%d|%s", r.offChain, holder, r.block, r.bver, r.pver, r.ts.Format(time.RFC3339))
			if seen[k] {
				continue // the spine PK forbids a duplicate coordinate
			}
			seen[k] = true
			rows = append(rows, r)
		}
	}
	return rows
}

// closure is the oracle for the materializer's closure rule, written over the sorted history rather
// than as a window: keep a positive row, a zero directly after a positive, and a zero at the same block
// as its predecessor once the position has ever been positive.
func closure(rows []obsRow) []obsRow {
	byPos := map[string][]obsRow{}
	var order []string
	for _, r := range rows {
		k := fmt.Sprintf("%v|%s", r.offChain, r.holder) // one on-chain and one off-chain instrument per holder
		if _, ok := byPos[k]; !ok {
			order = append(order, k)
		}
		byPos[k] = append(byPos[k], r)
	}
	var out []obsRow
	for _, k := range order {
		seq := byPos[k]
		sort.Slice(seq, func(i, j int) bool {
			a, b := seq[i], seq[j]
			if a.block != b.block {
				return a.block < b.block
			}
			if a.bver != b.bver {
				return a.bver < b.bver
			}
			return a.pver < b.pver
		})
		opened := false
		for i, r := range seq {
			keep := r.qty > 0
			if i > 0 {
				prev := seq[i-1]
				keep = keep || prev.qty > 0 || (opened && prev.block == r.block)
			}
			if keep {
				out = append(out, r)
			}
			if r.qty > 0 {
				opened = true
			}
		}
	}
	return out
}

// splitBatches cuts the history into 2-5 arrival batches in random order, so a later run can carry
// an OLDER observation than one already stored.
func splitBatches(rng *rand.Rand, rows []obsRow) [][]obsRow {
	s := make([]obsRow, len(rows))
	copy(s, rows)
	rng.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
	n := 2 + rng.Intn(4)
	if n > len(s) {
		n = len(s)
	}
	buckets := make([][]obsRow, n)
	for i, r := range s {
		buckets[i%n] = append(buckets[i%n], r)
	}
	var kept [][]obsRow
	for _, b := range buckets {
		if len(b) > 0 {
			kept = append(kept, b)
		}
	}
	return kept
}

func valuesBody(rows []obsRow) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		dt := "NULL::text"
		if r.dealType != "" {
			dt = "'" + r.dealType + "'::text"
		}
		chain, proto, inst := "1::int", "10::bigint", "design-inst"
		if r.offChain {
			chain, proto, inst = "NULL::int", "NULL::bigint", "design-custody"
		}
		parts = append(parts, fmt.Sprintf(
			"(%s, %s, '%s'::text, '%s'::text, %d::numeric, %d::bigint, %d::int, %d::int, '%s'::timestamptz, %s)",
			chain, proto, inst, r.holder, r.qty, r.block, r.bver, r.pver, r.ts.Format(time.RFC3339), dt))
	}
	return `SELECT * FROM (VALUES ` + strings.Join(parts, ",") +
		`) v(chain_id,protocol_id,instrument_key,holder_id,quantity,block_number,block_version,` +
		`processing_version,block_timestamp,deal_type)`
}
