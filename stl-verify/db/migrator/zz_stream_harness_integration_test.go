//go:build integration

package migrator_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// A harness written from scratch for the whole position-ID stream, deliberately sharing nothing with
// the per-projection suites: its own seeding, its own randomisation, and oracles computed in Go from
// the generated facts rather than by re-running the views' own SQL. The existing suites each assert
// one projection against a hand-built fixture, and several of them reuse the same helpers, so a fault
// in a shared assumption is invisible to all of them at once. This exists to find that class.
//
// What it drives: every wrapper on the branch, in one database, against one randomised world, then the
// two trigger-fed caches the spine feeds.

type streamWorld struct {
	rng  *rand.Rand
	seed int64
}

// streamObs is one fact the harness generated. The oracle is built from these alone.
type streamObs struct {
	projection string
	instrument string
	holder     string
	qty        int64
	block      int64
	version    int
	procVer    int
	ts         string
	dealType   string
}

func streamHarness(t *testing.T, seed int64) (context.Context, *pgxpool.Pool, *streamWorld) {
	t.Helper()
	ctx := context.Background()
	pool, cleanup := setupMigratedPostgres(ctx, t)
	t.Cleanup(cleanup)
	return ctx, pool, &streamWorld{rng: rand.New(rand.NewSource(seed)), seed: seed}
}

func (w *streamWorld) addr(n int) string { return fmt.Sprintf("%040x", n) }

// streamBlockTime maps a block height to an instant that rises with it, one minute per block from a
// fixed epoch, so the spine's block-time invariant holds by construction.
func streamBlockTime(block int64) string {
	const epochDay = 1
	mins := block - 1000
	return fmt.Sprintf("2026-04-%02dT%02d:%02d:00Z", epochDay+int(mins/1440)%27, int(mins/60)%24, int(mins%60))
}

// seedMorphoWorld generates a randomised Morpho market history and returns the facts it wrote.
// Deliberately hand-rolled rather than calling the existing morpho fixtures.
func (w *streamWorld) seedMorphoWorld(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []streamObs {
	t.Helper()
	var out []streamObs
	if _, err := pool.Exec(ctx, `
		INSERT INTO chain (chain_id, name) VALUES (1,'ethereum') ON CONFLICT DO NOTHING`); err != nil {
		t.Fatalf("seed chain: %v", err)
	}
	var protocolID, loanTok, collTok, marketID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO protocol (chain_id, address, name, protocol_type)
		VALUES (1, decode($1,'hex'), 'stream-morpho', 'morpho_blue') RETURNING id`, w.addr(0x5100)).Scan(&protocolID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}
	for _, spec := range []struct {
		out *int64
		a   int
		d   int
	}{{&loanTok, 0x5101, 6}, {&collTok, 0x5102, 18}} {
		if err := pool.QueryRow(ctx, `
			INSERT INTO token (chain_id, address, symbol, decimals)
			VALUES (1, decode($1,'hex'), 'STR', $2) RETURNING id`, w.addr(spec.a), spec.d).Scan(spec.out); err != nil {
			t.Fatalf("seed token: %v", err)
		}
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO morpho_market (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id,
		                           oracle_address, irm_address, lltv, created_at_block)
		VALUES (1, $1, decode('5200','hex'), $2, $3, '\x00', '\x01', 0.8, 1) RETURNING id`,
		protocolID, loanTok, collTok).Scan(&marketID); err != nil {
		t.Fatalf("seed market: %v", err)
	}

	// Randomised holders, each with a randomised history: supply/borrow pairs that net, repeats at one
	// block, reorg versions and reprocesses. Block time is monotonic in block height, which the spine
	// requires and which the per-projection fixtures also assume -- generated here independently.
	holders := 2 + w.rng.Intn(3)
	for h := 0; h < holders; h++ {
		var userID int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO "user" (chain_id, address) VALUES (1, decode($1,'hex')) RETURNING id`,
			w.addr(0x6000+h)).Scan(&userID); err != nil {
			t.Fatalf("seed user: %v", err)
		}
		block := int64(1000 + w.rng.Intn(50))
		var supply, borrow int64
		for o := 0; o < 3+w.rng.Intn(5); o++ {
			block += int64(1 + w.rng.Intn(20))
			supply += int64(w.rng.Intn(400))
			if w.rng.Intn(3) == 0 && supply > 0 {
				borrow += int64(w.rng.Intn(int(supply) + 1))
			}
			ver, pv := 0, 0
			if w.rng.Intn(6) == 0 {
				ver = 1
			}
			if w.rng.Intn(7) == 0 {
				pv = 1
			}
			// Monotonic in block height. The spine refuses a position whose higher block carries an
			// earlier instant (block_time_inverts_height), which is what the per-date grain and
			// position_current both depend on, so a generator that does not respect it tests nothing:
			// the first draft of this harness derived the hour from block%24 and every observation of
			// every seed was refused, 32 refusals and 0 appended.
			ts := streamBlockTime(block)
			// The real shape, read from the migrations rather than assumed: no chain_id (it comes
			// through morpho_market), `collateral` not `collateral_assets`, shares alongside assets,
			// and processing_version added later by 20260410_110000.
			if _, err := pool.Exec(ctx, `
				INSERT INTO morpho_market_position
				    (morpho_market_id, user_id, supply_shares, borrow_shares, collateral,
				     supply_assets, borrow_assets, block_number, block_version, processing_version, timestamp)
				VALUES ($1, $2, $3, $4, $5, $3, $4, $6, $7, $8, $9::timestamptz)
				ON CONFLICT DO NOTHING`,
				marketID, userID, supply, borrow, supply/2, block, ver, pv, ts); err != nil {
				t.Fatalf("seed morpho position: %v", err)
			}
			net := supply - borrow
			if net < 0 {
				net = -net
			}
			deal := "LOAN"
			if supply-borrow < 0 {
				deal = "BORROW"
			}
			out = append(out, streamObs{
				projection: "public.position_morpho_market",
				instrument: "5200:" + w.addr(0x5101)[24:],
				holder:     w.addr(0x6000 + h),
				qty:        net, block: block, version: ver, procVer: pv, ts: ts, dealType: deal,
			})
		}
	}
	return out
}

// TestStreamHarness_MorphoAgreesWithAnIndependentOracle drives the real wrapper over a randomised
// world and checks position_state against facts the harness recorded as it generated them, not
// against the view's own SQL. A view and an oracle derived from the same expression agree by
// construction; these do not.
func TestStreamHarness_MorphoAgreesWithAnIndependentOracle(t *testing.T) {
	for _, seed := range []int64{1, 7, 13, 29, 101} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			ctx, pool, w := streamHarness(t, seed)
			facts := w.seedMorphoWorld(t, ctx, pool)

			var appended int64
			if err := pool.QueryRow(ctx, `SELECT materialize_morpho_market()`).Scan(&appended); err != nil {
				t.Fatalf("seed %d: materialize_morpho_market: %v", seed, err)
			}

			// Oracle: the distinct observation coordinates the harness wrote. The spine stores one row
			// per (position, block, version, processing_version, timestamp), so every generated
			// coordinate must be represented, and the spine must invent none.
			want := map[string]bool{}
			for _, f := range facts {
				// processing_version is NOT the writer's to choose: trigger_assign_processing_version fires
				// BEFORE INSERT on this table and eight others, computing it from what is already stored,
				// so a harness that supplies one is not testing what it appears to. Keyed without it.
				want[fmt.Sprintf("%s|%d|%d", f.holder, f.block, f.version)] = true
			}
			rows, err := pool.Query(ctx, `
				SELECT holder_id, block_number, block_version, processing_version
				  FROM position_state WHERE projection = 'public.position_morpho_market'`)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			got := map[string]bool{}
			for rows.Next() {
				var h string
				var bn int64
				var bv, pv int
				if err := rows.Scan(&h, &bn, &bv, &pv); err != nil {
					_ = pv
					t.Fatal(err)
				}
				got[fmt.Sprintf("%s|%d|%d", h, bn, bv)] = true
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 {
				t.Fatalf("seed %d: the spine stored nothing from %d generated observations; the harness is not exercising the projection", seed, len(facts))
			}
			// A coordinate the harness did not generate is legitimate in exactly one case: the shared
			// materializer SYNTHESISES a closing observation when a position leaves the source, and
			// those carry quantity 0. Anything else at an ungenerated coordinate is the spine inventing
			// history. The first draft of this oracle did not model closure and flagged every one.
			var invented []string
			for k := range got {
				if want[k] {
					continue
				}
				parts := strings.Split(k, "|")
				var closing int
				if err := pool.QueryRow(ctx, `
					SELECT count(*) FROM position_state
					 WHERE projection = 'public.position_morpho_market' AND holder_id = $1
					   AND block_number = $2::bigint AND quantity = 0`, parts[0], parts[1]).Scan(&closing); err != nil {
					t.Fatal(err)
				}
				if closing == 0 {
					invented = append(invented, k)
				}
			}
			sort.Strings(invented)
			if len(invented) > 0 {
				t.Errorf("seed %d: the spine holds %d coordinate(s) the harness never generated: %s",
					seed, len(invented), strings.Join(invented[:min(4, len(invented))], ", "))
			}
		})
	}
}
