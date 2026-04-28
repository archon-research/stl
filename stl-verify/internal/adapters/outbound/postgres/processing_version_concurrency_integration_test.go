//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Tests the per-table assign_processing_version_* triggers under cross-build
// contention: two transactions inserting the same natural key with different
// build_ids must each receive a distinct processing_version (0 and 1) and
// both rows must survive.
//
// Without the pg_advisory_xact_lock that lives at the top of each trigger
// function, this race silently drops the loser via ON CONFLICT DO NOTHING.
// Regression test for VEC-194 (ADR-0002 §3 gap).

const concurrencySchemaName = "test_pv_concurrency"

// concurrencyPool is opened per-test (not at package setup) so it doesn't
// hold idle connections that would starve the rest of the integration suite —
// the package's shared Postgres container is configured with the
// out-of-the-box max_connections, and 11 schema pools each at the pgxpool
// default of runtime.NumCPU() saturates it.
var concurrencyPool *pgxpool.Pool

func init() {
	registerTestFileSetup(concurrencySchemaName, func() {
		// SetupSchemaForMain creates the schema and runs migrations. We
		// throw away the pool it returns — we'll mint a small short-lived
		// pool inside each test. Schema and migrations persist across pools.
		testutil.SetupSchemaForMain(sharedDSN, concurrencySchemaName).Close()
	}, func() {
		// Reopen a tiny pool just for the cleanup so CleanupSchemaForMain
		// has something to close.
		concurrencyPool = openConcurrencyPool()
		testutil.CleanupSchemaForMain(sharedDSN, concurrencyPool, concurrencySchemaName)
	})
}

// openConcurrencyPool mints a small, short-lived pool against the
// concurrency-test schema. Caller is responsible for closing it.
func openConcurrencyPool() *pgxpool.Pool {
	separator := "?"
	if strings.Contains(sharedDSN, "?") {
		separator = "&"
	}
	dsn := fmt.Sprintf("%s%ssearch_path=%s,public&pool_max_conns=2", sharedDSN, separator, concurrencySchemaName)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		panic(fmt.Sprintf("connect concurrency pool: %v", err))
	}
	return pool
}

// withConcurrencyPool installs a fresh pool for the duration of one test
// and closes it on cleanup.
func withConcurrencyPool(t *testing.T) {
	t.Helper()
	concurrencyPool = openConcurrencyPool()
	t.Cleanup(func() {
		concurrencyPool.Close()
		concurrencyPool = nil
	})
}

func truncateForConcurrency(t *testing.T, ctx context.Context) {
	t.Helper()
	tables := []string{
		`morpho_market_state`,
		`morpho_market_position`,
		`morpho_market`,
	}
	for _, table := range tables {
		if _, err := concurrencyPool.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
	if _, err := concurrencyPool.Exec(ctx, `TRUNCATE protocol CASCADE`); err != nil {
		t.Fatalf("truncate protocol: %v", err)
	}
	if _, err := concurrencyPool.Exec(ctx, `TRUNCATE token CASCADE`); err != nil {
		t.Fatalf("truncate token: %v", err)
	}
}

type morphoMarketKey struct {
	morphoMarketID int64
	blockNumber    int64
	blockVersion   int
	timestamp      time.Time
}

// seedMorphoMarketKey seeds the protocol/token/market rows that
// morpho_market_state depends on via FK and returns the natural key the race
// test inserts under.
func seedMorphoMarketKey(t *testing.T, ctx context.Context) morphoMarketKey {
	t.Helper()

	var protocolID int64
	if err := concurrencyPool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, '\xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb'::bytea, 'Morpho Blue', 'lending', 18883124, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`).Scan(&protocolID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}

	var loanTokenID, collTokenID int64
	if err := concurrencyPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4) RETURNING id`,
		1, []byte{0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0x00, 0xab, 0xcd, 0xef, 0x01}, "USDC", 6,
	).Scan(&loanTokenID); err != nil {
		t.Fatalf("seed loan token: %v", err)
	}
	if err := concurrencyPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4) RETURNING id`,
		1, []byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x12, 0x34, 0x56, 0x78}, "WETH", 18,
	).Scan(&collTokenID); err != nil {
		t.Fatalf("seed coll token: %v", err)
	}

	var marketID int64
	if err := concurrencyPool.QueryRow(ctx,
		`INSERT INTO morpho_market (chain_id, protocol_id, market_id, loan_token_id, collateral_token_id, oracle_address, irm_address, lltv, created_at_block)
		 VALUES (1, $1, $2, $3, $4, '\x0000000000000000000000000000000000000000'::bytea, '\x0000000000000000000000000000000000000000'::bytea, 860000000000000000, 18883124)
		 RETURNING id`,
		protocolID,
		[]byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8, 0xa9, 0xaa, 0xab, 0xac, 0xad, 0xae, 0xaf, 0xb0, 0xb1, 0xb2, 0xb3, 0xb4, 0xb5, 0xb6, 0xb7, 0xb8, 0xb9, 0xba, 0xbb, 0xbc, 0xbd, 0xbe, 0xbf, 0xc0},
		loanTokenID, collTokenID,
	).Scan(&marketID); err != nil {
		t.Fatalf("seed market: %v", err)
	}

	return morphoMarketKey{
		morphoMarketID: marketID,
		blockNumber:    20_000_000,
		blockVersion:   0,
		timestamp:      time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
}

// runMorphoMarketStateRace fires off two goroutines that both insert the same
// morpho_market_state natural key with different build_ids. They synchronise
// on a Go channel so both issue their INSERT statement within microseconds of
// each other; the trigger's own advisory lock is what keeps them from
// double-assigning processing_version. With the lockless variant installed by
// the negative control, the (test-only) pg_sleep inside the trigger widens
// the race window enough to be reliably observable.
//
// Returns the processing_versions visible after both transactions complete
// and the per-worker errors. A worker's INSERT can legitimately fail with
// 23505 (unique_violation) under the lockless trigger — the caller is
// expected to tolerate that.
func runMorphoMarketStateRace(t *testing.T, ctx context.Context, key morphoMarketKey) (versions []int, errs [2]error) {
	t.Helper()

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer wg.Done()
			tx, err := concurrencyPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				errs[idx] = err
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()

			<-start
			if _, err := tx.Exec(ctx,
				`INSERT INTO morpho_market_state (
					morpho_market_id, block_number, block_version, timestamp,
					total_supply_assets, total_supply_shares,
					total_borrow_assets, total_borrow_shares,
					last_update, fee, build_id
				) VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0, 0, $5)
				ON CONFLICT (morpho_market_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
				key.morphoMarketID, key.blockNumber, key.blockVersion, key.timestamp, idx+1,
			); err != nil {
				errs[idx] = err
				return
			}
			if err := tx.Commit(ctx); err != nil {
				errs[idx] = err
			}
		}(i)
	}

	// Small grace period to let both goroutines acquire their tx and reach
	// the start channel — without this the slower goroutine can show up
	// after the faster has already returned its INSERT.
	time.Sleep(50 * time.Millisecond)
	close(start)
	wg.Wait()

	versions = collectMorphoMarketStateVersions(t, ctx, key)
	return versions, errs
}

func collectMorphoMarketStateVersions(t *testing.T, ctx context.Context, key morphoMarketKey) []int {
	t.Helper()
	rows, err := concurrencyPool.Query(ctx,
		`SELECT processing_version FROM morpho_market_state
		 WHERE morpho_market_id = $1 AND block_number = $2
		   AND block_version = $3 AND timestamp = $4
		 ORDER BY processing_version`,
		key.morphoMarketID, key.blockNumber, key.blockVersion, key.timestamp,
	)
	if err != nil {
		t.Fatalf("query versions: %v", err)
	}
	defer rows.Close()
	var versions []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			t.Fatalf("scan version: %v", err)
		}
		versions = append(versions, v)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iter versions: %v", err)
	}
	return versions
}

// TestProcessingVersionTrigger_CrossBuildRace_MorphoMarketState is the
// VEC-194 acceptance test: two concurrent transactions inserting the same
// morpho_market_state natural key with different build_ids must both
// commit, with processing_version 0 going to one and 1 going to the other.
func TestProcessingVersionTrigger_CrossBuildRace_MorphoMarketState(t *testing.T) {
	withConcurrencyPool(t)
	ctx := context.Background()
	truncateForConcurrency(t, ctx)
	key := seedMorphoMarketKey(t, ctx)

	versions, errs := runMorphoMarketStateRace(t, ctx, key)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	if want := []int{0, 1}; !equalInts(versions, want) {
		t.Fatalf("processing_version assignment incorrect: got %v, want %v — both rows must survive with distinct versions", versions, want)
	}
}

// TestProcessingVersionTrigger_NegativeControl_LocklessFunction verifies the
// race test above is genuinely exercising the lock by re-running it after
// stripping the advisory lock from the trigger function. Without the lock,
// both transactions race past MAX(processing_version), both compute 0, and
// one row is silently swallowed by ON CONFLICT DO NOTHING — yielding one
// surviving row instead of two. If this ever fails to observe the lost row,
// the harness is no longer testing what it claims to test.
//
// The race is statistical even with the synchronisation barrier: the workers
// are released from the barrier with sub-millisecond skew, but Postgres
// scheduling can still serialise them. Retry until the race manifests once.
func TestProcessingVersionTrigger_NegativeControl_LocklessFunction(t *testing.T) {
	withConcurrencyPool(t)
	ctx := context.Background()

	t.Cleanup(func() { restoreLockedMorphoMarketStateTrigger(t) })
	installLocklessMorphoMarketStateTrigger(t, ctx)

	// With the pg_sleep widened race window the bug is observed on the first
	// attempt in practice, but a small retry buffer absorbs CI scheduling
	// variance.
	const attempts = 5
	for attempt := 0; attempt < attempts; attempt++ {
		truncateForConcurrency(t, ctx)
		key := seedMorphoMarketKey(t, ctx)

		versions, errs := runMorphoMarketStateRace(t, ctx, key)
		for i, err := range errs {
			if err != nil && !isUniqueViolation(err) {
				t.Fatalf("attempt %d, worker %d: unexpected error: %v", attempt, i, err)
			}
		}

		if len(versions) < 2 {
			// Race observed: one row was lost. Negative control passed.
			return
		}
	}

	t.Fatalf("negative control failed to observe a lost row in %d attempts — the race test is not actually exercising the trigger lock", attempts)
}

func installLocklessMorphoMarketStateTrigger(t *testing.T, ctx context.Context) {
	t.Helper()
	// pg_sleep at the end of the trigger function widens the race window:
	// both workers compute MAX(processing_version)+1=0 against an empty
	// snapshot, sleep, then INSERT — the second INSERT hits the unique
	// constraint and ON CONFLICT DO NOTHING swallows it. Without the sleep,
	// the in-process INSERT path is fast enough that the workers serialize
	// naturally and the negative control passes by accident. The sleep is
	// safe because the production lockless function (which the trigger
	// always calls in production) doesn't have it — it's a test-only knob.
	const sql = `
		CREATE OR REPLACE FUNCTION public.assign_processing_version_morpho_market_state()
		RETURNS TRIGGER AS $$
		DECLARE existing_ver INT; max_ver INT;
		BEGIN
			SELECT processing_version INTO existing_ver
			FROM morpho_market_state
			WHERE morpho_market_id = NEW.morpho_market_id
			  AND block_number = NEW.block_number
			  AND block_version = NEW.block_version
			  AND timestamp = NEW.timestamp
			  AND build_id = NEW.build_id
			LIMIT 1;
			IF FOUND THEN
				NEW.processing_version := existing_ver;
			ELSE
				SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
				FROM morpho_market_state
				WHERE morpho_market_id = NEW.morpho_market_id
				  AND block_number = NEW.block_number
				  AND block_version = NEW.block_version
				  AND timestamp = NEW.timestamp;
				NEW.processing_version := max_ver + 1;
			END IF;
			PERFORM pg_sleep(0.1);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`
	if _, err := concurrencyPool.Exec(ctx, sql); err != nil {
		t.Fatalf("install lockless trigger: %v", err)
	}
	// Verify by following the trigger pointer on the test-schema table —
	// `WHERE proname = ...` resolves via search_path which can return a
	// shadowed function in test_pv_concurrency that the trigger doesn't use.
	var src string
	if err := concurrencyPool.QueryRow(ctx, `
		SELECT prosrc FROM pg_proc
		WHERE oid = (SELECT tgfoid FROM pg_trigger
		             WHERE tgrelid = 'morpho_market_state'::regclass
		               AND NOT tgisinternal
		             LIMIT 1)`).Scan(&src); err != nil {
		t.Fatalf("read trigger function source: %v", err)
	}
	if !strings.Contains(src, "pg_sleep") {
		t.Fatalf("lockless trigger install didn't take effect — function source has no pg_sleep:\n%s", src)
	}
}

func restoreLockedMorphoMarketStateTrigger(t *testing.T) {
	t.Helper()
	const sql = `
		CREATE OR REPLACE FUNCTION public.assign_processing_version_morpho_market_state()
		RETURNS TRIGGER AS $$
		DECLARE existing_ver INT; max_ver INT;
		BEGIN
			PERFORM pg_advisory_xact_lock(hashtextextended(
				format('mms|%s|%s|%s|%s',
					NEW.morpho_market_id, NEW.block_number, NEW.block_version, NEW.timestamp),
				0));
			SELECT processing_version INTO existing_ver
			FROM morpho_market_state
			WHERE morpho_market_id = NEW.morpho_market_id
			  AND block_number = NEW.block_number
			  AND block_version = NEW.block_version
			  AND timestamp = NEW.timestamp
			  AND build_id = NEW.build_id
			LIMIT 1;
			IF FOUND THEN
				NEW.processing_version := existing_ver;
			ELSE
				SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
				FROM morpho_market_state
				WHERE morpho_market_id = NEW.morpho_market_id
				  AND block_number = NEW.block_number
				  AND block_version = NEW.block_version
				  AND timestamp = NEW.timestamp;
				NEW.processing_version := max_ver + 1;
			END IF;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`
	if _, err := concurrencyPool.Exec(context.Background(), sql); err != nil {
		t.Logf("WARNING: restore locked trigger function failed: %v — subsequent tests in this schema may behave unexpectedly", err)
	}
}

// isUniqueViolation reports whether err is a Postgres unique constraint
// violation. Avoids importing pgconn just to type-assert one error code.
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	type sqlStateProvider interface {
		SQLState() string
	}
	var p sqlStateProvider
	if errors.As(err, &p) {
		return p.SQLState() == "23505"
	}
	msg := err.Error()
	return strings.Contains(msg, "23505") || strings.Contains(msg, "unique constraint")
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
