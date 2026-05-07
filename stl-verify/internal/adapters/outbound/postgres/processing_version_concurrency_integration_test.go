//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// runRace fires off two goroutines that each open their own READ COMMITTED
// transaction, park on a synchronisation barrier, and call insertOnce when
// released. They reach the barrier deterministically via a "ready" WaitGroup
// so neither can race past it and miss the simultaneous release.
//
// insertOnce receives the worker's transaction and a build_id (1 or 2). It
// should perform the trigger-firing INSERT for the natural key under test.
//
// Returns the per-worker errors. A worker's INSERT can legitimately fail with
// 23505 (unique_violation) under a lockless trigger — the negative-control
// test tolerates that error specifically.
func runRace(t *testing.T, ctx context.Context, insertOnce func(ctx context.Context, tx pgx.Tx, buildID int) error) [2]error {
	t.Helper()

	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(2)
	done.Add(2)
	var errs [2]error

	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer done.Done()
			tx, err := concurrencyPool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
			if err != nil {
				errs[idx] = err
				ready.Done()
				return
			}
			defer func() { _ = tx.Rollback(ctx) }()

			// Signal that the worker has acquired its tx and is parked on
			// the start barrier. The test waits for both workers to reach
			// this point before releasing them, so neither can race past
			// and miss the simultaneous unblock.
			ready.Done()
			<-start
			if err := insertOnce(ctx, tx, idx+1); err != nil {
				errs[idx] = err
				return
			}
			if err := tx.Commit(ctx); err != nil {
				errs[idx] = err
			}
		}(i)
	}

	ready.Wait()
	close(start)
	done.Wait()

	return errs
}

// runMorphoMarketStateRace is a thin wrapper that issues the
// morpho_market_state INSERT with two different build_ids.
func runMorphoMarketStateRace(t *testing.T, ctx context.Context, key morphoMarketKey) (versions []int, errs [2]error) {
	t.Helper()
	errs = runRace(t, ctx, func(ctx context.Context, tx pgx.Tx, buildID int) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO morpho_market_state (
				morpho_market_id, block_number, block_version, timestamp,
				total_supply_assets, total_supply_shares,
				total_borrow_assets, total_borrow_shares,
				last_update, fee, build_id
			) VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0, 0, $5)
			ON CONFLICT (morpho_market_id, block_number, block_version, processing_version, timestamp) DO NOTHING`,
			key.morphoMarketID, key.blockNumber, key.blockVersion, key.timestamp, buildID,
		)
		return err
	})
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

// offchainTokenPriceKey identifies a single (token_id, source_id, timestamp)
// tuple that the offchain_token_price race test inserts under.
type offchainTokenPriceKey struct {
	tokenID   int64
	sourceID  int16
	timestamp time.Time
}

// seedOffchainTokenPriceKey seeds a token row and reuses the migration-seeded
// 'coingecko' price source so the trigger has something to insert against.
func seedOffchainTokenPriceKey(t *testing.T, ctx context.Context) offchainTokenPriceKey {
	t.Helper()

	var tokenID int64
	if err := concurrencyPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals) VALUES ($1, $2, $3, $4) RETURNING id`,
		1, []byte{0xa0, 0xb8, 0x69, 0x91, 0xc6, 0x21, 0x8b, 0x36, 0xc1, 0xd1, 0x9d, 0x4a, 0x2e, 0x9e, 0xb0, 0xce, 0x36, 0x06, 0xeb, 0x48}, "USDC", 6,
	).Scan(&tokenID); err != nil {
		t.Fatalf("seed token: %v", err)
	}

	var sourceID int16
	if err := concurrencyPool.QueryRow(ctx,
		`SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID); err != nil {
		t.Fatalf("look up coingecko source: %v", err)
	}

	return offchainTokenPriceKey{
		tokenID:   tokenID,
		sourceID:  sourceID,
		timestamp: time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC),
	}
}

func runOffchainTokenPriceRace(t *testing.T, ctx context.Context, key offchainTokenPriceKey) (versions []int, errs [2]error) {
	t.Helper()
	errs = runRace(t, ctx, func(ctx context.Context, tx pgx.Tx, buildID int) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO offchain_token_price (token_id, source_id, timestamp, price_usd, build_id)
			 VALUES ($1, $2, $3, 0, $4)
			 ON CONFLICT (token_id, source_id, processing_version, timestamp) DO NOTHING`,
			key.tokenID, key.sourceID, key.timestamp, buildID,
		)
		return err
	})
	versions = collectOffchainTokenPriceVersions(t, ctx, key)
	return versions, errs
}

func collectOffchainTokenPriceVersions(t *testing.T, ctx context.Context, key offchainTokenPriceKey) []int {
	t.Helper()
	rows, err := concurrencyPool.Query(ctx,
		`SELECT processing_version FROM offchain_token_price
		 WHERE token_id = $1 AND source_id = $2 AND timestamp = $3
		 ORDER BY processing_version`,
		key.tokenID, key.sourceID, key.timestamp,
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

// primeDebtKey identifies a single (prime_id, block_number, block_version,
// synced_at) tuple that the prime_debt race test inserts under. prime_debt is
// the only state table without a primary key — only a UNIQUE constraint —
// which exercises a different trigger shape.
type primeDebtKey struct {
	primeID      int64
	blockNumber  int64
	blockVersion int
	syncedAt     time.Time
}

// seedPrimeDebtKey reuses one of the migration-seeded 'spark'/'grove'/'obex'
// rows from the prime registry so the FK is satisfied.
func seedPrimeDebtKey(t *testing.T, ctx context.Context) primeDebtKey {
	t.Helper()
	var primeID int64
	if err := concurrencyPool.QueryRow(ctx,
		`SELECT id FROM prime WHERE name = 'spark'`).Scan(&primeID); err != nil {
		t.Fatalf("look up spark prime: %v", err)
	}
	return primeDebtKey{
		primeID:      primeID,
		blockNumber:  20_000_000,
		blockVersion: 0,
		syncedAt:     time.Date(2026, 4, 28, 12, 0, 0, 0, time.UTC),
	}
}

func runPrimeDebtRace(t *testing.T, ctx context.Context, key primeDebtKey) (versions []int, errs [2]error) {
	t.Helper()
	errs = runRace(t, ctx, func(ctx context.Context, tx pgx.Tx, buildID int) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, block_version, synced_at, build_id)
			 VALUES ($1, 'TEST-ILK', 0, $2, $3, $4, $5)
			 ON CONFLICT (prime_id, block_number, block_version, processing_version, synced_at) DO NOTHING`,
			key.primeID, key.blockNumber, key.blockVersion, key.syncedAt, buildID,
		)
		return err
	})
	versions = collectPrimeDebtVersions(t, ctx, key)
	return versions, errs
}

func collectPrimeDebtVersions(t *testing.T, ctx context.Context, key primeDebtKey) []int {
	t.Helper()
	rows, err := concurrencyPool.Query(ctx,
		`SELECT processing_version FROM prime_debt
		 WHERE prime_id = $1 AND block_number = $2
		   AND block_version = $3 AND synced_at = $4
		 ORDER BY processing_version`,
		key.primeID, key.blockNumber, key.blockVersion, key.syncedAt,
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

func truncateOffchainPriceForConcurrency(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := concurrencyPool.Exec(ctx, `DELETE FROM offchain_token_price`); err != nil {
		t.Fatalf("truncate offchain_token_price: %v", err)
	}
	if _, err := concurrencyPool.Exec(ctx, `TRUNCATE token CASCADE`); err != nil {
		t.Fatalf("truncate token: %v", err)
	}
}

func truncatePrimeDebtForConcurrency(t *testing.T, ctx context.Context) {
	t.Helper()
	if _, err := concurrencyPool.Exec(ctx, `DELETE FROM prime_debt`); err != nil {
		t.Fatalf("truncate prime_debt: %v", err)
	}
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
	if want := []int{0, 1}; !slices.Equal(versions, want) {
		t.Fatalf("processing_version assignment incorrect: got %v, want %v — both rows must survive with distinct versions", versions, want)
	}
}

// TestProcessingVersionTrigger_CrossBuildRace_OffchainTokenPrice exercises
// the off-chain trigger shape: natural key has no block columns, just
// (token_id, source_id, timestamp). Same race semantics as the
// morpho_market_state test but against a different table to catch
// shape-specific regressions in the migration.
func TestProcessingVersionTrigger_CrossBuildRace_OffchainTokenPrice(t *testing.T) {
	withConcurrencyPool(t)
	ctx := context.Background()
	truncateOffchainPriceForConcurrency(t, ctx)
	key := seedOffchainTokenPriceKey(t, ctx)

	versions, errs := runOffchainTokenPriceRace(t, ctx, key)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	if want := []int{0, 1}; !slices.Equal(versions, want) {
		t.Fatalf("processing_version assignment incorrect: got %v, want %v", versions, want)
	}
}

// TestProcessingVersionTrigger_CrossBuildRace_PrimeDebt exercises the
// UNIQUE-only-no-PK trigger shape: prime_debt has only a UNIQUE constraint
// (prime_id, block_number, block_version, processing_version, synced_at),
// no primary key. The ON CONFLICT clause therefore targets a unique index
// rather than a PK, and the trigger's MAX-of-processing_version path runs
// against a structurally different constraint set.
func TestProcessingVersionTrigger_CrossBuildRace_PrimeDebt(t *testing.T) {
	withConcurrencyPool(t)
	ctx := context.Background()
	truncatePrimeDebtForConcurrency(t, ctx)
	key := seedPrimeDebtKey(t, ctx)

	versions, errs := runPrimeDebtRace(t, ctx, key)

	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	if want := []int{0, 1}; !slices.Equal(versions, want) {
		t.Fatalf("processing_version assignment incorrect: got %v, want %v", versions, want)
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

	swapInLocklessTrigger(t, ctx, "morpho_market_state")

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

// swapInLocklessTrigger captures the production trigger function via
// pg_get_functiondef, replaces it with a lockless variant for the duration of
// the test, and restores the captured DDL on cleanup. Restore failure is
// fatal — leaking the lockless variant into subsequent tests would silently
// invalidate them.
//
// The lockless variant inserts a pg_sleep between MAX-read and the implicit
// INSERT to widen the race window. The same body is used for every shape;
// only the table name is parameterised.
func swapInLocklessTrigger(t *testing.T, ctx context.Context, table string) {
	t.Helper()

	var originalDDL string
	if err := concurrencyPool.QueryRow(ctx, `
		SELECT pg_get_functiondef(tgfoid) FROM pg_trigger
		WHERE tgrelid = $1::regclass AND NOT tgisinternal
		LIMIT 1`, table).Scan(&originalDDL); err != nil {
		t.Fatalf("capture original trigger function ddl for %s: %v", table, err)
	}

	t.Cleanup(func() {
		if _, err := concurrencyPool.Exec(context.Background(), originalDDL); err != nil {
			t.Fatalf("restore captured trigger function ddl for %s: %v — subsequent tests in this schema would otherwise see the lockless variant", table, err)
		}
	})

	locklessSQL := buildLocklessTriggerSQL(table, originalDDL)
	if _, err := concurrencyPool.Exec(ctx, locklessSQL); err != nil {
		t.Fatalf("install lockless trigger for %s: %v", table, err)
	}

	// Verify the swap took effect: follow the trigger pointer to the function
	// body and assert pg_sleep is present.
	var src string
	if err := concurrencyPool.QueryRow(ctx, `
		SELECT prosrc FROM pg_proc
		WHERE oid = (SELECT tgfoid FROM pg_trigger
		             WHERE tgrelid = $1::regclass AND NOT tgisinternal
		             LIMIT 1)`, table).Scan(&src); err != nil {
		t.Fatalf("read trigger function source for %s: %v", table, err)
	}
	if !strings.Contains(src, "pg_sleep") {
		t.Fatalf("lockless trigger install for %s didn't take effect — function source has no pg_sleep:\n%s", table, src)
	}
}

// buildLocklessTriggerSQL constructs a CREATE OR REPLACE FUNCTION statement
// that reuses the captured signature (so the trigger keeps pointing at the
// same function OID) but installs a body that omits pg_advisory_xact_lock and
// adds a pg_sleep before RETURN NEW. The pg_sleep widens the race window so
// the negative control reliably observes the lost row.
func buildLocklessTriggerSQL(table, originalDDL string) string {
	// pg_get_functiondef wraps the body in $function$. Older PostgreSQL or
	// hand-written DDL might use $$ — tolerate both.
	delim := "$function$"
	bodyStart := strings.Index(originalDDL, delim)
	if bodyStart < 0 {
		delim = "$$"
		bodyStart = strings.Index(originalDDL, delim)
	}
	if bodyStart < 0 {
		// Caller will surface this when the resulting SQL fails to execute.
		return originalDDL
	}
	header := originalDDL[:bodyStart+len(delim)]
	where := naturalKeyWhere(table)

	return header + `
DECLARE existing_ver INT; max_ver INT;
BEGIN
    SELECT processing_version INTO existing_ver
    FROM ` + table + `
    WHERE ` + where + `
      AND build_id = NEW.build_id
    LIMIT 1;
    IF FOUND THEN
        NEW.processing_version := existing_ver;
    ELSE
        SELECT COALESCE(MAX(processing_version), -1) INTO max_ver
        FROM ` + table + `
        WHERE ` + where + `;
        NEW.processing_version := max_ver + 1;
    END IF;
    PERFORM pg_sleep(0.1);
    RETURN NEW;
END;
` + delim + `;`
}

// naturalKeyWhere returns the natural-key WHERE clause for the given test
// table. Hardcoded per-shape because the negative control needs a known body;
// extend the switch when adding new shapes.
func naturalKeyWhere(table string) string {
	switch table {
	case "morpho_market_state":
		return `morpho_market_id = NEW.morpho_market_id
		      AND block_number = NEW.block_number
		      AND block_version = NEW.block_version
		      AND timestamp = NEW.timestamp`
	default:
		panic(fmt.Sprintf("naturalKeyWhere: unknown table %q — extend this switch when adding new shapes to the negative control", table))
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
