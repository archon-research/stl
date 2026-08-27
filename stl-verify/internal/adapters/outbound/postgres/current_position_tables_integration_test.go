//go:build integration

package postgres

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Covers the *_current caches (20260820_120000_create_current_position_tables.sql
// and its per-cache siblings) through the production write path — the batched,
// natural-key-sorted repository inserts — rather than hand-written SQL. The trigger
// semantics themselves are covered per-row in python/tests/integration/repositories/
// (test_current_position_tables.py and the per-cache test_*_current.py modules);
// what only shows up here is batching, chunk compression, and cross-writer locking.

const currentTablesDBName = "test_current_tables"

// currentTablesPool is minted per test rather than at package setup, for the
// max_connections reason documented on concurrencyPool.
var currentTablesPool *pgxpool.Pool

func init() {
	registerTestFileSetup(func() {
		testutil.SetupDBForMain(sharedDSN, currentTablesDBName).Close()
	}, func() {
		currentTablesPool = openCurrentTablesPool()
		testutil.CleanupDBForMain(sharedDSN, currentTablesPool, currentTablesDBName)
	})
}

func openCurrentTablesPool() *pgxpool.Pool {
	cfg, err := pgxpool.ParseConfig(testutil.DatabaseDSN(sharedDSN, currentTablesDBName))
	if err != nil {
		panic(fmt.Sprintf("parse current-tables DSN: %v", err))
	}
	cfg.MaxConns = 4

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		panic(fmt.Sprintf("connect current-tables pool: %v", err))
	}
	return pool
}

func withCurrentTablesPool(t *testing.T) {
	t.Helper()
	currentTablesPool = openCurrentTablesPool()
	t.Cleanup(func() {
		currentTablesPool.Close()
		currentTablesPool = nil
	})
}

// currentTablesFixture holds the FK rows the histories need plus the
// production writers under test.
type currentTablesFixture struct {
	positionRepo *PositionRepository
	priceRepo    *OnchainPriceRepository
	supplyRepo   *TokenTotalSupplyRepository
	txm          *TxManager
	protocolID   int64
	userID       int64
	tokenIDs     [2]int64
}

func setupCurrentTables(t *testing.T) *currentTablesFixture {
	t.Helper()
	ctx := context.Background()
	resetCurrentTables(t, ctx)

	positionRepo, err := NewPositionRepository(currentTablesPool, nil, 0, 100)
	if err != nil {
		t.Fatalf("new position repository: %v", err)
	}
	priceRepo, err := NewOnchainPriceRepository(currentTablesPool, nil, 0, 100)
	if err != nil {
		t.Fatalf("new onchain price repository: %v", err)
	}
	tokenRepo, err := NewTokenRepository(currentTablesPool, nil, 0)
	if err != nil {
		t.Fatalf("new token repository: %v", err)
	}
	txm, err := NewTxManager(currentTablesPool, nil)
	if err != nil {
		t.Fatalf("new tx manager: %v", err)
	}
	supplyRepo := NewTokenTotalSupplyRepository(currentTablesPool, txm, tokenRepo, nil, buildregistry.BuildID(1))

	f := &currentTablesFixture{positionRepo: positionRepo, priceRepo: priceRepo, supplyRepo: supplyRepo, txm: txm}
	f.seedRegistries(t, ctx)
	return f
}

// resetCurrentTables clears the histories and their caches. The caches are
// cleared explicitly: nothing cascades from a history delete to them, which is
// the point of the design.
func resetCurrentTables(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, table := range []string{
		"borrower", "borrower_current",
		"borrower_collateral", "borrower_collateral_current",
		"onchain_token_price", "token_price_current",
		"token_total_supply", "token_total_supply_current",
	} {
		if _, err := currentTablesPool.Exec(ctx, `DELETE FROM `+table); err != nil {
			t.Fatalf("clear %s: %v", table, err)
		}
	}
	if _, err := currentTablesPool.Exec(ctx, `TRUNCATE "user" CASCADE`); err != nil {
		t.Fatalf("truncate user: %v", err)
	}
	if _, err := currentTablesPool.Exec(ctx, `TRUNCATE token CASCADE`); err != nil {
		t.Fatalf("truncate token: %v", err)
	}
}

func (f *currentTablesFixture) seedRegistries(t *testing.T, ctx context.Context) {
	t.Helper()

	if err := currentTablesPool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES (1, '\x2222222222222222222222222222222222222222'::bytea, 'CurrentTablesTest', 'lending', 100, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`).Scan(&f.protocolID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}

	if err := currentTablesPool.QueryRow(ctx,
		`INSERT INTO "user" (chain_id, address, first_seen_block) VALUES (1, $1, 100) RETURNING id`,
		[]byte{0x33, 0x33, 0x33},
	).Scan(&f.userID); err != nil {
		t.Fatalf("seed user: %v", err)
	}

	for i := range f.tokenIDs {
		if err := currentTablesPool.QueryRow(ctx,
			`INSERT INTO token (chain_id, address, symbol, decimals) VALUES (1, $1, $2, 18) RETURNING id`,
			[]byte{0x44, 0x44, byte(i)}, fmt.Sprintf("TK%d", i),
		).Scan(&f.tokenIDs[i]); err != nil {
			t.Fatalf("seed token %d: %v", i, err)
		}
	}
}

// borrowerAt builds a debt row for one of the fixture's tokens.
func (f *currentTablesFixture) borrowerAt(tokenIdx int, block int64, blockVersion int, amount int64) *entity.Borrower {
	return &entity.Borrower{
		UserID: f.userID, ProtocolID: f.protocolID, TokenID: f.tokenIDs[tokenIdx],
		BlockNumber: block, BlockVersion: blockVersion,
		Amount: big.NewInt(amount), Change: big.NewInt(0),
		EventType: "Borrow", TxHash: []byte{byte(block)},
		CreatedAt: time.Unix(1700000000, 0).UTC(),
	}
}

func (f *currentTablesFixture) collateralAt(tokenIdx int, block int64, amount int64, enabled bool) *entity.BorrowerCollateral {
	return &entity.BorrowerCollateral{
		UserID: f.userID, ProtocolID: f.protocolID, TokenID: f.tokenIDs[tokenIdx],
		BlockNumber: block, BlockVersion: 0,
		Amount: big.NewInt(amount), Change: big.NewInt(0),
		EventType: "Deposit", TxHash: []byte{byte(block)},
		CollateralEnabled: enabled,
		CreatedAt:         time.Unix(1700000000, 0).UTC(),
	}
}

// saveBorrowers runs one batch through the production writer in its own
// transaction, the way the position tracker does per block.
func (f *currentTablesFixture) saveBorrowers(t *testing.T, ctx context.Context, rows ...*entity.Borrower) {
	t.Helper()
	tx, err := currentTablesPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := f.positionRepo.SaveBorrowers(ctx, tx, rows); err != nil {
		t.Fatalf("SaveBorrowers: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func (f *currentTablesFixture) saveCollaterals(t *testing.T, ctx context.Context, rows ...*entity.BorrowerCollateral) {
	t.Helper()
	tx, err := currentTablesPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := f.positionRepo.SaveBorrowerCollaterals(ctx, tx, rows); err != nil {
		t.Fatalf("SaveBorrowerCollaterals: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// cachedDebt returns the cached amount and block for one key.
func (f *currentTablesFixture) cachedDebt(t *testing.T, ctx context.Context, tokenIdx int) (amount string, block int64) {
	t.Helper()
	if err := currentTablesPool.QueryRow(ctx,
		`SELECT amount::text, block_number FROM borrower_current
		 WHERE protocol_id = $1 AND user_id = $2 AND token_id = $3`,
		f.protocolID, f.userID, f.tokenIDs[tokenIdx],
	).Scan(&amount, &block); err != nil {
		t.Fatalf("read borrower_current: %v", err)
	}
	return amount, block
}

// assertCachesMatchHistory is the invariant the whole design rests on: each cache
// holds exactly "newest row per key" over its history, nothing more and nothing
// stale. Symmetric EXCEPT so a missing row and a stale row are distinguishable.
func assertCachesMatchHistory(t *testing.T, ctx context.Context) {
	t.Helper()

	checks := []struct{ table, newest string }{
		{"borrower_current", `
			SELECT DISTINCT ON (protocol_id, user_id, token_id)
			       protocol_id, user_id, token_id, amount, block_number, block_version, processing_version
			FROM borrower
			ORDER BY protocol_id, user_id, token_id,
			         block_number DESC, block_version DESC, processing_version DESC`},
		{"borrower_collateral_current", `
			SELECT DISTINCT ON (protocol_id, user_id, token_id)
			       protocol_id, user_id, token_id, amount, collateral_enabled,
			       block_number, block_version, processing_version
			FROM borrower_collateral
			ORDER BY protocol_id, user_id, token_id,
			         block_number DESC, block_version DESC, processing_version DESC`},
		{"token_price_current", `
			SELECT DISTINCT ON (oracle_id, token_id)
			       oracle_id::bigint, token_id, price_usd, block_number,
			       block_version::int, processing_version
			FROM onchain_token_price
			ORDER BY oracle_id, token_id,
			         block_number DESC, block_version DESC, processing_version DESC`},
		{"token_total_supply_current", `
			SELECT DISTINCT ON (chain_id, token_id)
			       chain_id, token_id, total_supply, scaled_total_supply, block_timestamp,
			       block_number, block_version, processing_version, created_at
			FROM token_total_supply
			ORDER BY chain_id, token_id,
			         block_number DESC, block_version DESC, processing_version DESC,
			         block_timestamp DESC`},
	}

	for _, c := range checks {
		query := fmt.Sprintf(`
			WITH newest AS (%s)
			SELECT (SELECT count(*) FROM (TABLE newest EXCEPT TABLE %s) a),
			       (SELECT count(*) FROM (TABLE %s EXCEPT TABLE newest) b)`,
			c.newest, c.table, c.table)

		var historyNotInCache, cacheNotInHistory int
		if err := currentTablesPool.QueryRow(ctx, query).Scan(&historyNotInCache, &cacheNotInHistory); err != nil {
			t.Fatalf("invariant check for %s: %v", c.table, err)
		}
		if historyNotInCache != 0 || cacheNotInHistory != 0 {
			t.Errorf("%s diverged from history: %d newest-history rows missing from the cache, %d cache rows that are not the newest",
				c.table, historyNotInCache, cacheNotInHistory)
		}
	}
}

// TestCurrentTables_ProductionWritePath_CachesNewestPerKey drives the batched
// repository writers across several blocks and two keys, then asserts each cache
// equals "newest row per key". The per-row triggers fire inside pgx.Batch and
// multi-row INSERT statements here, which the per-row SQL tests never exercise.
func TestCurrentTables_ProductionWritePath_CachesNewestPerKey(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	for _, block := range []int64{100, 200, 300} {
		f.saveBorrowers(t, ctx,
			f.borrowerAt(0, block, 0, block*10),
			f.borrowerAt(1, block, 0, block*20),
		)
		f.saveCollaterals(t, ctx,
			f.collateralAt(0, block, block*30, true),
			f.collateralAt(1, block, block*40, block != 300),
		)
	}

	prices := []*entity.OnchainTokenPrice{
		{TokenID: f.tokenIDs[0], OracleID: 7, BlockNumber: 300, BlockVersion: 0, Timestamp: time.Unix(1700000300, 0).UTC(), PriceUSD: 3},
		{TokenID: f.tokenIDs[0], OracleID: 7, BlockNumber: 100, BlockVersion: 0, Timestamp: time.Unix(1700000100, 0).UTC(), PriceUSD: 1},
		{TokenID: f.tokenIDs[1], OracleID: 7, BlockNumber: 200, BlockVersion: 0, Timestamp: time.Unix(1700000200, 0).UTC(), PriceUSD: 2},
	}
	if err := f.priceRepo.UpsertPrices(ctx, prices); err != nil {
		t.Fatalf("UpsertPrices: %v", err)
	}

	for _, block := range []int64{100, 200, 300} {
		f.saveSupplies(t, ctx,
			f.supplyAt(0, block, block*50),
			f.supplyAt(1, block, block*60),
		)
	}

	if amount, block := f.cachedDebt(t, ctx, 0); amount != "3000" || block != 300 {
		t.Errorf("borrower_current for token 0: got amount %s at block %d, want 3000 at 300", amount, block)
	}
	if supply, block := f.cachedSupply(t, ctx, 0); supply != "15000" || block != 300 {
		t.Errorf("token_total_supply_current for token 0: got supply %s at block %d, want 15000 at 300", supply, block)
	}
	assertCachesMatchHistory(t, ctx)
}

// TestCurrentTables_ProductionWritePath_BackfilledOldRowDoesNotRegress covers the
// backfill case through each batched writer: a run that fills a gap below the
// current block must leave the cache on the newer row.
func TestCurrentTables_ProductionWritePath_BackfilledOldRowDoesNotRegress(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()

	cases := []struct {
		name string
		// write persists one batch of {block, value} rows; read returns the cached row.
		write func(t *testing.T, f *currentTablesFixture, rows ...[2]int64)
		read  func(t *testing.T, f *currentTablesFixture) (value string, block int64)
	}{
		{
			name: "borrower",
			write: func(t *testing.T, f *currentTablesFixture, rows ...[2]int64) {
				batch := make([]*entity.Borrower, len(rows))
				for i, r := range rows {
					batch[i] = f.borrowerAt(0, r[0], 0, r[1])
				}
				f.saveBorrowers(t, ctx, batch...)
			},
			read: func(t *testing.T, f *currentTablesFixture) (string, int64) {
				return f.cachedDebt(t, ctx, 0)
			},
		},
		{
			name: "token_total_supply",
			write: func(t *testing.T, f *currentTablesFixture, rows ...[2]int64) {
				batch := make([]*entity.TokenTotalSupply, len(rows))
				for i, r := range rows {
					batch[i] = f.supplyAt(0, r[0], r[1])
				}
				f.saveSupplies(t, ctx, batch...)
			},
			read: func(t *testing.T, f *currentTablesFixture) (string, int64) {
				return f.cachedSupply(t, ctx, 0)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := setupCurrentTables(t)

			c.write(t, f, [2]int64{500, 5000})

			// A backfiller filling blocks 100-300 after the live worker already wrote 500.
			c.write(t, f, [2]int64{100, 1000}, [2]int64{200, 2000}, [2]int64{300, 3000})

			value, block := c.read(t, f)
			if value != "5000" || block != 500 {
				t.Errorf("backfill regressed the cache: got %s at block %d, want 5000 at 500", value, block)
			}
			assertCachesMatchHistory(t, ctx)
		})
	}
}

// TestCurrentTables_TriggerFiresOnInsertIntoCompressedChunk pins the behaviour the
// caches depend on for backfills. onchain_token_price partitions on the block
// timestamp and compresses at 2 days, so a backfill of old prices inserts straight
// into an already-compressed chunk — if row triggers did not fire there, those
// writes would never reach the cache and nothing would report it.
func TestCurrentTables_TriggerFiresOnInsertIntoCompressedChunk(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	old := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	if err := f.priceRepo.UpsertPrices(ctx, []*entity.OnchainTokenPrice{
		{TokenID: f.tokenIDs[0], OracleID: 7, BlockNumber: 1000, BlockVersion: 0, Timestamp: old, PriceUSD: 1},
	}); err != nil {
		t.Fatalf("seed price: %v", err)
	}

	compressAllChunks(t, ctx, "onchain_token_price")

	// Same chunk (same day), strictly newer block.
	if err := f.priceRepo.UpsertPrices(ctx, []*entity.OnchainTokenPrice{
		{TokenID: f.tokenIDs[0], OracleID: 7, BlockNumber: 2000, BlockVersion: 0, Timestamp: old.Add(time.Hour), PriceUSD: 2},
	}); err != nil {
		t.Fatalf("insert into compressed chunk: %v", err)
	}

	var block int64
	if err := currentTablesPool.QueryRow(ctx,
		`SELECT block_number FROM token_price_current WHERE oracle_id = 7 AND token_id = $1`,
		f.tokenIDs[0],
	).Scan(&block); err != nil {
		t.Fatalf("read token_price_current: %v", err)
	}
	if block != 2000 {
		t.Errorf("cache did not follow an insert into a compressed chunk: got block %d, want 2000", block)
	}
	assertCachesMatchHistory(t, ctx)
}

// TestCurrentTables_SupplyTriggerFiresOnInsertIntoCompressedChunk mirrors the
// price test for token_total_supply (2-day columnstore policy, segmentby
// chain_id): a sweep backfilling into an already-compressed chunk must still
// reach the cache.
func TestCurrentTables_SupplyTriggerFiresOnInsertIntoCompressedChunk(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	f.saveSupplies(t, ctx, f.supplyAt(0, 1000, 1000))
	compressAllChunks(t, ctx, "token_total_supply")

	// Same chunk (1,000 seconds later), strictly newer block.
	f.saveSupplies(t, ctx, f.supplyAt(0, 2000, 2000))

	supply, block := f.cachedSupply(t, ctx, 0)
	if supply != "2000" || block != 2000 {
		t.Errorf("cache did not follow an insert into a compressed chunk: got supply %s at block %d, want 2000 at 2000", supply, block)
	}
	assertCachesMatchHistory(t, ctx)
}

func compressAllChunks(t *testing.T, ctx context.Context, hypertable string) {
	t.Helper()
	if _, err := currentTablesPool.Exec(ctx,
		`SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks($1) c`,
		hypertable,
	); err != nil {
		t.Fatalf("compress chunks of %s: %v", hypertable, err)
	}

	var uncompressed int
	if err := currentTablesPool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.chunks
		 WHERE hypertable_name = $1 AND NOT is_compressed`, hypertable,
	).Scan(&uncompressed); err != nil {
		t.Fatalf("check compression of %s: %v", hypertable, err)
	}
	if uncompressed != 0 {
		t.Fatalf("%s still has %d uncompressed chunks — the test would not be exercising the compressed path", hypertable, uncompressed)
	}
}

// TestCurrentTables_ConcurrentOverlappingWritersDoNotDeadlock is the fast-chain
// case: a live worker and a backfiller writing the same keys at different blocks.
// The cache row lock is keyed on (protocol, user, token) only, so unlike the
// assign_processing_version advisory lock — which also keys on the block — these
// two writers now contend. They stay deadlock-free solely because SaveBorrowers
// sorts by natural key, giving every caller the same lock order. Shuffled input
// per worker so the test fails if that sort is ever dropped.
func TestCurrentTables_ConcurrentOverlappingWritersDoNotDeadlock(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	const rounds = 20
	for round := 0; round < rounds; round++ {
		liveBlock := int64(1000 + round*2)
		backfillBlock := int64(1001 + round*2)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)

		// Each worker hands the repository its rows in the opposite order.
		go func() {
			defer wg.Done()
			errs[0] = f.trySaveBorrowers(ctx,
				f.borrowerAt(0, liveBlock, 0, 10),
				f.borrowerAt(1, liveBlock, 0, 10),
			)
		}()
		go func() {
			defer wg.Done()
			errs[1] = f.trySaveBorrowers(ctx,
				f.borrowerAt(1, backfillBlock, 0, 20),
				f.borrowerAt(0, backfillBlock, 0, 20),
			)
		}()
		wg.Wait()

		for i, err := range errs {
			if testutil.IsDeadlock(err) {
				t.Fatalf("round %d, worker %d deadlocked: %v — the repository's natural-key sort is what keeps the cache row locks in a caller-stable order", round, i, err)
			}
			if err != nil {
				t.Fatalf("round %d, worker %d: %v", round, i, err)
			}
		}
	}

	assertCachesMatchHistory(t, ctx)
}

// trySaveBorrowers is saveBorrowers without the t.Fatalf, for callers that need
// to inspect the error instead of failing on it.
func (f *currentTablesFixture) trySaveBorrowers(ctx context.Context, rows ...*entity.Borrower) error {
	tx, err := currentTablesPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := f.positionRepo.SaveBorrowers(ctx, tx, rows); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// supplyAt builds a supply observation for one of two tokens minted here rather
// than taken from seedRegistries: SaveSupplies resolves addresses through
// GetOrCreateToken. Zero decimals, so the cached numeric reads back as written;
// a scaled supply too, so the cache's copy of it is compared rather than NULL.
func (f *currentTablesFixture) supplyAt(tokenIdx int, block int64, supply int64) *entity.TokenTotalSupply {
	return &entity.TokenTotalSupply{
		ChainID:           1,
		TokenAddress:      supplyTokenAddress(tokenIdx),
		TokenSymbol:       fmt.Sprintf("TS%d", tokenIdx),
		TokenDecimals:     0,
		TotalSupply:       big.NewInt(supply),
		ScaledTotalSupply: big.NewInt(supply / 2),
		BlockNumber:       block,
		BlockVersion:      0,
		BlockTimestamp:    time.Unix(1700000000+block, 0).UTC(),
		Source:            "sweep",
		CreatedAtBlock:    block,
	}
}

func supplyTokenAddress(tokenIdx int) common.Address {
	return common.BytesToAddress([]byte{0x88, byte(tokenIdx)})
}

func (f *currentTablesFixture) saveSupplies(t *testing.T, ctx context.Context, rows ...*entity.TokenTotalSupply) {
	t.Helper()
	if err := f.trySaveSupplies(ctx, rows...); err != nil {
		t.Fatalf("SaveSupplies: %v", err)
	}
}

// trySaveSupplies runs one batch the way production does: through the TxManager
// the repository is constructed around.
func (f *currentTablesFixture) trySaveSupplies(ctx context.Context, rows ...*entity.TokenTotalSupply) error {
	return f.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return f.supplyRepo.SaveSupplies(ctx, tx, rows)
	})
}

func (f *currentTablesFixture) cachedSupply(t *testing.T, ctx context.Context, tokenIdx int) (supply string, block int64) {
	t.Helper()
	if err := currentTablesPool.QueryRow(ctx,
		`SELECT c.total_supply::text, c.block_number
		 FROM token_total_supply_current c
		 JOIN token tk ON tk.id = c.token_id AND tk.chain_id = c.chain_id
		 WHERE tk.address = $1`,
		supplyTokenAddress(tokenIdx).Bytes(),
	).Scan(&supply, &block); err != nil {
		t.Fatalf("read token_total_supply_current: %v", err)
	}
	return supply, block
}

// TestCurrentTables_ConcurrentOverlappingSupplyWritersDoNotDeadlock: two
// SaveSupplies batches over the same tokens at different blocks, shuffled input
// per writer, so the test fails if the natural-key sort is ever dropped. Today
// the sort's first job is ordering the token registry rows — GetOrCreateToken's
// upsert locks those before any cache row, serializing overlapping batches — and
// the test keeps holding if that upsert ever becomes DO NOTHING + SELECT and the
// batches truly overlap on the cache rows. The negative control below is what
// pins the cache-row half of the claim.
func TestCurrentTables_ConcurrentOverlappingSupplyWritersDoNotDeadlock(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	// Register the tokens first: concurrent GetOrCreateToken of an absent token is
	// its own race, and this test is about the cache rows.
	f.saveSupplies(t, ctx, f.supplyAt(0, 3999, 1), f.supplyAt(1, 3999, 1))

	const rounds = 20
	for round := 0; round < rounds; round++ {
		liveBlock := int64(4000 + round*2)
		sweepBlock := int64(4001 + round*2)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)

		go func() {
			defer wg.Done()
			errs[0] = f.trySaveSupplies(ctx,
				f.supplyAt(0, liveBlock, 10),
				f.supplyAt(1, liveBlock, 10),
			)
		}()
		go func() {
			defer wg.Done()
			errs[1] = f.trySaveSupplies(ctx,
				f.supplyAt(1, sweepBlock, 20),
				f.supplyAt(0, sweepBlock, 20),
			)
		}()
		wg.Wait()

		for i, err := range errs {
			if testutil.IsDeadlock(err) {
				t.Fatalf("round %d, writer %d deadlocked: %v — SaveSupplies' natural-key sort is what keeps the registry and cache row locks in a caller-stable order", round, i, err)
			}
			if err != nil {
				t.Fatalf("round %d, writer %d: %v", round, i, err)
			}
		}
	}

	assertCachesMatchHistory(t, ctx)
}

// TestCurrentTables_NegativeControl_ReverseKeyOrderDeadlocks proves the test above
// is exercising a real lock rather than passing for want of contention: the same
// two writers, on the same two keys at different blocks, deadlock as soon as they
// take the cache rows in opposite orders. Inserted row-by-row with a pause so the
// interleaving is deterministic instead of timing-dependent.
func TestCurrentTables_NegativeControl_ReverseKeyOrderDeadlocks(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)

	go func() {
		defer wg.Done()
		errs[0] = f.insertPausedPair(ctx, 2000, 0, 1)
	}()
	go func() {
		defer wg.Done()
		errs[1] = f.insertPausedPair(ctx, 2001, 1, 0)
	}()
	wg.Wait()

	if !testutil.IsDeadlock(errs[0]) && !testutil.IsDeadlock(errs[1]) {
		t.Fatalf("expected one writer to deadlock on the cache rows, got %v and %v — if this stops deadlocking, the ordering guarantee the sibling test relies on is no longer what makes it pass", errs[0], errs[1])
	}
}

// insertPausedPair inserts two debt rows for the given token order inside one
// transaction, pausing between them so the two callers overlap.
func (f *currentTablesFixture) insertPausedPair(ctx context.Context, block int64, firstToken, secondToken int) error {
	tx, err := currentTablesPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := f.positionRepo.SaveBorrower(ctx, tx, f.borrowerAt(firstToken, block, 0, 10)); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	if err := f.positionRepo.SaveBorrower(ctx, tx, f.borrowerAt(secondToken, block, 0, 10)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// TestCurrentTables_NegativeControl_ReverseSupplyKeyOrderDeadlocks is the supply
// cache's proof that its row locks are real: hand-written inserts that bypass
// SaveSupplies' sort and take the two cache rows in opposite orders deadlock. The
// production writers cannot reach this state today (the registry upsert
// serializes them first), which is exactly why the claim needs its own control.
func TestCurrentTables_NegativeControl_ReverseSupplyKeyOrderDeadlocks(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	// Register the tokens (and their cache rows) before contending on them.
	f.saveSupplies(t, ctx, f.supplyAt(0, 5999, 1), f.supplyAt(1, 5999, 1))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	wg.Add(2)

	go func() {
		defer wg.Done()
		errs[0] = f.insertPausedSupplyPair(ctx, 6000, 0, 1)
	}()
	go func() {
		defer wg.Done()
		errs[1] = f.insertPausedSupplyPair(ctx, 6001, 1, 0)
	}()
	wg.Wait()

	if !testutil.IsDeadlock(errs[0]) && !testutil.IsDeadlock(errs[1]) {
		t.Fatalf("expected one writer to deadlock on the cache rows, got %v and %v — if this stops deadlocking, the trigger no longer locks the cache rows the way the migration header claims", errs[0], errs[1])
	}
}

// insertPausedSupplyPair inserts supply rows for two minted tokens in the given
// order inside one transaction, pausing between them so the two callers overlap.
// Raw SQL, deliberately: the point is to reach the cache rows without
// SaveSupplies' sort or its registry upsert.
func (f *currentTablesFixture) insertPausedSupplyPair(ctx context.Context, block int64, firstToken, secondToken int) error {
	tx, err := currentTablesPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for i, tokenIdx := range []int{firstToken, secondToken} {
		if i == 1 {
			time.Sleep(500 * time.Millisecond)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO token_total_supply
				(chain_id, token_id, total_supply, block_number, block_version, block_timestamp, source)
			SELECT 1, tk.id, 1, $2, 0, $3, 'sweep'
			FROM token tk WHERE tk.chain_id = 1 AND tk.address = $1`,
			supplyTokenAddress(tokenIdx).Bytes(), block, time.Unix(1700000000+block, 0).UTC(),
		); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
