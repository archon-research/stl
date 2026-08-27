//go:build integration

package postgres

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Covers the *_current caches from 20260820_120000_create_current_position_tables.sql
// through the production write path — the batched, natural-key-sorted repository
// inserts — rather than hand-written SQL. The trigger semantics themselves are
// covered per-row in python/tests/integration/repositories/test_current_position_tables.py;
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

// currentTablesFixture holds the FK rows the histories need plus the two
// production writers under test.
type currentTablesFixture struct {
	positionRepo *PositionRepository
	priceRepo    *OnchainPriceRepository
	allocRepo    *AllocationRepository
	tokenRepo    *TokenRepository
	txm          *TxManager
	protocolID   int64
	userID       int64
	primeID      int64
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
	allocRepo := NewAllocationRepository(currentTablesPool, txm, tokenRepo, nil, buildregistry.BuildID(1))

	f := &currentTablesFixture{
		positionRepo: positionRepo, priceRepo: priceRepo, allocRepo: allocRepo,
		tokenRepo: tokenRepo, txm: txm,
	}
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
		"allocation_position", "allocation_position_current",
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
		`INSERT INTO prime (name, vault_address)
		 VALUES ('CurrentTablesTest', '\x5151515151515151515151515151515151515151'::bytea)
		 ON CONFLICT (name) DO UPDATE SET vault_address = EXCLUDED.vault_address
		 RETURNING id`).Scan(&f.primeID); err != nil {
		t.Fatalf("seed prime: %v", err)
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

	// cached is spelled out per table rather than `TABLE <cache>` because
	// allocation_position_current carries created_at/updated_at, which are
	// row-write times of the cache row itself and have no counterpart in history.
	checks := []struct{ table, cached, newest string }{
		{"borrower_current", `TABLE borrower_current`, `
			SELECT DISTINCT ON (protocol_id, user_id, token_id)
			       protocol_id, user_id, token_id, amount, block_number, block_version, processing_version
			FROM borrower
			ORDER BY protocol_id, user_id, token_id,
			         block_number DESC, block_version DESC, processing_version DESC`},
		{"borrower_collateral_current", `TABLE borrower_collateral_current`, `
			SELECT DISTINCT ON (protocol_id, user_id, token_id)
			       protocol_id, user_id, token_id, amount, collateral_enabled,
			       block_number, block_version, processing_version
			FROM borrower_collateral
			ORDER BY protocol_id, user_id, token_id,
			         block_number DESC, block_version DESC, processing_version DESC`},
		{"token_price_current", `TABLE token_price_current`, `
			SELECT DISTINCT ON (oracle_id, token_id)
			       oracle_id::bigint, token_id, price_usd, block_number,
			       block_version::int, processing_version
			FROM onchain_token_price
			ORDER BY oracle_id, token_id,
			         block_number DESC, block_version DESC, processing_version DESC`},
		// The seven-term order is the cache's newer-wins comparison, spelled the
		// same way here: identity first, processing_version last.
		{"allocation_position_current", `
			SELECT proxy_address, chain_id, token_id, balance, underlying_value, underlying_token_id,
			       tx_amount, direction, tx_hash, block_timestamp,
			       block_number, block_version, log_index, processing_version
			FROM allocation_position_current`, `
			SELECT DISTINCT ON (proxy_address, chain_id, token_id)
			       proxy_address, chain_id, token_id, balance, underlying_value, underlying_token_id,
			       tx_amount, direction, tx_hash, created_at AS block_timestamp,
			       block_number, block_version, log_index, processing_version
			FROM allocation_position
			ORDER BY proxy_address, chain_id, token_id,
			         block_number DESC, block_version DESC, created_at DESC, log_index DESC,
			         direction DESC, tx_hash DESC, processing_version DESC`},
	}

	for _, c := range checks {
		query := fmt.Sprintf(`
			WITH newest AS (%s), cached AS (%s)
			SELECT (SELECT count(*) FROM (TABLE newest EXCEPT TABLE cached) a),
			       (SELECT count(*) FROM (TABLE cached EXCEPT TABLE newest) b)`,
			c.newest, c.cached)

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

	if amount, block := f.cachedDebt(t, ctx, 0); amount != "3000" || block != 300 {
		t.Errorf("borrower_current for token 0: got amount %s at block %d, want 3000 at 300", amount, block)
	}
	assertCachesMatchHistory(t, ctx)
}

// TestCurrentTables_ProductionWritePath_BackfilledOldRowDoesNotRegress covers the
// backfill case through the batched writer: a run that fills a gap below the
// current block must leave the cache on the newer row.
func TestCurrentTables_ProductionWritePath_BackfilledOldRowDoesNotRegress(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	f.saveBorrowers(t, ctx, f.borrowerAt(0, 500, 0, 5000))

	// A backfiller filling blocks 100-300 after the live worker already wrote 500.
	f.saveBorrowers(t, ctx,
		f.borrowerAt(0, 100, 0, 1000),
		f.borrowerAt(0, 200, 0, 2000),
		f.borrowerAt(0, 300, 0, 3000),
	)

	amount, block := f.cachedDebt(t, ctx, 0)
	if amount != "5000" || block != 500 {
		t.Errorf("backfill regressed the cache: got amount %s at block %d, want 5000 at 500", amount, block)
	}
	assertCachesMatchHistory(t, ctx)
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

// allocationAt builds a transfer-driven position row for one of the fixture's
// tokens on the given proxy. Token addresses are minted here rather than taken
// from seedRegistries: SavePositions resolves them through GetOrCreateToken.
// The options vary the fields the cache's newer-wins comparison turns on.
func (f *currentTablesFixture) allocationAt(tokenIdx, proxyIdx int, block int64, logIndex int, balance int64, opts ...allocationOpt) *entity.AllocationPosition {
	proxy := common.BytesToAddress([]byte{0x55, byte(proxyIdx)})
	counterparty := common.BytesToAddress([]byte{0x77, byte(proxyIdx)})
	pos := &entity.AllocationPosition{
		ChainID: 1, PrimeID: f.primeID, ProxyAddress: proxy,
		FromAddress: &counterparty, ToAddress: &proxy,
		TokenAddress: common.BytesToAddress([]byte{0x66, byte(tokenIdx)}),
		TokenSymbol:  fmt.Sprintf("AP%d", tokenIdx), TokenDecimals: 18,
		Balance: big.NewInt(balance), TxAmount: big.NewInt(balance),
		BlockNumber: block, BlockVersion: 0, LogIndex: logIndex,
		TxHash:         fmt.Sprintf("0x%064x", block),
		Direction:      "in",
		CreatedAtBlock: block,
		CreatedAt:      time.Unix(1700000000, 0).UTC(),
	}
	for _, opt := range opts {
		opt(pos)
	}
	return pos
}

type allocationOpt func(*entity.AllocationPosition)

// asSweep turns the row into the reconciliation snapshot the tracker's sweep path
// writes: no transfer, so no parties, no tx hash — the writer stores the zero hash
// — and a zero tx_amount. A real sweep also leaves log_index at 0, which is what
// puts it beside an event at log_index 0 in the same block.
func asSweep() allocationOpt {
	return func(pos *entity.AllocationPosition) {
		pos.Direction = "sweep"
		pos.TxHash = ""
		pos.FromAddress, pos.ToAddress = nil, nil
		pos.TxAmount = big.NewInt(0)
	}
}

// atBlockVersion re-emits the row on a reorg replacement of its block.
func atBlockVersion(version int) allocationOpt {
	return func(pos *entity.AllocationPosition) { pos.BlockVersion = version }
}

// atBlockTime sets the on-chain block time, which is allocation_position's
// partition column and therefore picks the chunk, and is copied into the cache as
// block_timestamp.
func atBlockTime(ts time.Time) allocationOpt {
	return func(pos *entity.AllocationPosition) { pos.CreatedAt = ts }
}

func (f *currentTablesFixture) trySaveAllocations(ctx context.Context, rows ...*entity.AllocationPosition) error {
	return f.trySaveAllocationsAs(ctx, f.allocRepo, rows...)
}

// trySaveAllocationsAs writes through a caller-supplied writer, so a test can
// re-save the same rows under a second build id — the only way to make
// assign_processing_version treat them as a reprocess rather than a duplicate.
func (f *currentTablesFixture) trySaveAllocationsAs(ctx context.Context, repo *AllocationRepository, rows ...*entity.AllocationPosition) error {
	tx, err := currentTablesPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := repo.SavePositions(ctx, tx, rows); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// saveAllocations runs one batch through the production writer in its own
// transaction, the way the allocation tracker does per block.
func (f *currentTablesFixture) saveAllocations(t *testing.T, ctx context.Context, rows ...*entity.AllocationPosition) {
	t.Helper()
	if err := f.trySaveAllocations(ctx, rows...); err != nil {
		t.Fatalf("SavePositions: %v", err)
	}
}

// allocRepoForBuild is a second writer over the same pool under a different build
// id. assign_processing_version_allocation_position reuses an existing row's
// version only when build_id matches too, so a re-save from another build lands as
// processing_version + 1 instead of colliding with ON CONFLICT DO NOTHING.
func (f *currentTablesFixture) allocRepoForBuild(buildID int) *AllocationRepository {
	return NewAllocationRepository(currentTablesPool, f.txm, f.tokenRepo, nil, buildregistry.BuildID(buildID))
}

// cachedAllocation returns the whole payload of one cache row, which is what the
// newer-wins tests assert against.
func (f *currentTablesFixture) cachedAllocation(t *testing.T, ctx context.Context, tokenIdx, proxyIdx int) cachedAllocationRow {
	t.Helper()
	proxy := common.BytesToAddress([]byte{0x55, byte(proxyIdx)})
	tokenAddr := common.BytesToAddress([]byte{0x66, byte(tokenIdx)})

	var row cachedAllocationRow
	// The writer stores a balance scaled by the token's decimals, and the fixture
	// mints 18-decimal tokens, so 1e18 recovers the raw integer the caller passed.
	if err := currentTablesPool.QueryRow(ctx,
		`SELECT (c.balance * 1e18)::bigint, c.direction, encode(c.tx_hash, 'hex'), c.block_number,
		        c.block_version, c.log_index, c.processing_version
		 FROM allocation_position_current c
		 JOIN token t ON t.id = c.token_id
		 WHERE c.proxy_address = $1 AND c.chain_id = 1 AND t.address = $2`,
		proxy.Bytes(), tokenAddr.Bytes(),
	).Scan(&row.balance, &row.direction, &row.txHash, &row.blockNumber,
		&row.blockVersion, &row.logIndex, &row.processingVersion); err != nil {
		t.Fatalf("read allocation_position_current: %v", err)
	}
	return row
}

type cachedAllocationRow struct {
	balance           int64
	direction         string
	txHash            string
	blockNumber       int64
	blockVersion      int
	logIndex          int
	processingVersion int
}

// String makes a failure message show the whole cached row, since every one of
// these fields is part of what the newer-wins comparison decided.
func (r cachedAllocationRow) String() string {
	return fmt.Sprintf("balance=%d direction=%s tx_hash=%s block=%d/%d log_index=%d processing_version=%d",
		r.balance, r.direction, r.txHash, r.blockNumber, r.blockVersion, r.logIndex, r.processingVersion)
}

// TestCurrentTables_ConcurrentOverlappingAllocationWritersDoNotDeadlock is the
// borrower test's counterpart for allocation_position_current, and the reason its
// migration calls SavePositions' natural-key sort load-bearing: the cache row lock
// is keyed on (chain, proxy, token) alone, so a live tracker and a backfiller
// writing the same proxy at different blocks now contend. Shuffled input per
// writer, so the test fails if that sort is ever dropped.
func TestCurrentTables_ConcurrentOverlappingAllocationWritersDoNotDeadlock(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	// Register the tokens first: concurrent GetOrCreateToken of an absent token is
	// its own (pre-existing) race, and this test is about the cache rows.
	if err := f.trySaveAllocations(ctx,
		f.allocationAt(0, 0, 2999, 0, 1),
		f.allocationAt(1, 1, 2999, 1, 1),
	); err != nil {
		t.Fatalf("register tokens: %v", err)
	}

	const rounds = 20
	for round := 0; round < rounds; round++ {
		liveBlock := int64(3000 + round*2)
		backfillBlock := int64(3001 + round*2)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		wg.Add(2)

		go func() {
			defer wg.Done()
			errs[0] = f.trySaveAllocations(ctx,
				f.allocationAt(0, 0, liveBlock, 0, 10),
				f.allocationAt(1, 1, liveBlock, 1, 10),
			)
		}()
		go func() {
			defer wg.Done()
			errs[1] = f.trySaveAllocations(ctx,
				f.allocationAt(1, 1, backfillBlock, 1, 20),
				f.allocationAt(0, 0, backfillBlock, 0, 20),
			)
		}()
		wg.Wait()

		for i, err := range errs {
			if testutil.IsDeadlock(err) {
				t.Fatalf("round %d, writer %d deadlocked: %v — SavePositions' natural-key sort is what keeps the cache row locks in a caller-stable order", round, i, err)
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

// TestCurrentTables_AllocationSweepWinsSameBlockTie covers the one collision the
// allocation tracker can actually write for a single key inside one block: the
// sweep row — log_index 0, zero tx hash, direction sweep — beside a transfer event
// that also sits at log_index 0. block_number, block_version, block_timestamp and
// log_index are all equal across that pair, so unless direction and tx_hash are
// part of the comparison the winner is whichever row arrived first, and a cache
// whose content depends on arrival order is not the newest-row query it claims to
// be. The sweep is the winner in both orders because it is a reconciled balance
// read of the whole position, not a per-event derivation.
func TestCurrentTables_AllocationSweepWinsSameBlockTie(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		eventFirst bool
	}{
		{"event arrives first", true},
		{"sweep arrives first", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setupCurrentTables(t)

			const block = 900
			event := f.allocationAt(0, 0, block, 0, 111)
			sweep := f.allocationAt(0, 0, block, 0, 222, asSweep())

			first, second := event, sweep
			if !tc.eventFirst {
				first, second = sweep, event
			}
			// Two batches, not one: SavePositions sorts within a batch, so arrival
			// order only exists between calls.
			f.saveAllocations(t, ctx, first)
			f.saveAllocations(t, ctx, second)

			got := f.cachedAllocation(t, ctx, 0, 0)
			zeroHash := fmt.Sprintf("%064x", 0)
			if got.direction != "sweep" || got.balance != 222 || got.txHash != zeroHash {
				t.Errorf("cache holds %s, want the sweep row (balance=222 direction=sweep tx_hash=%s)", got, zeroHash)
			}
			assertCachesMatchHistory(t, ctx)
		})
	}
}

// TestCurrentTables_AllocationReorgReplacementWinsAtLowerLogIndex pins the rank of
// block_version above log_index. A reorg re-emits a block's logs at whatever
// positions the new block gives them, so the replacement routinely lands EARLIER
// in the block than the row it supersedes; if log_index outranked block_version
// the cache would keep the orphaned observation.
func TestCurrentTables_AllocationReorgReplacementWinsAtLowerLogIndex(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	const block = 1100
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, block, 5, 10))
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, block, 1, 20, atBlockVersion(1)))

	got := f.cachedAllocation(t, ctx, 0, 0)
	if got.blockVersion != 1 || got.balance != 20 || got.logIndex != 1 {
		t.Errorf("cache holds %s, want the reorg replacement (balance=20 block_version=1 log_index=1)", got)
	}
	assertCachesMatchHistory(t, ctx)
}

// TestCurrentTables_AllocationReprocessWinsAndSameBuildIsNoop covers the last term
// of the comparison from both sides. A re-save from a different build is a genuine
// correction of ONE row: assign_processing_version_allocation_position keys on
// build_id too, so it lands as processing_version 1 and must overwrite the cache.
// A re-save from the SAME build is a duplicate: it collides on the full natural
// key, ON CONFLICT DO NOTHING drops it, no AFTER INSERT trigger fires, and the
// cache must not move even though the payload offered differs.
func TestCurrentTables_AllocationReprocessWinsAndSameBuildIsNoop(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	const block = 1200
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, block, 0, 10))

	// Same row, corrected balance, second build: processing_version 1.
	if err := f.trySaveAllocationsAs(ctx, f.allocRepoForBuild(2), f.allocationAt(0, 0, block, 0, 99)); err != nil {
		t.Fatalf("reprocess through the second build: %v", err)
	}
	if got := f.cachedAllocation(t, ctx, 0, 0); got.processingVersion != 1 || got.balance != 99 {
		t.Errorf("cache holds %s, want the reprocessed row (balance=99 processing_version=1)", got)
	}

	// Same row again from the original build, with a payload that would be visible
	// if it were written at all.
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, block, 0, 12345))

	var historyRows int
	if err := currentTablesPool.QueryRow(ctx, `SELECT count(*)::int FROM allocation_position`).Scan(&historyRows); err != nil {
		t.Fatalf("count allocation_position: %v", err)
	}
	if historyRows != 2 {
		t.Errorf("history holds %d rows, want 2 — the same-build re-save should have been dropped by ON CONFLICT DO NOTHING", historyRows)
	}
	if got := f.cachedAllocation(t, ctx, 0, 0); got.processingVersion != 1 || got.balance != 99 {
		t.Errorf("a same-build re-save moved the cache: holds %s, want balance=99 processing_version=1", got)
	}
	assertCachesMatchHistory(t, ctx)
}

// TestCurrentTables_AllocationTriggerFiresOnInsertIntoCompressedChunk is the
// allocation counterpart of the price test above, and matters more here:
// allocation_position partitions on the block timestamp and columnstores at 2
// days, so every backfill of historical positions writes straight into an
// already-compressed chunk. If row triggers did not fire there, a backfilled
// proxy would never reach the cache and nothing would report it.
func TestCurrentTables_AllocationTriggerFiresOnInsertIntoCompressedChunk(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	// One day, so all three rows share a chunk and the two below are written into
	// the compressed one rather than into a fresh chunk beside it.
	day := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, 1500, 0, 15, atBlockTime(day.Add(2*time.Hour))))

	compressAllChunks(t, ctx, "allocation_position")

	// A backfiller filling in below the cached block, then the live tracker moving
	// past it — both into the compressed chunk.
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, 500, 0, 5, atBlockTime(day.Add(time.Hour))))
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, 2000, 0, 20, atBlockTime(day.Add(3*time.Hour))))

	got := f.cachedAllocation(t, ctx, 0, 0)
	if got.blockNumber != 2000 || got.balance != 20 {
		t.Errorf("cache holds %s after writes into a compressed chunk, want balance=20 at block 2000", got)
	}
	assertCachesMatchHistory(t, ctx)
}

// TestCurrentTables_AllocationBackfillAgreesWithTheTrigger covers the half of the
// design nothing else reaches. 20260825_120100 is a separate migration so that
// CREATE TRIGGER's lock is not held for a full-history scan, and the cost of the
// split is that the backfill applies against an empty allocation_position and is
// never exercised again — while its DISTINCT ON has to stay the trigger's
// newer-wins comparison, term for term, or the two writers disagree about which
// row is current. So: build a history that turns on every term, wipe the cache the
// trigger filled, and require the backfill alone to reproduce it.
func TestCurrentTables_AllocationBackfillAgreesWithTheTrigger(t *testing.T) {
	withCurrentTablesPool(t)
	ctx := context.Background()
	f := setupCurrentTables(t)

	const block = 1300
	// One key where a later event beats the block's sweep, then a reprocess of
	// that event beats itself.
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, block, 3, 10))
	f.saveAllocations(t, ctx, f.allocationAt(0, 0, block, 0, 20, asSweep()))
	if err := f.trySaveAllocationsAs(ctx, f.allocRepoForBuild(2), f.allocationAt(0, 0, block, 3, 11)); err != nil {
		t.Fatalf("reprocess through the second build: %v", err)
	}
	// One key where a reorg replacement at a lower log_index beats the original.
	f.saveAllocations(t, ctx, f.allocationAt(1, 0, block, 1, 30))
	f.saveAllocations(t, ctx, f.allocationAt(1, 0, block, 0, 40, atBlockVersion(1)))
	// One key holding the tie itself — an event and the block's sweep, both at
	// log_index 0 — which only direction and tx_hash separate.
	f.saveAllocations(t, ctx, f.allocationAt(2, 0, block, 0, 50))
	f.saveAllocations(t, ctx, f.allocationAt(2, 0, block, 0, 60, asSweep()))
	assertCachesMatchHistory(t, ctx)

	if _, err := currentTablesPool.Exec(ctx, `DELETE FROM allocation_position_current`); err != nil {
		t.Fatalf("empty the cache: %v", err)
	}
	runAllocationBackfillMigration(t, ctx)
	assertCachesMatchHistory(t, ctx)

	// And over the cache it just filled it is a guarded no-op, not a regression:
	// this is the statement an operator re-runs to converge a drifted cache.
	runAllocationBackfillMigration(t, ctx)
	assertCachesMatchHistory(t, ctx)
}

// runAllocationBackfillMigration applies 20260825_120100 the way the migrator
// does — the whole file, one Exec, one transaction, since SET LOCAL only warns
// outside a transaction block.
func runAllocationBackfillMigration(t *testing.T, ctx context.Context) {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve this file's path")
	}
	sql, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "../../../../db/migrations",
		"20260825_120100_backfill_allocation_position_current.sql"))
	if err != nil {
		t.Fatalf("read the backfill migration: %v", err)
	}

	tx, err := currentTablesPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, string(sql)); err != nil {
		t.Fatalf("apply the backfill migration: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit the backfill migration: %v", err)
	}
}
