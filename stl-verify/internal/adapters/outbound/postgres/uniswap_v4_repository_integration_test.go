//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Synthetic chains the registry fixtures below are seeded on, one per scenario
// so a deliberately-broken or multi-version registry can never leak into
// another test's LoadPools call. Only the mainnet fixtures live on chain 1.
const (
	uniswapV4RepoSaveChainID        = 490001
	uniswapV4RepoNullDecChainID     = 490002
	uniswapV4RepoMismatchChainID    = 490003
	uniswapV4RepoEmptyChainID       = 490004
	uniswapV4RepoNoManagerChainID   = 490005
	uniswapV4RepoPoolVerChainID     = 490006
	uniswapV4RepoManagerVerChainID  = 490007
	uniswapV4RepoNativeChainID      = 490008
	uniswapV4RepoHomeChainID        = 490009
	uniswapV4RepoNeighbourChainID   = 490010
	uniswapV4RepoXChainTokenChainID = 490011
	uniswapV4RepoXChainDonorChainID = 490012
	uniswapV4RepoUnsupportedChainID = 490013
	uniswapV4RepoPriorStateChainID  = 490014
	uniswapV4RepoSupersededChainID  = 490015
	uniswapV4RepoEverIndexedChainID = 490016
	uniswapV4RepoEverIndexedFwdChID = 490017
	uniswapV4RepoEverIndexedNbrChID = 490018
	uniswapV4RepoEverIndexedFgnChID = 490019
	uniswapV4RepoXChainMgrChainID   = 490020
	uniswapV4RepoXChainMgrDonorChID = 490021
)

// testUniswapV4BuildID / testUniswapV4RebuildID are two distinct build ids so a
// test can prove build_id is threaded through and that a re-index by a newer
// build lands at processing_version 1 instead of deduplicating.
const (
	testUniswapV4BuildID   = buildregistry.BuildID(1)
	testUniswapV4RebuildID = buildregistry.BuildID(2)
)

func newUniswapV4Repo(t *testing.T) *UniswapV4Repository {
	t.Helper()
	return NewUniswapV4Repository(uniswapV4TestPool, testUniswapV4BuildID)
}

// withUniswapV4Tx runs fn inside a transaction against uniswapV4TestPool,
// committing on success. Rollback is deferred so a t.Fatal mid-fn still
// releases the connection.
func withUniswapV4Tx(t *testing.T, ctx context.Context, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := uniswapV4TestPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// withUniswapV4RollbackTx runs fn in a transaction that is always rolled back,
// so a failed SaveBlock is observed exactly as the worker would leave it.
func withUniswapV4RollbackTx(t *testing.T, ctx context.Context, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := uniswapV4TestPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
}

// seedUniswapV4RepoChain registers the synthetic chain a fixture hangs off, so
// the chain_id FKs on token / protocol / uniswap_v4_pool resolve.
func seedUniswapV4RepoChain(t *testing.T, ctx context.Context, chainID int) {
	t.Helper()
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES ($1, $2) ON CONFLICT (chain_id) DO NOTHING`,
		chainID, fmt.Sprintf("uniswap_v4_test_%d", chainID),
	); err != nil {
		t.Fatalf("seed chain %d: %v", chainID, err)
	}
}

// seedUniswapV4RepoToken upserts a token row and returns its id. decimals ==
// nil inserts SQL NULL, which is what the NULL-decimals rejection test needs.
func seedUniswapV4RepoToken(t *testing.T, ctx context.Context, chainID int, addr common.Address, symbol string, decimals *int) int64 {
	t.Helper()
	seedUniswapV4RepoChain(t, ctx, chainID)
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (chain_id, address) DO NOTHING`,
		chainID, addr.Bytes(), symbol, decimals,
	); err != nil {
		t.Fatalf("seed token %s: %v", symbol, err)
	}
	var tokenID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM token WHERE chain_id = $1 AND address = $2`,
		chainID, addr.Bytes(),
	).Scan(&tokenID); err != nil {
		t.Fatalf("read back token %s: %v", symbol, err)
	}
	return tokenID
}

// uniswapV4RepoManagerFixture is one version of a chain's PoolManager registry
// row. buildID drives the append: re-seeding with the same buildID reuses the
// row's processing_version and is a no-op, a new buildID appends the next
// version, which is the one LoadPools must pick up.
type uniswapV4RepoManagerFixture struct {
	chainID int
	// protocolChainID is the chain the FK'd protocol row is seeded on; 0 means
	// chainID, the only coherent registry. Setting it elsewhere seeds the
	// cross-chain PoolManager defect.
	protocolChainID int
	manager         common.Address
	stateView       common.Address
	deployBlock     int64
	buildID         int
}

func newUniswapV4RepoManagerFixture(chainID int) uniswapV4RepoManagerFixture {
	return uniswapV4RepoManagerFixture{
		chainID:     chainID,
		manager:     common.HexToAddress("0x00000000000000000000000000000000000044c5"),
		stateView:   common.HexToAddress("0x0000000000000000000000000000000000007ffe"),
		deployBlock: 1,
	}
}

// seedUniswapV4RepoPoolManager upserts the protocol row plus one version of the
// chain's uniswap_v4_pool_manager row.
func seedUniswapV4RepoPoolManager(t *testing.T, ctx context.Context, f uniswapV4RepoManagerFixture) {
	t.Helper()
	seedUniswapV4RepoChain(t, ctx, f.chainID)
	protocolChainID := f.protocolChainID
	if protocolChainID == 0 {
		protocolChainID = f.chainID
	}
	seedUniswapV4RepoChain(t, ctx, protocolChainID)
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		 VALUES ($1, $2, 'UniswapV4', 'dex', $3)
		 ON CONFLICT (chain_id, address) DO NOTHING`,
		protocolChainID, f.manager.Bytes(), f.deployBlock,
	); err != nil {
		t.Fatalf("seed protocol on chain %d: %v", protocolChainID, err)
	}
	var protocolID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE chain_id = $1 AND address = $2`,
		protocolChainID, f.manager.Bytes(),
	).Scan(&protocolID); err != nil {
		t.Fatalf("read back protocol on chain %d: %v", protocolChainID, err)
	}

	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO uniswap_v4_pool_manager
		    (chain_id, protocol_id, state_view_address, deploy_block, build_id)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (chain_id, processing_version) DO NOTHING`,
		f.chainID, protocolID, f.stateView.Bytes(), f.deployBlock, f.buildID,
	); err != nil {
		t.Fatalf("seed pool manager on chain %d: %v", f.chainID, err)
	}
}

// uniswapV4RepoPoolFixture is one version of a registry pool row. buildID has
// the same append semantics as on uniswapV4RepoManagerFixture.
type uniswapV4RepoPoolFixture struct {
	chainID          int
	poolID           common.Hash
	currency0        common.Address
	currency1        common.Address
	currency0TokenID int64
	currency1TokenID int64
	fee              int
	tickSpacing      int
	hooks            common.Address
	deployBlock      int64
	buildID          int
	// excludeFromSnapshots seeds snapshot_supported = false; the column's own
	// default is true, which is what every other fixture wants.
	excludeFromSnapshots bool
}

// seedUniswapV4RepoPool appends one pool version and returns the surrogate id
// of the chain's current version of that PoolKey.
func seedUniswapV4RepoPool(t *testing.T, ctx context.Context, f uniswapV4RepoPoolFixture) int64 {
	t.Helper()
	seedUniswapV4RepoChain(t, ctx, f.chainID)
	if f.deployBlock == 0 {
		f.deployBlock = 21688329
	}
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO uniswap_v4_pool
		    (chain_id, pool_id, currency0, currency1,
		     currency0_token_id, currency1_token_id, fee, tick_spacing, hooks,
		     deploy_block, build_id, snapshot_supported)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		 ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING`,
		f.chainID, f.poolID.Bytes(), f.currency0.Bytes(), f.currency1.Bytes(),
		f.currency0TokenID, f.currency1TokenID, f.fee, f.tickSpacing,
		f.hooks.Bytes(), f.deployBlock, f.buildID, !f.excludeFromSnapshots,
	); err != nil {
		t.Fatalf("seed uniswap_v4_pool %s: %v", f.poolID, err)
	}
	var poolID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM uniswap_v4_pool WHERE chain_id = $1 AND pool_id = $2
		 ORDER BY processing_version DESC LIMIT 1`,
		f.chainID, f.poolID.Bytes(),
	).Scan(&poolID); err != nil {
		t.Fatalf("read back uniswap_v4_pool %s: %v", f.poolID, err)
	}
	return poolID
}

// newUniswapV4RepoPoolFixture builds a pool fixture on chainID whose currencies
// and PoolId are derived from discriminator, so each caller gets a pool of its
// own without repeating the token seeding.
func newUniswapV4RepoPoolFixture(t *testing.T, ctx context.Context, chainID int, discriminator byte) uniswapV4RepoPoolFixture {
	t.Helper()
	currency0 := common.Address{0x10, discriminator}
	currency1 := common.Address{0x20, discriminator}
	decimals := 18
	return uniswapV4RepoPoolFixture{
		chainID:          chainID,
		poolID:           common.Hash{discriminator},
		currency0:        currency0,
		currency1:        currency1,
		currency0TokenID: seedUniswapV4RepoToken(t, ctx, chainID, currency0, "TK0", &decimals),
		currency1TokenID: seedUniswapV4RepoToken(t, ctx, chainID, currency1, "TK1", &decimals),
		fee:              3000,
		tickSpacing:      60,
	}
}

// seedUniswapV4RepoTestPool builds a self-contained registry pool on the chain
// the write-path tests use, keyed by discriminator so each test gets its own.
func seedUniswapV4RepoTestPool(t *testing.T, ctx context.Context, discriminator byte) int64 {
	t.Helper()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoSaveChainID))
	return seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoSaveChainID, discriminator))
}

// uniswapV4SeedTokenDecimals indexes the seeded token decimals by the bytea
// literal of the token's address.
func uniswapV4SeedTokenDecimals() map[string]int {
	byAddr := make(map[string]int, len(uniswapV4SeedTokens))
	for _, tok := range uniswapV4SeedTokens {
		byAddr[tok.addrHex] = tok.decimals
	}
	return byAddr
}

func TestUniswapV4Repository_LoadPools_SeededMainnetPools(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4Registry(t, ctx)

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var registryRows int
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT count(DISTINCT pool_id) FROM uniswap_v4_pool WHERE chain_id = 1`,
	).Scan(&registryRows); err != nil {
		t.Fatalf("counting chain-1 registry pools: %v", err)
	}
	if len(pools) != registryRows {
		t.Fatalf("LoadPools returned %d pools, want %d (one current version per PoolKey; the token join must neither drop nor duplicate)", len(pools), registryRows)
	}

	byPoolID := make(map[common.Hash]outbound.UniswapV4PoolRow, len(pools))
	for _, p := range pools {
		if _, dup := byPoolID[p.PoolIDHash]; dup {
			t.Fatalf("LoadPools returned pool_id %s twice", p.PoolIDHash)
		}
		byPoolID[p.PoolIDHash] = p
	}

	wantPoolManager := common.BytesToAddress(decodeBytea(t, uniswapV4PoolManagerHex))
	wantStateView := common.BytesToAddress(decodeBytea(t, uniswapV4StateViewHex))
	decimalsByAddr := uniswapV4SeedTokenDecimals()

	for _, want := range uniswapV4ExpectedPools {
		t.Run(want.name, func(t *testing.T) {
			wantPoolID := common.BytesToHash(decodeBytea(t, want.poolIDHex))
			got, ok := byPoolID[wantPoolID]
			if !ok {
				t.Fatalf("pool %s (pool_id %s) missing from LoadPools result", want.name, wantPoolID)
			}
			if got.ID <= 0 {
				t.Errorf("ID = %d, want a positive surrogate id", got.ID)
			}
			if got.ProtocolID <= 0 {
				t.Errorf("ProtocolID = %d, want a positive protocol id", got.ProtocolID)
			}
			if got.PoolManager != wantPoolManager {
				t.Errorf("PoolManager = %s, want %s", got.PoolManager, wantPoolManager)
			}
			if got.StateView != wantStateView {
				t.Errorf("StateView = %s, want %s", got.StateView, wantStateView)
			}
			if wantC0 := common.BytesToAddress(decodeBytea(t, want.currency0Hex)); got.Currency0 != wantC0 {
				t.Errorf("Currency0 = %s, want %s", got.Currency0, wantC0)
			}
			if wantC1 := common.BytesToAddress(decodeBytea(t, want.currency1Hex)); got.Currency1 != wantC1 {
				t.Errorf("Currency1 = %s, want %s", got.Currency1, wantC1)
			}
			if wantDec := decimalsByAddr[uniswapV4TokenAddrFor(want.currency0Hex)]; got.Currency0Decimals != wantDec {
				t.Errorf("Currency0Decimals = %d, want %d", got.Currency0Decimals, wantDec)
			}
			if wantDec := decimalsByAddr[uniswapV4TokenAddrFor(want.currency1Hex)]; got.Currency1Decimals != wantDec {
				t.Errorf("Currency1Decimals = %d, want %d", got.Currency1Decimals, wantDec)
			}
			if int64(got.Fee) != want.fee {
				t.Errorf("Fee = %d, want %d", got.Fee, want.fee)
			}
			if int64(got.TickSpacing) != want.tickSpacing {
				t.Errorf("TickSpacing = %d, want %d", got.TickSpacing, want.tickSpacing)
			}
			if wantHooks := common.BytesToAddress(decodeBytea(t, want.hooksHex)); got.Hooks != wantHooks {
				t.Errorf("Hooks = %s, want %s", got.Hooks, wantHooks)
			}
			if got.DeployBlock != want.deployBlock {
				t.Errorf("DeployBlock = %d, want %d", got.DeployBlock, want.deployBlock)
			}
		})
	}
}

func TestUniswapV4Repository_LoadPools_UnregisteredChainIsEmpty(t *testing.T) {
	ctx := context.Background()

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoEmptyChainID)
	if err != nil {
		t.Fatalf("LoadPools on a chain with no registry rows: %v", err)
	}
	if len(pools) != 0 {
		t.Fatalf("LoadPools returned %d pools, want 0", len(pools))
	}
}

func TestUniswapV4Repository_LoadPools_RejectsNullTokenDecimals(t *testing.T) {
	ctx := context.Background()

	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoNullDecChainID))

	currency0 := common.HexToAddress("0x00000000000000000000000000000000000000b1")
	currency1 := common.HexToAddress("0x00000000000000000000000000000000000000b2")
	decimals := 18
	token0ID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoNullDecChainID, currency0, "GOOD", &decimals)
	token1ID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoNullDecChainID, currency1, "NODEC", nil)

	poolID := seedUniswapV4RepoPool(t, ctx, uniswapV4RepoPoolFixture{
		chainID:          uniswapV4RepoNullDecChainID,
		poolID:           common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000dec01"),
		currency0:        currency0,
		currency1:        currency1,
		currency0TokenID: token0ID,
		currency1TokenID: token1ID,
		fee:              500,
		tickSpacing:      10,
	})

	repo := newUniswapV4Repo(t)
	_, err := repo.LoadPools(ctx, uniswapV4RepoNullDecChainID)
	if err == nil {
		t.Fatalf("LoadPools with a NULL-decimals token on pool id=%d: want error, got nil", poolID)
	}
	if !containsPoolID(err.Error(), poolID) {
		t.Errorf("error %q does not name the offending pool id %d", err, poolID)
	}
}

func TestUniswapV4Repository_LoadPools_RejectsCurrencyTokenMismatch(t *testing.T) {
	ctx := context.Background()

	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoMismatchChainID))

	currency0 := common.HexToAddress("0x00000000000000000000000000000000000000d1")
	currency1 := common.HexToAddress("0x00000000000000000000000000000000000000d2")
	wrongToken := common.HexToAddress("0x00000000000000000000000000000000000000ff")
	decimals := 18
	wrongTokenID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoMismatchChainID, wrongToken, "WRONG", &decimals)
	token1ID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoMismatchChainID, currency1, "RIGHT", &decimals)

	poolID := seedUniswapV4RepoPool(t, ctx, uniswapV4RepoPoolFixture{
		chainID:          uniswapV4RepoMismatchChainID,
		poolID:           common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000a15a15"),
		currency0:        currency0,
		currency1:        currency1,
		currency0TokenID: wrongTokenID,
		currency1TokenID: token1ID,
		fee:              500,
		tickSpacing:      10,
	})

	repo := newUniswapV4Repo(t)
	_, err := repo.LoadPools(ctx, uniswapV4RepoMismatchChainID)
	if err == nil {
		t.Fatalf("LoadPools with currency0 != token0.address on pool id=%d: want error, got nil", poolID)
	}
	if !containsPoolID(err.Error(), poolID) {
		t.Errorf("error %q does not name the offending pool id %d", err, poolID)
	}
}

func TestUniswapV4Repository_LoadPools_RejectsChainWithPoolsButNoPoolManager(t *testing.T) {
	ctx := context.Background()

	poolID := seedUniswapV4RepoPool(t, ctx,
		newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoNoManagerChainID, 0x21))

	repo := newUniswapV4Repo(t)
	_, err := repo.LoadPools(ctx, uniswapV4RepoNoManagerChainID)
	if err == nil {
		t.Fatalf("LoadPools on a chain with pool id=%d but no pool manager: want error, got nil", poolID)
	}
	if !strings.Contains(err.Error(), "uniswap_v4_pool_manager") {
		t.Errorf("error %q does not name the missing uniswap_v4_pool_manager row", err)
	}
}

// TestUniswapV4Repository_LoadPools_ReturnsLatestPoolVersion pins the
// append-only registry read: a corrected pool is a NEW row with a new surrogate
// id, and LoadPools must return only that one, never the superseded version.
func TestUniswapV4Repository_LoadPools_ReturnsLatestPoolVersion(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoPoolVerChainID))

	fixture := newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoPoolVerChainID, 0x22)
	fixture.deployBlock = 1000
	originalID := seedUniswapV4RepoPool(t, ctx, fixture)

	fixture.deployBlock = 2000
	fixture.buildID = 1
	correctedID := seedUniswapV4RepoPool(t, ctx, fixture)
	if correctedID == originalID {
		t.Fatalf("the corrected pool reused id %d; the fixture did not append a new version", correctedID)
	}

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoPoolVerChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	var matches []outbound.UniswapV4PoolRow
	for _, p := range pools {
		if p.PoolIDHash == fixture.poolID {
			matches = append(matches, p)
		}
	}
	if len(matches) != 1 {
		t.Fatalf("LoadPools returned %d rows for pool_id %s, want 1 (only the current version)", len(matches), fixture.poolID)
	}
	if matches[0].ID != correctedID {
		t.Errorf("ID = %d, want %d (the corrected version, not the superseded %d)", matches[0].ID, correctedID, originalID)
	}
	if matches[0].DeployBlock != 2000 {
		t.Errorf("DeployBlock = %d, want 2000 (the corrected version's value)", matches[0].DeployBlock)
	}
}

// TestUniswapV4Repository_LoadPools_UsesLatestPoolManagerVersion covers the
// other half of the versioned registry: a corrected PoolManager row must
// re-point every pool on the chain at the new StateView.
func TestUniswapV4Repository_LoadPools_UsesLatestPoolManagerVersion(t *testing.T) {
	ctx := context.Background()

	manager := newUniswapV4RepoManagerFixture(uniswapV4RepoManagerVerChainID)
	supersededStateView := manager.stateView
	seedUniswapV4RepoPoolManager(t, ctx, manager)

	correctedStateView := common.HexToAddress("0x000000000000000000000000000000000000c0de")
	manager.stateView = correctedStateView
	manager.buildID = 1
	seedUniswapV4RepoPoolManager(t, ctx, manager)

	seedUniswapV4RepoPool(t, ctx,
		newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoManagerVerChainID, 0x23))

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoManagerVerChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	if len(pools) == 0 {
		t.Fatal("LoadPools returned no pools")
	}
	for _, p := range pools {
		if p.StateView != correctedStateView {
			t.Errorf("pool %d StateView = %s, want %s (the superseded %s must not win)",
				p.ID, p.StateView, correctedStateView, supersededStateView)
		}
	}
}

func TestUniswapV4Repository_SaveBlock_RoundTripsEveryTable(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x01)

	const blockNumber = int64(21800000)
	blockTimestamp := time.Unix(1740000000, 0).UTC()
	txHash := common.HexToHash("0xaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd")
	sender := common.HexToAddress("0x66a9893cc07d91d95644aedd05d03f95e1dba8af")
	salt := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ff")

	poolEventParams, err := json.Marshal(map[string]any{"amount0": "10", "amount1": "20"})
	if err != nil {
		t.Fatalf("marshal donate params: %v", err)
	}

	state := &entity.UniswapV4PoolState{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: 0, BlockTimestamp: blockTimestamp,
		SqrtPriceX96:         bigFromString(t, "79228162514264337593543950336"),
		Tick:                 -276324,
		ProtocolFee:          1000 | (500 << 12),
		LpFee:                3000,
		Liquidity:            bigFromString(t, "1234567890123456789"),
		FeeGrowthGlobal0X128: bigFromString(t, "340282366920938463463374607431768211456"),
		FeeGrowthGlobal1X128: big.NewInt(22),
	}
	swap := &entity.UniswapV4Swap{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: 0, BlockTimestamp: blockTimestamp,
		TxHash: txHash, LogIndex: 3, Sender: sender,
		Amount0:      bigFromString(t, "-1000000000000000000"),
		Amount1:      bigFromString(t, "990000000000000000"),
		SqrtPriceX96: bigFromString(t, "79228162514264337593543950336"),
		Liquidity:    big.NewInt(123456789),
		Tick:         -5,
		Fee:          500,
	}
	liquidityEvent := &entity.UniswapV4LiquidityEvent{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: 0, BlockTimestamp: blockTimestamp,
		TxHash: txHash, LogIndex: 4, Sender: sender,
		TickLower: -120, TickUpper: 180,
		LiquidityDelta: bigFromString(t, "-9876543210987654321"),
		Salt:           salt,
	}
	poolEvent := &entity.UniswapV4PoolEvent{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: 0, BlockTimestamp: blockTimestamp,
		TxHash: txHash, LogIndex: 5,
		EventName: entity.UniswapV4PoolEventDonate, Params: poolEventParams,
	}
	tick := newUniswapV4TestTick(poolID, 180, blockNumber, 0, big.NewInt(4242))

	for _, v := range []interface{ Validate() error }{state, swap, liquidityEvent, poolEvent, tick} {
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	}

	repo := newUniswapV4Repo(t)
	var stateRows int64
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		var saveErr error
		stateRows, saveErr = repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States:          []*entity.UniswapV4PoolState{state},
			Swaps:           []*entity.UniswapV4Swap{swap},
			LiquidityEvents: []*entity.UniswapV4LiquidityEvent{liquidityEvent},
			Ticks:           []*entity.UniswapV4Tick{tick},
			PoolEvents:      []*entity.UniswapV4PoolEvent{poolEvent},
		})
		if saveErr != nil {
			t.Fatalf("SaveBlock: %v", saveErr)
		}
	})
	if stateRows != 1 {
		t.Errorf("stateRows = %d, want 1", stateRows)
	}

	t.Run("pool_state", func(t *testing.T) {
		var (
			sqrtPrice, liquidity, feeGrowth0, feeGrowth1 string
			gotTick, protocolFee, lpFee, buildID         int
			gotTimestamp                                 time.Time
		)
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT sqrt_price_x96::text, tick, protocol_fee, lp_fee, liquidity::text,
			        fee_growth_global0_x128::text, fee_growth_global1_x128::text,
			        block_timestamp, build_id
			 FROM uniswap_v4_pool_state WHERE pool_id=$1 AND block_number=$2`,
			poolID, blockNumber,
		).Scan(&sqrtPrice, &gotTick, &protocolFee, &lpFee, &liquidity,
			&feeGrowth0, &feeGrowth1, &gotTimestamp, &buildID); err != nil {
			t.Fatalf("read back state: %v", err)
		}
		if sqrtPrice != state.SqrtPriceX96.String() {
			t.Errorf("sqrt_price_x96 = %q, want %q", sqrtPrice, state.SqrtPriceX96)
		}
		if gotTick != state.Tick {
			t.Errorf("tick = %d, want %d", gotTick, state.Tick)
		}
		if protocolFee != state.ProtocolFee {
			t.Errorf("protocol_fee = %d, want %d", protocolFee, state.ProtocolFee)
		}
		if lpFee != state.LpFee {
			t.Errorf("lp_fee = %d, want %d", lpFee, state.LpFee)
		}
		if liquidity != state.Liquidity.String() {
			t.Errorf("liquidity = %q, want %q", liquidity, state.Liquidity)
		}
		if feeGrowth0 != state.FeeGrowthGlobal0X128.String() {
			t.Errorf("fee_growth_global0_x128 = %q, want %q", feeGrowth0, state.FeeGrowthGlobal0X128)
		}
		if feeGrowth1 != state.FeeGrowthGlobal1X128.String() {
			t.Errorf("fee_growth_global1_x128 = %q, want %q", feeGrowth1, state.FeeGrowthGlobal1X128)
		}
		if !gotTimestamp.UTC().Equal(blockTimestamp) {
			t.Errorf("block_timestamp = %s, want %s", gotTimestamp.UTC(), blockTimestamp)
		}
		if buildID != int(testUniswapV4BuildID) {
			t.Errorf("build_id = %d, want %d (threaded from the constructor, not defaulted)", buildID, testUniswapV4BuildID)
		}
	})

	t.Run("swap", func(t *testing.T) {
		var (
			amount0, amount1, sqrtPrice, liquidity string
			gotTick, logIndex, fee                 int
			gotTxHash, gotSender                   []byte
		)
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT tx_hash, log_index, sender, amount0::text, amount1::text,
			        sqrt_price_x96::text, liquidity::text, tick, fee
			 FROM uniswap_v4_swap WHERE pool_id=$1 AND block_number=$2`,
			poolID, blockNumber,
		).Scan(&gotTxHash, &logIndex, &gotSender, &amount0, &amount1,
			&sqrtPrice, &liquidity, &gotTick, &fee); err != nil {
			t.Fatalf("read back swap: %v", err)
		}
		if common.BytesToHash(gotTxHash) != txHash {
			t.Errorf("tx_hash = %s, want %s", common.BytesToHash(gotTxHash), txHash)
		}
		if logIndex != swap.LogIndex {
			t.Errorf("log_index = %d, want %d", logIndex, swap.LogIndex)
		}
		if len(gotSender) != common.AddressLength {
			t.Errorf("sender is %d bytes, want %d", len(gotSender), common.AddressLength)
		}
		if common.BytesToAddress(gotSender) != sender {
			t.Errorf("sender = %s, want %s", common.BytesToAddress(gotSender), sender)
		}
		if amount0 != swap.Amount0.String() {
			t.Errorf("amount0 = %q, want %q (the swapper paid token0 in)", amount0, swap.Amount0)
		}
		if amount1 != swap.Amount1.String() {
			t.Errorf("amount1 = %q, want %q", amount1, swap.Amount1)
		}
		if sqrtPrice != swap.SqrtPriceX96.String() {
			t.Errorf("sqrt_price_x96 = %q, want %q", sqrtPrice, swap.SqrtPriceX96)
		}
		if liquidity != swap.Liquidity.String() {
			t.Errorf("liquidity = %q, want %q", liquidity, swap.Liquidity)
		}
		if gotTick != swap.Tick {
			t.Errorf("tick = %d, want %d", gotTick, swap.Tick)
		}
		if fee != swap.Fee {
			t.Errorf("fee = %d, want %d", fee, swap.Fee)
		}
	})

	t.Run("liquidity_event", func(t *testing.T) {
		var (
			liquidityDelta         string
			tickLower, tickUpper   int
			logIndex               int
			gotSender, gotSalt     []byte
			gotTxHash              []byte
			gotBlockVersionOnRowIs int
		)
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT tx_hash, log_index, sender, tick_lower, tick_upper,
			        liquidity_delta::text, salt, block_version
			 FROM uniswap_v4_liquidity_event WHERE pool_id=$1 AND block_number=$2`,
			poolID, blockNumber,
		).Scan(&gotTxHash, &logIndex, &gotSender, &tickLower, &tickUpper,
			&liquidityDelta, &gotSalt, &gotBlockVersionOnRowIs); err != nil {
			t.Fatalf("read back liquidity event: %v", err)
		}
		if common.BytesToHash(gotTxHash) != txHash {
			t.Errorf("tx_hash = %s, want %s", common.BytesToHash(gotTxHash), txHash)
		}
		if logIndex != liquidityEvent.LogIndex {
			t.Errorf("log_index = %d, want %d", logIndex, liquidityEvent.LogIndex)
		}
		if common.BytesToAddress(gotSender) != sender {
			t.Errorf("sender = %s, want %s", common.BytesToAddress(gotSender), sender)
		}
		if tickLower != liquidityEvent.TickLower || tickUpper != liquidityEvent.TickUpper {
			t.Errorf("ticks = (%d,%d), want (%d,%d)", tickLower, tickUpper, liquidityEvent.TickLower, liquidityEvent.TickUpper)
		}
		if liquidityDelta != liquidityEvent.LiquidityDelta.String() {
			t.Errorf("liquidity_delta = %q, want %q (a burn's negative delta must survive NUMERIC round-trip)", liquidityDelta, liquidityEvent.LiquidityDelta)
		}
		if len(gotSalt) != common.HashLength {
			t.Errorf("salt is %d bytes, want %d", len(gotSalt), common.HashLength)
		}
		if common.BytesToHash(gotSalt) != salt {
			t.Errorf("salt = %s, want %s", common.BytesToHash(gotSalt), salt)
		}
		if gotBlockVersionOnRowIs != 0 {
			t.Errorf("block_version = %d, want 0", gotBlockVersionOnRowIs)
		}
	})

	t.Run("pool_event", func(t *testing.T) {
		var eventName string
		var params []byte
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT event_name, params FROM uniswap_v4_pool_event WHERE pool_id=$1 AND block_number=$2`,
			poolID, blockNumber,
		).Scan(&eventName, &params); err != nil {
			t.Fatalf("read back pool event: %v", err)
		}
		if eventName != string(entity.UniswapV4PoolEventDonate) {
			t.Errorf("event_name = %q, want %q", eventName, entity.UniswapV4PoolEventDonate)
		}
		var got map[string]any
		if err := json.Unmarshal(params, &got); err != nil {
			t.Fatalf("unmarshal params: %v", err)
		}
		if got["amount0"] != "10" || got["amount1"] != "20" {
			t.Errorf("params = %v, want amount0=10 amount1=20", got)
		}
	})

	t.Run("tick", func(t *testing.T) {
		var liquidityGross, liquidityNet, feeGrowth0, feeGrowth1 string
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT liquidity_gross::text, liquidity_net::text,
			        fee_growth_outside0_x128::text, fee_growth_outside1_x128::text
			 FROM uniswap_v4_tick WHERE pool_id=$1 AND tick=180`,
			poolID,
		).Scan(&liquidityGross, &liquidityNet, &feeGrowth0, &feeGrowth1); err != nil {
			t.Fatalf("read back tick: %v", err)
		}
		if liquidityGross != tick.LiquidityGross.String() {
			t.Errorf("liquidity_gross = %q, want %q", liquidityGross, tick.LiquidityGross)
		}
		if liquidityNet != tick.LiquidityNet.String() {
			t.Errorf("liquidity_net = %q, want %q", liquidityNet, tick.LiquidityNet)
		}
		if feeGrowth0 != tick.FeeGrowthOutside0X128.String() {
			t.Errorf("fee_growth_outside0_x128 = %q, want %q", feeGrowth0, tick.FeeGrowthOutside0X128)
		}
		if feeGrowth1 != tick.FeeGrowthOutside1X128.String() {
			t.Errorf("fee_growth_outside1_x128 = %q, want %q", feeGrowth1, tick.FeeGrowthOutside1X128)
		}
	})
}

func TestUniswapV4Repository_SaveBlock_IdenticalReplayInsertsNothing(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x02)

	writes := outbound.UniswapV4BlockWrites{
		States: []*entity.UniswapV4PoolState{
			newUniswapV4TestState(poolID, 21800010, 0, 1),
			newUniswapV4TestState(poolID, 21800011, 0, 2),
		},
	}

	repo := newUniswapV4Repo(t)
	save := func() int64 {
		var n int64
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			var err error
			if n, err = repo.SaveBlock(ctx, tx, writes); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
		return n
	}

	if got := save(); got != 2 {
		t.Errorf("first save stateRows = %d, want 2", got)
	}
	if got := save(); got != 0 {
		t.Errorf("replay stateRows = %d, want 0 (ON CONFLICT DO NOTHING)", got)
	}

	var count int
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT count(*) FROM uniswap_v4_pool_state WHERE pool_id=$1`, poolID,
	).Scan(&count); err != nil {
		t.Fatalf("count states: %v", err)
	}
	if count != 2 {
		t.Errorf("uniswap_v4_pool_state row count = %d, want 2 (a replay must not duplicate)", count)
	}
}

// TestUniswapV4Repository_SaveBlock_ReorgVersionAppendsAtProcessingVersionZero
// pins block_version as part of every fact row's identity: the same logical row
// re-observed on a new fork appends beside the old one at processing_version 0,
// so a reorg is never mistaken for a correction of the orphaned row.
func TestUniswapV4Repository_SaveBlock_ReorgVersionAppendsAtProcessingVersionZero(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x16)

	const blockNumber = int64(21800030)
	repo := newUniswapV4Repo(t)
	for _, blockVersion := range []int{0, 1} {
		writes := newUniswapV4TestBlockWrites(t, poolID, blockNumber, blockVersion)
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, writes); err != nil {
				t.Fatalf("SaveBlock at block_version %d: %v", blockVersion, err)
			}
		})
	}

	for _, table := range []string{
		"uniswap_v4_pool_state",
		"uniswap_v4_swap",
		"uniswap_v4_liquidity_event",
		"uniswap_v4_pool_event",
	} {
		t.Run(table, func(t *testing.T) {
			got := uniswapV4RowVersions(t, ctx, table, poolID, blockNumber)
			want := [][2]int{{0, 0}, {1, 0}}
			if !slices.Equal(got, want) {
				t.Errorf("(block_version, processing_version) = %v, want %v (the reorg version is part of the key, so processing_version must not bump)", got, want)
			}
		})
	}
}

// uniswapV4RowVersions returns the (block_version, processing_version) pair of
// every row table holds for the pool at blockNumber, ordered by block_version.
func uniswapV4RowVersions(t *testing.T, ctx context.Context, table string, poolID int64, blockNumber int64) [][2]int {
	t.Helper()
	rows, err := uniswapV4TestPool.Query(ctx, fmt.Sprintf(
		`SELECT block_version, processing_version FROM %s
		 WHERE pool_id=$1 AND block_number=$2 ORDER BY block_version`, table),
		poolID, blockNumber)
	if err != nil {
		t.Fatalf("query %s versions: %v", table, err)
	}
	defer rows.Close()

	var versions [][2]int
	for rows.Next() {
		var blockVersion, processingVersion int
		if err := rows.Scan(&blockVersion, &processingVersion); err != nil {
			t.Fatalf("scan %s versions: %v", table, err)
		}
		versions = append(versions, [2]int{blockVersion, processingVersion})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s versions: %v", table, err)
	}
	return versions
}

func TestUniswapV4Repository_SaveBlock_NewBuildBumpsProcessingVersion(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x03)

	state := newUniswapV4TestState(poolID, 21800020, 0, 7)
	for _, buildID := range []buildregistry.BuildID{testUniswapV4BuildID, testUniswapV4RebuildID} {
		repo := NewUniswapV4Repository(uniswapV4TestPool, buildID)
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
				States: []*entity.UniswapV4PoolState{state},
			}); err != nil {
				t.Fatalf("SaveBlock at build %d: %v", buildID, err)
			}
		})
	}

	versions := map[int]int{}
	rows, err := uniswapV4TestPool.Query(ctx,
		`SELECT processing_version, build_id FROM uniswap_v4_pool_state
		 WHERE pool_id=$1 AND block_number=21800020 ORDER BY processing_version`,
		poolID,
	)
	if err != nil {
		t.Fatalf("query states: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var processingVersion, buildID int
		if err := rows.Scan(&processingVersion, &buildID); err != nil {
			t.Fatalf("scan: %v", err)
		}
		versions[processingVersion] = buildID
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate: %v", err)
	}

	if versions[0] != int(testUniswapV4BuildID) {
		t.Errorf("processing_version 0 build_id = %d, want %d", versions[0], testUniswapV4BuildID)
	}
	if versions[1] != int(testUniswapV4RebuildID) {
		t.Errorf("processing_version 1 build_id = %d, want %d (a re-index by a newer build must not dedupe)", versions[1], testUniswapV4RebuildID)
	}
}

// uniswapV4TickFixture owns one test's pool plus the tick save/read helpers, so
// every append-on-change test starts from rows nothing else can have touched.
type uniswapV4TickFixture struct {
	t      *testing.T
	ctx    context.Context
	repo   *UniswapV4Repository
	poolID int64
}

func newUniswapV4TickFixture(t *testing.T, ctx context.Context, discriminator byte) uniswapV4TickFixture {
	t.Helper()
	return uniswapV4TickFixture{
		t:      t,
		ctx:    ctx,
		repo:   newUniswapV4Repo(t),
		poolID: seedUniswapV4RepoTestPool(t, ctx, discriminator),
	}
}

func (f uniswapV4TickFixture) save(ticks ...*entity.UniswapV4Tick) {
	f.t.Helper()
	withUniswapV4Tx(f.t, f.ctx, func(tx pgx.Tx) {
		if _, err := f.repo.SaveBlock(f.ctx, tx, outbound.UniswapV4BlockWrites{Ticks: ticks}); err != nil {
			f.t.Fatalf("SaveBlock: %v", err)
		}
	})
}

func (f uniswapV4TickFixture) rowCount(tick int) int {
	f.t.Helper()
	var count int
	if err := uniswapV4TestPool.QueryRow(f.ctx,
		`SELECT count(*) FROM uniswap_v4_tick WHERE pool_id=$1 AND tick=$2`, f.poolID, tick,
	).Scan(&count); err != nil {
		f.t.Fatalf("count ticks at %d: %v", tick, err)
	}
	return count
}

// latestValue reads one value column off the canonical-latest row at tick, the
// row a consumer asking "what is this tick now" would get.
func (f uniswapV4TickFixture) latestValue(tick int, column string) string {
	f.t.Helper()
	var value string
	if err := uniswapV4TestPool.QueryRow(f.ctx, fmt.Sprintf(
		`SELECT %s::text FROM uniswap_v4_tick WHERE pool_id=$1 AND tick=$2
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1`, column),
		f.poolID, tick,
	).Scan(&value); err != nil {
		f.t.Fatalf("query latest %s at tick %d: %v", column, tick, err)
	}
	return value
}

func TestUniswapV4Repository_WriteTicks_FirstWriteInserts(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x04)

	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))

	if got := f.rowCount(60); got != 1 {
		t.Fatalf("row count = %d, want 1", got)
	}
}

func TestUniswapV4Repository_WriteTicks_UnchangedValuesDoNotAppend(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x09)

	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5001, 0, big.NewInt(100)))

	if got := f.rowCount(60); got != 1 {
		t.Fatalf("row count = %d, want 1 (unchanged values must not append)", got)
	}
}

// TestUniswapV4Repository_WriteTicks_ChangedValueAppends covers every value
// column: a change in any one of the four must append, or that column's history
// silently flatlines.
func TestUniswapV4Repository_WriteTicks_ChangedValueAppends(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		column        string
		discriminator byte
		set           func(*uniswapV4TickValues, *big.Int)
	}{
		{"liquidity_gross", 0x0a, func(v *uniswapV4TickValues, n *big.Int) { v.liquidityGross = n }},
		{"liquidity_net", 0x13, func(v *uniswapV4TickValues, n *big.Int) { v.liquidityNet = n }},
		{"fee_growth_outside0_x128", 0x14, func(v *uniswapV4TickValues, n *big.Int) { v.feeGrowthOutside0X128 = n }},
		{"fee_growth_outside1_x128", 0x15, func(v *uniswapV4TickValues, n *big.Int) { v.feeGrowthOutside1X128 = n }},
	} {
		t.Run(tc.column, func(t *testing.T) {
			f := newUniswapV4TickFixture(t, ctx, tc.discriminator)
			const tick = 60

			f.save(newUniswapV4TestTickWithValues(f.poolID, tick, 5000, 0, defaultUniswapV4TickValues()))

			changed := defaultUniswapV4TickValues()
			changedValue := big.NewInt(999)
			tc.set(&changed, changedValue)
			f.save(newUniswapV4TestTickWithValues(f.poolID, tick, 5002, 0, changed))

			if got := f.rowCount(tick); got != 2 {
				t.Fatalf("row count = %d, want 2", got)
			}
			if got := f.latestValue(tick, tc.column); got != changedValue.String() {
				t.Errorf("latest %s = %q, want %q", tc.column, got, changedValue)
			}
		})
	}
}

func TestUniswapV4Repository_WriteTicks_ReorgReobservationAppends(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x0b)

	f.save(newUniswapV4TestTick(f.poolID, 60, 5002, 0, big.NewInt(100)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5002, 1, big.NewInt(100)))

	if got := f.rowCount(60); got != 2 {
		t.Fatalf("row count = %d, want 2 (a reorg re-observation appends even with identical values)", got)
	}
}

// TestUniswapV4Repository_WriteTicks_SameBlockRedeliveryDoesNotAppend pins the
// case the block_version gate must still skip: an at-least-once redelivery of
// the same block at the same version.
func TestUniswapV4Repository_WriteTicks_SameBlockRedeliveryDoesNotAppend(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x17)

	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))

	if got := f.rowCount(60); got != 1 {
		t.Fatalf("row count = %d, want 1 (a redelivery of one block at one version must not append)", got)
	}
}

// TestUniswapV4Repository_WriteTicks_LaterIdenticalTouchAfterReorgDoesNotAppend
// pins that block_version is only comparable within one height: once a reorg
// has written (N, v1), the next touch at N+k carries v0, and treating that as a
// re-observation appends a row claiming a change the chain never made.
func TestUniswapV4Repository_WriteTicks_LaterIdenticalTouchAfterReorgDoesNotAppend(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x18)

	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 1, big.NewInt(100)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5001, 0, big.NewInt(100)))

	if got := f.rowCount(60); got != 2 {
		t.Fatalf("row count = %d, want 2 (a later unchanged touch must not append because a prior height was reorged)", got)
	}
}

// TestUniswapV4Repository_WriteTicks_BackfilledGapBlockAppendsBelowNewerRow
// pins the height bound: the gap backfiller republishes a missed block at
// block_version 0, under a row a later touch already wrote, and the
// append-on-change decision has to be made against the tick's state at that
// height or the missed block leaves a permanent hole.
func TestUniswapV4Repository_WriteTicks_BackfilledGapBlockAppendsBelowNewerRow(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x19)

	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5002, 0, big.NewInt(200)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5001, 0, big.NewInt(200)))

	if got := f.rowCount(60); got != 3 {
		t.Fatalf("row count = %d, want 3 (the backfilled block's own change must be recorded, not swallowed by the newer row)", got)
	}
}

// TestUniswapV4Repository_WriteTicks_MixedBatchSkipsOnlyUnchanged pins that the
// append-on-change decision is per tick, not per batch: one SaveBlock carrying
// an unchanged, a changed and a brand-new tick must insert exactly two rows.
func TestUniswapV4Repository_WriteTicks_MixedBatchSkipsOnlyUnchanged(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x0c)

	f.save(
		newUniswapV4TestTick(f.poolID, 10, 5000, 0, big.NewInt(1)),
		newUniswapV4TestTick(f.poolID, 20, 5000, 0, big.NewInt(2)),
	)

	changed := newUniswapV4TestTick(f.poolID, 20, 5001, 0, big.NewInt(2))
	changed.LiquidityGross = big.NewInt(777)
	f.save(
		newUniswapV4TestTick(f.poolID, 10, 5001, 0, big.NewInt(1)),
		changed,
		newUniswapV4TestTick(f.poolID, 30, 5001, 0, big.NewInt(3)),
	)

	for _, tc := range []struct {
		tick int
		want int
	}{
		{tick: 10, want: 1},
		{tick: 20, want: 2},
		{tick: 30, want: 1},
	} {
		if got := f.rowCount(tc.tick); got != tc.want {
			t.Errorf("tick %d row count = %d, want %d", tc.tick, got, tc.want)
		}
	}
}

// TestUniswapV4Repository_WriteTicks_RoundTripsExtremeNumerics pins the NUMERIC
// columns against the widths v4-core really produces: liquidityNet is a signed
// int128 and feeGrowthOutside is a full uint256.
func TestUniswapV4Repository_WriteTicks_RoundTripsExtremeNumerics(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x0d)

	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	minInt128 := new(big.Int).Neg(new(big.Int).Lsh(big.NewInt(1), 127))

	tick := newUniswapV4TestTick(f.poolID, 120, 5010, 0, minInt128)
	tick.FeeGrowthOutside0X128 = maxUint256
	tick.FeeGrowthOutside1X128 = maxUint256
	f.save(tick)

	var liquidityNet, feeGrowth0, feeGrowth1 string
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT liquidity_net::text, fee_growth_outside0_x128::text, fee_growth_outside1_x128::text
		 FROM uniswap_v4_tick WHERE pool_id=$1 AND tick=120`,
		f.poolID,
	).Scan(&liquidityNet, &feeGrowth0, &feeGrowth1); err != nil {
		t.Fatalf("read back tick: %v", err)
	}
	if liquidityNet != minInt128.String() {
		t.Errorf("liquidity_net = %q, want %q", liquidityNet, minInt128)
	}
	if feeGrowth0 != maxUint256.String() || feeGrowth1 != maxUint256.String() {
		t.Errorf("fee growth = (%q, %q), want %q", feeGrowth0, feeGrowth1, maxUint256)
	}
}

func TestUniswapV4Repository_WriteTicks_TwoPoolsInOneCall(t *testing.T) {
	ctx := context.Background()
	poolA := seedUniswapV4RepoTestPool(t, ctx, 0x05)
	poolB := seedUniswapV4RepoTestPool(t, ctx, 0x06)

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Ticks: []*entity.UniswapV4Tick{
			newUniswapV4TestTick(poolB, 240, 5100, 0, big.NewInt(1)),
			newUniswapV4TestTick(poolA, -240, 5100, 0, big.NewInt(2)),
			newUniswapV4TestTick(poolB, -120, 5100, 0, big.NewInt(3)),
			newUniswapV4TestTick(poolA, 120, 5100, 0, big.NewInt(4)),
		}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	for _, tc := range []struct {
		name   string
		poolID int64
		want   []int32
	}{
		{"pool_a", poolA, []int32{-240, 120}},
		{"pool_b", poolB, []int32{-120, 240}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := repo.TicksForPoolAtBlock(ctx, tc.poolID, 5100)
			if err != nil {
				t.Fatalf("TicksForPoolAtBlock: %v", err)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("ticks = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestUniswapV4Repository_TicksForPoolAtBlock_ReturnsDistinctTicksAscending(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x07)

	repo := newUniswapV4Repo(t)
	saveTicks := func(ticks ...*entity.UniswapV4Tick) {
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Ticks: ticks}); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
	}

	const targetBlock = int64(5200)
	saveTicks(
		newUniswapV4TestTick(poolID, 300, targetBlock, 0, big.NewInt(1)),
		newUniswapV4TestTick(poolID, -60, targetBlock, 0, big.NewInt(2)),
	)
	// A second version of one tick at the same block must be deduplicated.
	saveTicks(newUniswapV4TestTick(poolID, -60, targetBlock, 1, big.NewInt(3)))
	// A tick at a different block must not appear.
	saveTicks(newUniswapV4TestTick(poolID, 900, targetBlock+1, 0, big.NewInt(4)))

	got, err := repo.TicksForPoolAtBlock(ctx, poolID, targetBlock)
	if err != nil {
		t.Fatalf("TicksForPoolAtBlock: %v", err)
	}
	if want := []int32{-60, 300}; !slices.Equal(got, want) {
		t.Errorf("ticks = %v, want %v", got, want)
	}
}

func TestUniswapV4Repository_TicksForPoolAtBlock_UnknownBlockIsEmpty(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x08)

	repo := newUniswapV4Repo(t)
	got, err := repo.TicksForPoolAtBlock(ctx, poolID, 5300)
	if err != nil {
		t.Fatalf("TicksForPoolAtBlock: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ticks = %v, want none", got)
	}
}

// TestUniswapV4Repository_LoadPools_RejectsNativeCurrencyMappedToZeroSentinel
// pins the one currency mapping that is silently plausible: address(0) already
// exists in token as a 0-decimals "no token" row, so accepting it would scale
// every native-ETH amount by 10^0.
func TestUniswapV4Repository_LoadPools_RejectsNativeCurrencyMappedToZeroSentinel(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoNativeChainID))

	native := common.Address{}
	currency1 := common.HexToAddress("0x00000000000000000000000000000000000000e1")
	zeroDecimals := 0
	decimals := 18
	sentinelTokenID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoNativeChainID, native, "", &zeroDecimals)
	token1ID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoNativeChainID, currency1, "TK1", &decimals)

	poolID := seedUniswapV4RepoPool(t, ctx, uniswapV4RepoPoolFixture{
		chainID:          uniswapV4RepoNativeChainID,
		poolID:           common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000e7000"),
		currency0:        native,
		currency1:        currency1,
		currency0TokenID: sentinelTokenID,
		currency1TokenID: token1ID,
		fee:              100,
		tickSpacing:      1,
	})

	repo := newUniswapV4Repo(t)
	_, err := repo.LoadPools(ctx, uniswapV4RepoNativeChainID)
	if err == nil {
		t.Fatalf("LoadPools with native ETH mapped to the address(0) sentinel on pool id=%d: want error, got nil", poolID)
	}
	if !containsPoolID(err.Error(), poolID) {
		t.Errorf("error %q does not name the offending pool id %d", err, poolID)
	}
}

// TestUniswapV4Repository_LoadPools_CarriesTheSnapshotGate keeps an excluded
// pool in the result: dropping it here would stop its event indexing too, and
// the service is what decides to skip only its snapshots.
func TestUniswapV4Repository_LoadPools_CarriesTheSnapshotGate(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoUnsupportedChainID))

	supported := newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoUnsupportedChainID, 0x41)
	supportedID := seedUniswapV4RepoPool(t, ctx, supported)

	excluded := newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoUnsupportedChainID, 0x42)
	excluded.excludeFromSnapshots = true
	excludedID := seedUniswapV4RepoPool(t, ctx, excluded)

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoUnsupportedChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}

	got := make(map[int64]bool, len(pools))
	for _, p := range pools {
		got[p.ID] = p.SnapshotSupported
	}
	if len(pools) != 2 {
		t.Fatalf("LoadPools returned %d pools, want 2 (an excluded pool is still registered)", len(pools))
	}
	if !got[supportedID] {
		t.Errorf("pool %d SnapshotSupported = false, want true", supportedID)
	}
	if got[excludedID] {
		t.Errorf("pool %d SnapshotSupported = true, want false", excludedID)
	}
}

// TestUniswapV4Repository_LoadPools_RejectsCrossChainCurrencyToken pins the
// port contract against the join that used to enforce it: a currency_token_id
// pointing at another chain's token row must name the offending pool, never
// drop it from the result and leave the indexer running one pool short.
func TestUniswapV4Repository_LoadPools_RejectsCrossChainCurrencyToken(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoXChainTokenChainID))

	currency0 := common.HexToAddress("0x00000000000000000000000000000000000000c1")
	currency1 := common.HexToAddress("0x00000000000000000000000000000000000000c2")
	decimals := 18
	foreignToken0ID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoXChainDonorChainID, currency0, "TK0", &decimals)
	token1ID := seedUniswapV4RepoToken(t, ctx, uniswapV4RepoXChainTokenChainID, currency1, "TK1", &decimals)

	poolID := seedUniswapV4RepoPool(t, ctx, uniswapV4RepoPoolFixture{
		chainID:          uniswapV4RepoXChainTokenChainID,
		poolID:           common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000c8a1a1"),
		currency0:        currency0,
		currency1:        currency1,
		currency0TokenID: foreignToken0ID,
		currency1TokenID: token1ID,
		fee:              500,
		tickSpacing:      10,
	})

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoXChainTokenChainID)
	if err == nil {
		t.Fatalf("LoadPools with pool id=%d whose currency0 token lives on another chain: want error, got %d pools", poolID, len(pools))
	}
	if !containsPoolID(err.Error(), poolID) {
		t.Errorf("error %q does not name the offending pool id %d", err, poolID)
	}
}

// TestUniswapV4Repository_LoadPools_RejectsCrossChainPoolManagerProtocol pins
// the chain predicate on the PoolManager join. uniswap_v4_pool_manager.protocol_id
// is a surrogate-id FK with nothing tying it to the row's own chain, so an
// unscoped join hands back another chain's PoolManager address: state_view stays
// right, the pod boots clean, and every log is then dropped by the address filter
// with no error and no metric while all five fact tables stay empty.
func TestUniswapV4Repository_LoadPools_RejectsCrossChainPoolManagerProtocol(t *testing.T) {
	ctx := context.Background()

	manager := newUniswapV4RepoManagerFixture(uniswapV4RepoXChainMgrChainID)
	manager.protocolChainID = uniswapV4RepoXChainMgrDonorChID
	seedUniswapV4RepoPoolManager(t, ctx, manager)
	poolID := seedUniswapV4RepoPool(t, ctx,
		newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoXChainMgrChainID, 0x41))

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoXChainMgrChainID)
	if err == nil {
		t.Fatalf("LoadPools with pool id=%d whose pool manager protocol row lives on another chain: want error, got %d pools", poolID, len(pools))
	}
	if !strings.Contains(err.Error(), "uniswap_v4_pool_manager") {
		t.Errorf("error %q does not name the offending uniswap_v4_pool_manager row", err)
	}
}

// TestUniswapV4Repository_LoadPools_ExcludesOtherChains seeds two chains at
// once: the chain filter must reach the pools, the PoolManager and the token
// join alike.
func TestUniswapV4Repository_LoadPools_ExcludesOtherChains(t *testing.T) {
	ctx := context.Background()

	home := newUniswapV4RepoManagerFixture(uniswapV4RepoHomeChainID)
	seedUniswapV4RepoPoolManager(t, ctx, home)
	homePool := newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoHomeChainID, 0x31)
	homePoolID := seedUniswapV4RepoPool(t, ctx, homePool)

	neighbour := newUniswapV4RepoManagerFixture(uniswapV4RepoNeighbourChainID)
	neighbour.manager = common.HexToAddress("0x000000000000000000000000000000000000beef")
	neighbour.stateView = common.HexToAddress("0x000000000000000000000000000000000000cafe")
	seedUniswapV4RepoPoolManager(t, ctx, neighbour)
	seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoNeighbourChainID, 0x32))

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoHomeChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("LoadPools returned %d pools, want 1 (the neighbouring chain must not leak in)", len(pools))
	}
	if pools[0].ID != homePoolID {
		t.Errorf("ID = %d, want %d", pools[0].ID, homePoolID)
	}
	if pools[0].PoolManager != home.manager {
		t.Errorf("PoolManager = %s, want %s (the neighbouring chain's manager must not win)", pools[0].PoolManager, home.manager)
	}
	if pools[0].StateView != home.stateView {
		t.Errorf("StateView = %s, want %s", pools[0].StateView, home.stateView)
	}
}

// TestUniswapV4Repository_SaveBlock_RoundTripsProtocolFeeUpdatedPoolEvent binds
// the entity's event-name constant to the column CHECK: a rename on either side
// must fail here rather than at the first real ProtocolFeeUpdated log.
func TestUniswapV4Repository_SaveBlock_RoundTripsProtocolFeeUpdatedPoolEvent(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x0f)

	const blockNumber = int64(21801000)
	event := &entity.UniswapV4PoolEvent{
		PoolID:         poolID,
		BlockNumber:    blockNumber,
		BlockVersion:   0,
		BlockTimestamp: time.Unix(1740001000, 0).UTC(),
		TxHash:         common.HexToHash("0xfe00ccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccddaabbccdd"),
		LogIndex:       9,
		EventName:      entity.UniswapV4PoolEventProtocolFeeUpdated,
		Params:         json.RawMessage(`{"protocolFee":"1000"}`),
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			PoolEvents: []*entity.UniswapV4PoolEvent{event},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	var eventName string
	var params []byte
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT event_name, params FROM uniswap_v4_pool_event WHERE pool_id=$1 AND block_number=$2`,
		poolID, blockNumber,
	).Scan(&eventName, &params); err != nil {
		t.Fatalf("read back pool event: %v", err)
	}
	if eventName != string(entity.UniswapV4PoolEventProtocolFeeUpdated) {
		t.Errorf("event_name = %q, want %q", eventName, entity.UniswapV4PoolEventProtocolFeeUpdated)
	}
	var got map[string]any
	if err := json.Unmarshal(params, &got); err != nil {
		t.Fatalf("unmarshal params: %v", err)
	}
	if got["protocolFee"] != "1000" {
		t.Errorf("params = %v, want protocolFee=1000", got)
	}
}

// TestUniswapV4Repository_SaveBlock_NilNumericWritesNothing pins that a
// conversion failure discovered after the first statements are queued still
// leaves the block unwritten: the error must reach the caller so its
// transaction rolls back, never a half-persisted block.
func TestUniswapV4Repository_SaveBlock_NilNumericWritesNothing(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x0e)

	const blockNumber = int64(21802000)
	broken := newUniswapV4TestTick(poolID, 60, blockNumber, 0, big.NewInt(1))
	broken.LiquidityGross = nil

	repo := newUniswapV4Repo(t)
	withUniswapV4RollbackTx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{newUniswapV4TestState(poolID, blockNumber, 0, 5)},
			Ticks:  []*entity.UniswapV4Tick{broken},
		}); err == nil {
			t.Fatal("SaveBlock with a nil liquidity_gross: want error, got nil")
		}
	})

	for _, tc := range []struct{ table, column string }{
		{"uniswap_v4_pool_state", "block_number"},
		{"uniswap_v4_tick", "block_number"},
	} {
		var count int
		if err := uniswapV4TestPool.QueryRow(ctx,
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE pool_id=$1 AND %s=$2`, tc.table, tc.column),
			poolID, blockNumber,
		).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", tc.table, err)
		}
		if count != 0 {
			t.Errorf("%s has %d rows after a rolled-back SaveBlock, want 0", tc.table, count)
		}
	}
}

// TestUniswapV4Repository_PoolIDsWithStateAtBlock_ReturnsDistinctPoolsAscending
// covers the reorg due-set union: only pools with a state row at exactly that
// height, once each, ascending.
func TestUniswapV4Repository_PoolIDsWithStateAtBlock_ReturnsDistinctPoolsAscending(t *testing.T) {
	ctx := context.Background()
	poolA := seedUniswapV4RepoTestPool(t, ctx, 0x12)
	poolB := seedUniswapV4RepoTestPool(t, ctx, 0x10)
	poolElsewhere := seedUniswapV4RepoTestPool(t, ctx, 0x11)

	const targetBlock = int64(7100000)
	repo := newUniswapV4Repo(t)
	rebuilt := NewUniswapV4Repository(uniswapV4TestPool, testUniswapV4RebuildID)
	for _, tc := range []struct {
		repo   *UniswapV4Repository
		poolID int64
		block  int64
	}{
		{repo, poolB, targetBlock},
		{repo, poolA, targetBlock},
		{rebuilt, poolA, targetBlock}, // a second processing_version at the same height
		{repo, poolElsewhere, targetBlock + 1},
	} {
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := tc.repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
				States: []*entity.UniswapV4PoolState{newUniswapV4TestState(tc.poolID, tc.block, 0, 11)},
			}); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
	}

	got, err := repo.PoolIDsWithStateAtBlock(ctx, uniswapV4RepoSaveChainID, targetBlock, uniswapV4TestBlockTime(targetBlock))
	if err != nil {
		t.Fatalf("PoolIDsWithStateAtBlock: %v", err)
	}
	want := []int64{poolA, poolB}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("pool ids = %v, want %v", got, want)
	}
}

// A worker serves one chain, and the fact table carries no chain_id of its own,
// so the scope has to come from the registry join; a neighbouring chain's pool
// at the same height would look like a registry bug to the caller.
func TestUniswapV4Repository_PoolIDsWithStateAtBlock_ExcludesOtherChains(t *testing.T) {
	ctx := context.Background()
	homePool := seedUniswapV4RepoTestPool(t, ctx, 0x13)

	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoPriorStateChainID))
	neighbourPool := seedUniswapV4RepoPool(t, ctx,
		newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoPriorStateChainID, 0x14))

	const targetBlock = int64(7110000)
	repo := newUniswapV4Repo(t)
	for _, poolID := range []int64{homePool, neighbourPool} {
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
				States: []*entity.UniswapV4PoolState{newUniswapV4TestState(poolID, targetBlock, 0, 11)},
			}); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
	}

	got, err := repo.PoolIDsWithStateAtBlock(ctx, uniswapV4RepoSaveChainID, targetBlock, uniswapV4TestBlockTime(targetBlock))
	if err != nil {
		t.Fatalf("PoolIDsWithStateAtBlock: %v", err)
	}
	if !slices.Equal(got, []int64{homePool}) {
		t.Errorf("pool ids = %v, want %v (the neighbouring chain's pool %d must not leak in)", got, []int64{homePool}, neighbourPool)
	}
}

// A registry correction mints a new surrogate id while the fact rows keep the
// old one, so the id handed back must be the CURRENT version for the natural
// key: the caller's in-memory registry only knows current ids.
func TestUniswapV4Repository_PoolIDsWithStateAtBlock_ResolvesSupersededPoolForward(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoSupersededChainID))

	fixture := newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoSupersededChainID, 0x15)
	supersededID := seedUniswapV4RepoPool(t, ctx, fixture)

	const targetBlock = int64(7120000)
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{newUniswapV4TestState(supersededID, targetBlock, 0, 11)},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	fixture.buildID = 1
	fixture.deployBlock = 2
	currentID := seedUniswapV4RepoPool(t, ctx, fixture)
	if currentID == supersededID {
		t.Fatalf("the corrected pool reused id %d; the fixture did not append a new version", currentID)
	}

	got, err := repo.PoolIDsWithStateAtBlock(ctx, uniswapV4RepoSupersededChainID, targetBlock, uniswapV4TestBlockTime(targetBlock))
	if err != nil {
		t.Fatalf("PoolIDsWithStateAtBlock: %v", err)
	}
	if !slices.Equal(got, []int64{currentID}) {
		t.Errorf("pool ids = %v, want %v (the superseded %d must resolve forward)", got, []int64{currentID}, supersededID)
	}
}

// Without a block_timestamp predicate the reorg read scans every chunk of the
// hypertable (VEC-541), so the bound has to be live rather than assumed.
func TestUniswapV4Repository_PoolIDsWithStateAtBlock_BoundsTheScanToTheBlockTimestamp(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x16)

	const targetBlock = int64(7130000)
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{newUniswapV4TestState(poolID, targetBlock, 0, 11)},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	farAway := uniswapV4TestBlockTime(targetBlock).AddDate(0, 0, 10)
	got, err := repo.PoolIDsWithStateAtBlock(ctx, uniswapV4RepoSaveChainID, targetBlock, farAway)
	if err != nil {
		t.Fatalf("PoolIDsWithStateAtBlock: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("pool ids = %v, want none: the scan must be bounded to the chunks around the block timestamp", got)
	}
}

func TestUniswapV4Repository_PoolIDsWithStateAtBlock_UnknownBlockIsEmpty(t *testing.T) {
	ctx := context.Background()

	repo := newUniswapV4Repo(t)
	got, err := repo.PoolIDsWithStateAtBlock(ctx, uniswapV4RepoSaveChainID, 7199999, uniswapV4TestBlockTime(7199999))
	if err != nil {
		t.Fatalf("PoolIDsWithStateAtBlock: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("pool ids = %v, want none", got)
	}
}

// uniswapV4TestBlockTime is the block_timestamp every fixture row at a height
// carries; PoolIDsWithStateAtBlock bounds its chunk scan around it.
func uniswapV4TestBlockTime(blockNumber int64) time.Time {
	return time.Unix(1740000000+blockNumber, 0).UTC()
}

func newUniswapV4TestState(poolID int64, blockNumber int64, blockVersion int, tick int) *entity.UniswapV4PoolState {
	return &entity.UniswapV4PoolState{
		PoolID:               poolID,
		BlockNumber:          blockNumber,
		BlockVersion:         blockVersion,
		BlockTimestamp:       uniswapV4TestBlockTime(blockNumber),
		SqrtPriceX96:         big.NewInt(1),
		Tick:                 tick,
		ProtocolFee:          0,
		LpFee:                3000,
		Liquidity:            big.NewInt(1),
		FeeGrowthGlobal0X128: big.NewInt(1),
		FeeGrowthGlobal1X128: big.NewInt(1),
	}
}

// newUniswapV4TestBlockWrites builds one validated row for each of the four
// fact hypertables, sharing every key but blockVersion so two calls differ
// exactly as an original and its reorg re-observation do.
func newUniswapV4TestBlockWrites(t *testing.T, poolID int64, blockNumber int64, blockVersion int) outbound.UniswapV4BlockWrites {
	t.Helper()
	blockTimestamp := time.Unix(1740000000+blockNumber, 0).UTC()
	txHash := common.HexToHash("0x11ccddeeff00112233445566778899aabbccddeeff00112233445566778899aa")
	sender := common.HexToAddress("0x66a9893cc07d91d95644aedd05d03f95e1dba8af")

	state := newUniswapV4TestState(poolID, blockNumber, blockVersion, 7)
	swap := &entity.UniswapV4Swap{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: blockVersion, BlockTimestamp: blockTimestamp,
		TxHash: txHash, LogIndex: 1, Sender: sender,
		Amount0: big.NewInt(-100), Amount1: big.NewInt(100),
		SqrtPriceX96: big.NewInt(1), Liquidity: big.NewInt(1), Tick: 0, Fee: 3000,
	}
	liquidityEvent := &entity.UniswapV4LiquidityEvent{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: blockVersion, BlockTimestamp: blockTimestamp,
		TxHash: txHash, LogIndex: 2, Sender: sender,
		TickLower: -60, TickUpper: 60,
		LiquidityDelta: big.NewInt(1000),
		Salt:           common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ff"),
	}
	poolEvent := &entity.UniswapV4PoolEvent{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: blockVersion, BlockTimestamp: blockTimestamp,
		TxHash: txHash, LogIndex: 3,
		EventName: entity.UniswapV4PoolEventInitialize,
		Params:    json.RawMessage(`{"sqrtPriceX96":"1000","tick":10}`),
	}

	for _, v := range []interface{ Validate() error }{state, swap, liquidityEvent, poolEvent} {
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	}

	return outbound.UniswapV4BlockWrites{
		States:          []*entity.UniswapV4PoolState{state},
		Swaps:           []*entity.UniswapV4Swap{swap},
		LiquidityEvents: []*entity.UniswapV4LiquidityEvent{liquidityEvent},
		PoolEvents:      []*entity.UniswapV4PoolEvent{poolEvent},
	}
}

// uniswapV4TickValues holds a tick row's four append-on-change value columns,
// so a test can vary one of them without restating the other three.
type uniswapV4TickValues struct {
	liquidityGross        *big.Int
	liquidityNet          *big.Int
	feeGrowthOutside0X128 *big.Int
	feeGrowthOutside1X128 *big.Int
}

// defaultUniswapV4TickValues are four distinct values, so a case that mutates
// one column cannot pass on another column's value.
func defaultUniswapV4TickValues() uniswapV4TickValues {
	return uniswapV4TickValues{
		liquidityGross:        big.NewInt(1000),
		liquidityNet:          big.NewInt(1),
		feeGrowthOutside0X128: big.NewInt(2),
		feeGrowthOutside1X128: big.NewInt(3),
	}
}

func newUniswapV4TestTick(poolID int64, tick int, blockNumber int64, blockVersion int, liquidityNet *big.Int) *entity.UniswapV4Tick {
	values := defaultUniswapV4TickValues()
	values.liquidityNet = liquidityNet
	return newUniswapV4TestTickWithValues(poolID, tick, blockNumber, blockVersion, values)
}

func newUniswapV4TestTickWithValues(poolID int64, tick int, blockNumber int64, blockVersion int, values uniswapV4TickValues) *entity.UniswapV4Tick {
	return &entity.UniswapV4Tick{
		PoolID:                poolID,
		Tick:                  tick,
		BlockNumber:           blockNumber,
		BlockVersion:          blockVersion,
		BlockTimestamp:        time.Unix(1740000000+blockNumber, 0).UTC(),
		LiquidityGross:        values.liquidityGross,
		LiquidityNet:          values.liquidityNet,
		FeeGrowthOutside0X128: values.feeGrowthOutside0X128,
		FeeGrowthOutside1X128: values.feeGrowthOutside1X128,
	}
}

// containsPoolID reports whether msg names poolID, so an error assertion can
// require the offending registry row to be identified.
func containsPoolID(msg string, poolID int64) bool {
	return strings.Contains(msg, strconv.FormatInt(poolID, 10))
}

// seedUniswapV4EverIndexedPool builds a registry pool on chainID (with that
// chain's PoolManager) for the ever-snapshotted reads, which are chain-scoped
// and unbounded in block range and so need a chain of their own per scenario.
func seedUniswapV4EverIndexedPool(t *testing.T, ctx context.Context, chainID int, discriminator byte) int64 {
	t.Helper()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainID))
	return seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainID, discriminator))
}

// Both tables count, and neither alone is enough: a pool with no initialized
// ticks writes a state row and no tick rows, while the baseline enumeration of
// a pool can write tick rows the state snapshot's own row does not distinguish.
func TestUniswapV4Repository_PoolIDsEverSnapshotted_ReturnsPoolsWithStateOrTickRows(t *testing.T) {
	ctx := context.Background()
	const chainID = uniswapV4RepoEverIndexedChainID
	statePool := seedUniswapV4EverIndexedPool(t, ctx, chainID, 0x17)
	tickPool := seedUniswapV4EverIndexedPool(t, ctx, chainID, 0x18)
	untouchedPool := seedUniswapV4EverIndexedPool(t, ctx, chainID, 0x19)

	const block = int64(7140000)
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{newUniswapV4TestState(statePool, block, 0, 11)},
			Ticks:  []*entity.UniswapV4Tick{newUniswapV4TestTick(tickPool, 60, block, 0, big.NewInt(1))},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	got, err := repo.PoolIDsEverSnapshotted(ctx, chainID)
	if err != nil {
		t.Fatalf("PoolIDsEverSnapshotted: %v", err)
	}
	want := []int64{statePool, tickPool}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("pool ids = %v, want %v (pool %d has produced no row at all)", got, want, untouchedPool)
	}
}

// A registry correction mints a new surrogate id while the fact rows keep the
// old one; the caller only knows current ids, so a pool that WAS indexed under a
// superseded version must not read back as never indexed.
func TestUniswapV4Repository_PoolIDsEverSnapshotted_ResolvesSupersededPoolForward(t *testing.T) {
	ctx := context.Background()
	const chainID = uniswapV4RepoEverIndexedFwdChID
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainID))

	fixture := newUniswapV4RepoPoolFixture(t, ctx, chainID, 0x1a)
	supersededID := seedUniswapV4RepoPool(t, ctx, fixture)

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{newUniswapV4TestState(supersededID, 7150000, 0, 11)},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	fixture.buildID = 1
	fixture.deployBlock = 2
	currentID := seedUniswapV4RepoPool(t, ctx, fixture)
	if currentID == supersededID {
		t.Fatalf("the corrected pool reused id %d; the fixture did not append a new version", currentID)
	}

	got, err := repo.PoolIDsEverSnapshotted(ctx, chainID)
	if err != nil {
		t.Fatalf("PoolIDsEverSnapshotted: %v", err)
	}
	if !slices.Equal(got, []int64{currentID}) {
		t.Errorf("pool ids = %v, want %v (the superseded %d must resolve forward)", got, []int64{currentID}, supersededID)
	}
}

// The fact tables carry no chain_id, so the scope comes from the registry join;
// a neighbouring chain's indexed pool would otherwise mask this chain's own
// never-indexed one.
func TestUniswapV4Repository_PoolIDsEverSnapshotted_ExcludesOtherChains(t *testing.T) {
	ctx := context.Background()
	homePool := seedUniswapV4EverIndexedPool(t, ctx, uniswapV4RepoEverIndexedNbrChID, 0x1b)
	neighbourPool := seedUniswapV4EverIndexedPool(t, ctx, uniswapV4RepoEverIndexedFgnChID, 0x1c)

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{newUniswapV4TestState(neighbourPool, 7160000, 0, 11)},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	got, err := repo.PoolIDsEverSnapshotted(ctx, uniswapV4RepoEverIndexedNbrChID)
	if err != nil {
		t.Fatalf("PoolIDsEverSnapshotted: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("pool ids = %v, want none: pool %d has no rows and %d belongs to another chain", got, homePool, neighbourPool)
	}
}
