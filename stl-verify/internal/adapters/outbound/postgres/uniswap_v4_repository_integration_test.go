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
	uniswapV4RepoSaveChainID       = 490001
	uniswapV4RepoNullDecChainID    = 490002
	uniswapV4RepoMismatchChainID   = 490003
	uniswapV4RepoEmptyChainID      = 490004
	uniswapV4RepoNoManagerChainID  = 490005
	uniswapV4RepoPoolVerChainID    = 490006
	uniswapV4RepoManagerVerChainID = 490007
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
	chainID     int
	manager     common.Address
	stateView   common.Address
	deployBlock int64
	buildID     int
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
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block)
		 VALUES ($1, $2, 'UniswapV4', 'dex', $3)
		 ON CONFLICT (chain_id, address) DO NOTHING`,
		f.chainID, f.manager.Bytes(), f.deployBlock,
	); err != nil {
		t.Fatalf("seed protocol on chain %d: %v", f.chainID, err)
	}
	var protocolID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE chain_id = $1 AND address = $2`,
		f.chainID, f.manager.Bytes(),
	).Scan(&protocolID); err != nil {
		t.Fatalf("read back protocol on chain %d: %v", f.chainID, err)
	}

	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO uniswap_v4_pool_manager
		    (chain_id, protocol_id, pool_manager_address, state_view_address, deploy_block, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (chain_id, processing_version) DO NOTHING`,
		f.chainID, protocolID, f.manager.Bytes(), f.stateView.Bytes(), f.deployBlock, f.buildID,
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
		     deploy_block, build_id)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT (chain_id, pool_id, processing_version) DO NOTHING`,
		f.chainID, f.poolID.Bytes(), f.currency0.Bytes(), f.currency1.Bytes(),
		f.currency0TokenID, f.currency1TokenID, f.fee, f.tickSpacing,
		f.hooks.Bytes(), f.deployBlock, f.buildID,
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

// truncateUniswapV4FactTables clears all fact rows so each test starts clean.
// The registry tables are left alone: they are shared with sibling test files
// through the public schema.
func truncateUniswapV4FactTables(t *testing.T, ctx context.Context) {
	t.Helper()
	for _, table := range []string{
		"uniswap_v4_pool_state",
		"uniswap_v4_swap",
		"uniswap_v4_liquidity_event",
		"uniswap_v4_tick",
		"uniswap_v4_pool_event",
	} {
		if _, err := uniswapV4TestPool.Exec(ctx, "DELETE FROM "+table); err != nil {
			t.Fatalf("truncate %s: %v", table, err)
		}
	}
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
		if _, dup := byPoolID[p.PoolID]; dup {
			t.Fatalf("LoadPools returned pool_id %s twice", p.PoolID)
		}
		byPoolID[p.PoolID] = p
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
		if p.PoolID == fixture.poolID {
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
	truncateUniswapV4FactTables(t, ctx)
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
	truncateUniswapV4FactTables(t, ctx)
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

func TestUniswapV4Repository_SaveBlock_NewBuildBumpsProcessingVersion(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV4FactTables(t, ctx)
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

func TestUniswapV4Repository_WriteTicks_AppendOnChange(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV4FactTables(t, ctx)
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x04)

	repo := newUniswapV4Repo(t)
	saveTicks := func(ticks ...*entity.UniswapV4Tick) {
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Ticks: ticks}); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
	}
	countTicks := func(tick int) int {
		var count int
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT count(*) FROM uniswap_v4_tick WHERE pool_id=$1 AND tick=$2`, poolID, tick,
		).Scan(&count); err != nil {
			t.Fatalf("count ticks: %v", err)
		}
		return count
	}

	const testTick = 60

	t.Run("first_write_inserts", func(t *testing.T) {
		saveTicks(newUniswapV4TestTick(poolID, testTick, 5000, 0, big.NewInt(100)))
		if got := countTicks(testTick); got != 1 {
			t.Fatalf("row count = %d, want 1", got)
		}
	})

	t.Run("identical_values_at_a_later_block_do_not_insert", func(t *testing.T) {
		saveTicks(newUniswapV4TestTick(poolID, testTick, 5001, 0, big.NewInt(100)))
		if got := countTicks(testTick); got != 1 {
			t.Fatalf("row count = %d, want 1 (unchanged values must not append)", got)
		}
	})

	t.Run("changed_liquidity_gross_inserts", func(t *testing.T) {
		changed := newUniswapV4TestTick(poolID, testTick, 5002, 0, big.NewInt(100))
		changed.LiquidityGross = big.NewInt(999)
		saveTicks(changed)
		if got := countTicks(testTick); got != 2 {
			t.Fatalf("row count = %d, want 2", got)
		}

		var latestGross string
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT liquidity_gross::text FROM uniswap_v4_tick WHERE pool_id=$1 AND tick=$2
			 ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1`,
			poolID, testTick,
		).Scan(&latestGross); err != nil {
			t.Fatalf("query latest tick: %v", err)
		}
		if latestGross != "999" {
			t.Errorf("latest liquidity_gross = %q, want 999", latestGross)
		}
	})

	t.Run("same_values_at_a_new_block_version_insert", func(t *testing.T) {
		reorged := newUniswapV4TestTick(poolID, testTick, 5002, 1, big.NewInt(100))
		reorged.LiquidityGross = big.NewInt(999)
		saveTicks(reorged)
		if got := countTicks(testTick); got != 3 {
			t.Fatalf("row count = %d, want 3 (a reorg re-observation always appends)", got)
		}
	})
}

func TestUniswapV4Repository_WriteTicks_TwoPoolsInOneCall(t *testing.T) {
	ctx := context.Background()
	truncateUniswapV4FactTables(t, ctx)
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
	truncateUniswapV4FactTables(t, ctx)
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
	truncateUniswapV4FactTables(t, ctx)
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

func newUniswapV4TestState(poolID int64, blockNumber int64, blockVersion int, tick int) *entity.UniswapV4PoolState {
	return &entity.UniswapV4PoolState{
		PoolID:               poolID,
		BlockNumber:          blockNumber,
		BlockVersion:         blockVersion,
		BlockTimestamp:       time.Unix(1740000000+blockNumber, 0).UTC(),
		SqrtPriceX96:         big.NewInt(1),
		Tick:                 tick,
		ProtocolFee:          0,
		LpFee:                3000,
		Liquidity:            big.NewInt(1),
		FeeGrowthGlobal0X128: big.NewInt(1),
		FeeGrowthGlobal1X128: big.NewInt(1),
	}
}

func newUniswapV4TestTick(poolID int64, tick int, blockNumber int64, blockVersion int, liquidityNet *big.Int) *entity.UniswapV4Tick {
	return &entity.UniswapV4Tick{
		PoolID:                poolID,
		Tick:                  tick,
		BlockNumber:           blockNumber,
		BlockVersion:          blockVersion,
		BlockTimestamp:        time.Unix(1740000000+blockNumber, 0).UTC(),
		LiquidityGross:        big.NewInt(1000),
		LiquidityNet:          liquidityNet,
		FeeGrowthOutside0X128: big.NewInt(1),
		FeeGrowthOutside1X128: big.NewInt(2),
	}
}

// containsPoolID reports whether msg names poolID, so an error assertion can
// require the offending registry row to be identified.
func containsPoolID(msg string, poolID int64) bool {
	return strings.Contains(msg, strconv.FormatInt(poolID, 10))
}
