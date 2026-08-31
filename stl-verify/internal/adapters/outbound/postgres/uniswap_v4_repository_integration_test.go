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

// One synthetic chain per scenario, so a deliberately-broken or multi-version
// registry can never leak into another test's LoadPools call.
const (
	uniswapV4RepoSaveChainID         = 490001
	uniswapV4RepoNullDecChainID      = 490002
	uniswapV4RepoMismatchChainID     = 490003
	uniswapV4RepoEmptyChainID        = 490004
	uniswapV4RepoNoManagerChainID    = 490005
	uniswapV4RepoPoolVerChainID      = 490006
	uniswapV4RepoManagerVerChainID   = 490007
	uniswapV4RepoNativeChainID       = 490008
	uniswapV4RepoHomeChainID         = 490009
	uniswapV4RepoNeighbourChainID    = 490010
	uniswapV4RepoXChainTokenChainID  = 490011
	uniswapV4RepoXChainDonorChainID  = 490012
	uniswapV4RepoUnsupportedChainID  = 490013
	uniswapV4RepoPriorStateChainID   = 490014
	uniswapV4RepoSupersededChainID   = 490015
	uniswapV4RepoEverIndexedChainID  = 490016
	uniswapV4RepoEverIndexedFwdChID  = 490017
	uniswapV4RepoEverIndexedNbrChID  = 490018
	uniswapV4RepoEverIndexedFgnChID  = 490019
	uniswapV4RepoXChainMgrChainID    = 490020
	uniswapV4RepoXChainMgrDonorChID  = 490021
	uniswapV4RepoXChainMgrVerChainID = 490022
	uniswapV4RepoNoPosmChainID       = 490023
	uniswapV4RepoPosmVerChainID      = 490024
	uniswapV4RepoXChainPosmChainID   = 490025
)

const (
	testUniswapV4BuildID   = buildregistry.BuildID(1)
	testUniswapV4RebuildID = buildregistry.BuildID(2)
)

func newUniswapV4Repo(t *testing.T) *UniswapV4Repository {
	t.Helper()
	return NewUniswapV4Repository(uniswapV4TestPool, testUniswapV4BuildID)
}

// Rollback is deferred so a t.Fatal mid-fn still releases the connection.
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

func withUniswapV4RollbackTx(t *testing.T, ctx context.Context, fn func(tx pgx.Tx)) {
	t.Helper()
	tx, err := uniswapV4TestPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)
	fn(tx)
}

func seedUniswapV4RepoChain(t *testing.T, ctx context.Context, chainID int) {
	t.Helper()
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES ($1, $2) ON CONFLICT (chain_id) DO NOTHING`,
		chainID, fmt.Sprintf("uniswap_v4_test_%d", chainID),
	); err != nil {
		t.Fatalf("seed chain %d: %v", chainID, err)
	}
}

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

// buildID drives the append: re-seeding under the same one is a no-op, a new one
// appends the next processing_version.
type uniswapV4RepoManagerFixture struct {
	chainID int
	// 0 means chainID, the only coherent registry; anything else seeds the
	// cross-chain PoolManager defect.
	protocolChainID int
	manager         common.Address
	stateView       common.Address
	deployBlock     int64
	buildID         int
	// positionManager is the chain's ERC-721 PositionManager. The zero address
	// seeds no uniswap_v4_position_manager row at all, which is the
	// missing-registry defect.
	positionManager common.Address
	// posmProtocolChainID is the posm protocol row's chain; 0 means chainID.
	posmProtocolChainID int
}

func newUniswapV4RepoManagerFixture(chainID int) uniswapV4RepoManagerFixture {
	return uniswapV4RepoManagerFixture{
		chainID:         chainID,
		manager:         common.HexToAddress("0x00000000000000000000000000000000000044c5"),
		stateView:       common.HexToAddress("0x0000000000000000000000000000000000007ffe"),
		deployBlock:     1,
		positionManager: common.HexToAddress("0x00000000000000000000000000000000000bd216"),
	}
}

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

	seedUniswapV4RepoPositionManager(t, ctx, f)
}

// seedUniswapV4RepoPositionManager upserts the posm protocol row plus one
// version of the chain's uniswap_v4_position_manager row, skipping both when the
// fixture leaves positionManager at the zero address.
func seedUniswapV4RepoPositionManager(t *testing.T, ctx context.Context, f uniswapV4RepoManagerFixture) {
	t.Helper()
	if f.positionManager == (common.Address{}) {
		return
	}
	protocolChainID := f.posmProtocolChainID
	if protocolChainID == 0 {
		protocolChainID = f.chainID
	}
	seedUniswapV4RepoChain(t, ctx, protocolChainID)
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, metadata)
		 VALUES ($1, $2, 'UniswapV4PositionManager', 'dex', $3, '{"role":"position_manager"}'::jsonb)
		 ON CONFLICT (chain_id, address) DO NOTHING`,
		protocolChainID, f.positionManager.Bytes(), f.deployBlock,
	); err != nil {
		t.Fatalf("seed posm protocol on chain %d: %v", protocolChainID, err)
	}
	var protocolID int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE chain_id = $1 AND address = $2`,
		protocolChainID, f.positionManager.Bytes(),
	).Scan(&protocolID); err != nil {
		t.Fatalf("read back posm protocol on chain %d: %v", protocolChainID, err)
	}
	if _, err := uniswapV4TestPool.Exec(ctx,
		`INSERT INTO uniswap_v4_position_manager (chain_id, protocol_id, deploy_block, build_id)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (chain_id, processing_version) DO NOTHING`,
		f.chainID, protocolID, f.deployBlock, f.buildID,
	); err != nil {
		t.Fatalf("seed position manager on chain %d: %v", f.chainID, err)
	}
}

// currentUniswapV4RepoPositionManagerID reads the surrogate id LoadPools must
// hand back for a chain: the highest-processing_version registry row.
func currentUniswapV4RepoPositionManagerID(t *testing.T, ctx context.Context, chainID int) int64 {
	t.Helper()
	var id int64
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT id FROM uniswap_v4_position_manager WHERE chain_id = $1
		 ORDER BY processing_version DESC LIMIT 1`, chainID).Scan(&id); err != nil {
		t.Fatalf("reading current uniswap_v4_position_manager on chain %d: %v", chainID, err)
	}
	return id
}

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
	// Negated: the column defaults to true, which every other fixture wants.
	excludeFromSnapshots bool
}

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

// discriminator gives every caller currencies and a PoolId of its own.
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

func seedUniswapV4RepoTestPool(t *testing.T, ctx context.Context, discriminator byte) int64 {
	t.Helper()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoSaveChainID))
	return seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoSaveChainID, discriminator))
}

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
	var stateRows outbound.StateRowCounts
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
	if stateRows.Persisted != 1 {
		t.Errorf("stateRows.Persisted = %d, want 1", stateRows.Persisted)
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
			gotTick, logIndex, fee, buildID        int
			gotTxHash, gotSender                   []byte
		)
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT tx_hash, log_index, sender, amount0::text, amount1::text,
			        sqrt_price_x96::text, liquidity::text, tick, fee, build_id
			 FROM uniswap_v4_swap WHERE pool_id=$1 AND block_number=$2`,
			poolID, blockNumber,
		).Scan(&gotTxHash, &logIndex, &gotSender, &amount0, &amount1,
			&sqrtPrice, &liquidity, &gotTick, &fee, &buildID); err != nil {
			t.Fatalf("read back swap: %v", err)
		}
		if buildID != int(testUniswapV4BuildID) {
			t.Errorf("build_id = %d, want %d (threaded from the constructor, not defaulted)", buildID, testUniswapV4BuildID)
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
			buildID                int
			gotSender, gotSalt     []byte
			gotTxHash              []byte
			gotBlockVersionOnRowIs int
		)
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT tx_hash, log_index, sender, tick_lower, tick_upper,
			        liquidity_delta::text, salt, block_version, build_id
			 FROM uniswap_v4_liquidity_event WHERE pool_id=$1 AND block_number=$2`,
			poolID, blockNumber,
		).Scan(&gotTxHash, &logIndex, &gotSender, &tickLower, &tickUpper,
			&liquidityDelta, &gotSalt, &gotBlockVersionOnRowIs, &buildID); err != nil {
			t.Fatalf("read back liquidity event: %v", err)
		}
		if buildID != int(testUniswapV4BuildID) {
			t.Errorf("build_id = %d, want %d (threaded from the constructor, not defaulted)", buildID, testUniswapV4BuildID)
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
		var buildID int
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT event_name, params, build_id FROM uniswap_v4_pool_event WHERE pool_id=$1 AND block_number=$2`,
			poolID, blockNumber,
		).Scan(&eventName, &params, &buildID); err != nil {
			t.Fatalf("read back pool event: %v", err)
		}
		if buildID != int(testUniswapV4BuildID) {
			t.Errorf("build_id = %d, want %d (threaded from the constructor, not defaulted)", buildID, testUniswapV4BuildID)
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
		var buildID int
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT liquidity_gross::text, liquidity_net::text,
			        fee_growth_outside0_x128::text, fee_growth_outside1_x128::text, build_id
			 FROM uniswap_v4_tick WHERE pool_id=$1 AND tick=180`,
			poolID,
		).Scan(&liquidityGross, &liquidityNet, &feeGrowth0, &feeGrowth1, &buildID); err != nil {
			t.Fatalf("read back tick: %v", err)
		}
		if buildID != int(testUniswapV4BuildID) {
			t.Errorf("build_id = %d, want %d (threaded from the constructor, not defaulted)", buildID, testUniswapV4BuildID)
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

	const blockNumber = int64(21800010)
	writes := newUniswapV4TestBlockWrites(t, poolID, blockNumber, 0)

	repo := newUniswapV4Repo(t)
	save := func() outbound.StateRowCounts {
		var counts outbound.StateRowCounts
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			var err error
			if counts, err = repo.SaveBlock(ctx, tx, writes); err != nil {
				t.Fatalf("SaveBlock: %v", err)
			}
		})
		return counts
	}

	if got := save(); got.Attempted != 1 || got.Persisted != 1 {
		t.Errorf("first save = %+v, want {Attempted:1 Persisted:1}", got)
	}
	if got := save(); got.Attempted != 1 || got.Persisted != 0 {
		t.Errorf("replay = %+v, want {Attempted:1 Persisted:0} (ON CONFLICT DO NOTHING appends nothing, but the block still tried)", got)
	}

	for _, table := range uniswapV4BatchedFactTables {
		t.Run(table, func(t *testing.T) {
			if got := uniswapV4RowCount(t, ctx, table, poolID, blockNumber); got != 1 {
				t.Errorf("%s row count = %d, want 1 (a replay must not duplicate)", table, got)
			}
		})
	}
}

// Ticks are absent: they go through the append-on-change writer, not the batch.
var uniswapV4BatchedFactTables = []string{
	"uniswap_v4_pool_state",
	"uniswap_v4_swap",
	"uniswap_v4_liquidity_event",
	"uniswap_v4_pool_event",
}

func uniswapV4RowCount(t *testing.T, ctx context.Context, table string, poolID int64, blockNumber int64) int {
	t.Helper()
	var count int
	if err := uniswapV4TestPool.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE pool_id=$1 AND block_number=$2`, table),
		poolID, blockNumber,
	).Scan(&count); err != nil {
		t.Fatalf("count %s rows: %v", table, err)
	}
	return count
}

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

	for _, table := range append(slices.Clone(uniswapV4BatchedFactTables), "uniswap_v4_position") {
		t.Run(table, func(t *testing.T) {
			got := uniswapV4RowVersions(t, ctx, table, poolID, blockNumber)
			want := [][2]int{{0, 0}, {1, 0}}
			if !slices.Equal(got, want) {
				t.Errorf("(block_version, processing_version) = %v, want %v (the reorg version is part of the key, so processing_version must not bump)", got, want)
			}
		})
	}
}

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

	const blockNumber = int64(21800020)
	writes := newUniswapV4TestBlockWrites(t, poolID, blockNumber, 0)
	for _, buildID := range []buildregistry.BuildID{testUniswapV4BuildID, testUniswapV4RebuildID} {
		repo := NewUniswapV4Repository(uniswapV4TestPool, buildID)
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, writes); err != nil {
				t.Fatalf("SaveBlock at build %d: %v", buildID, err)
			}
		})
	}

	for _, table := range uniswapV4BatchedFactTables {
		t.Run(table, func(t *testing.T) {
			got := uniswapV4RowBuilds(t, ctx, table, poolID, blockNumber)
			want := [][2]int{
				{0, int(testUniswapV4BuildID)},
				{1, int(testUniswapV4RebuildID)},
			}
			if !slices.Equal(got, want) {
				t.Errorf("(processing_version, build_id) = %v, want %v (a re-index by a newer build must not dedupe, and build_id must be the repository's)", got, want)
			}
		})
	}
}

// Once a chunk is columnstored, TimescaleDB resolves ON CONFLICT before row triggers
// fire: a processing_version left to the trigger reaches the arbiter as DEFAULT 0,
// matches the pv=0 row already there, and the correction is discarded with no error
// (VEC-615). Every fact table here compresses at 2 days, so that is every chunk a
// rebuild or a backfill replay touches.
func TestUniswapV4Repository_SaveBlock_NewBuildAppendsIntoACompressedChunk(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x1c)

	const blockNumber = int64(25000000)
	writes := newUniswapV4TestBlockWrites(t, poolID, blockNumber, 0)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := NewUniswapV4Repository(uniswapV4TestPool, testUniswapV4BuildID).SaveBlock(ctx, tx, writes); err != nil {
			t.Fatalf("SaveBlock at build %d: %v", testUniswapV4BuildID, err)
		}
	})
	for _, table := range uniswapV4BatchedFactTables {
		compressUniswapV4ChunkHolding(t, ctx, table, uniswapV4TestBlockTime(blockNumber))
	}

	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := NewUniswapV4Repository(uniswapV4TestPool, testUniswapV4RebuildID).SaveBlock(ctx, tx, writes); err != nil {
			t.Fatalf("SaveBlock at build %d into compressed chunks: %v", testUniswapV4RebuildID, err)
		}
	})

	for _, table := range uniswapV4BatchedFactTables {
		t.Run(table, func(t *testing.T) {
			got := uniswapV4RowBuilds(t, ctx, table, poolID, blockNumber)
			want := [][2]int{
				{0, int(testUniswapV4BuildID)},
				{1, int(testUniswapV4RebuildID)},
			}
			if !slices.Equal(got, want) {
				t.Errorf("(processing_version, build_id) = %v, want %v (the rebuild's correction row was dropped by the compressed chunk's arbiter)", got, want)
			}
		})
	}
}

// compressUniswapV4ChunkHolding columnstores only the chunk that holds rows at ts, so
// the rest of the file's fixtures stay on rowstore and this test cannot mask theirs.
func compressUniswapV4ChunkHolding(t *testing.T, ctx context.Context, table string, ts time.Time) {
	t.Helper()
	var chunks int
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT count(*)::int FROM (
			SELECT compress_chunk(c, if_not_compressed => true)
			FROM show_chunks($1::regclass, newer_than => $2::timestamptz - INTERVAL '2 days', older_than => $2::timestamptz + INTERVAL '2 days') c
		) s`, table, ts).Scan(&chunks); err != nil {
		t.Fatalf("compress the %s chunk around %s: %v", table, ts, err)
	}
	if chunks == 0 {
		t.Fatalf("%s has no chunk around %s to compress; the seed write did not land", table, ts)
	}
}

func TestUniswapV4Repository_SaveBlock_CountsOnlyTheStateSectionsRows(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x1b)

	const blockNumber = int64(21800040)
	state := newUniswapV4TestState(poolID, blockNumber, 0, 3)
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{state},
		}); err != nil {
			t.Fatalf("seeding the state row: %v", err)
		}
	})

	// Re-saving the state conflicts away (0 rows) while the swap is new (1 row),
	// so the two statements' tags are no longer interchangeable.
	var counts outbound.StateRowCounts
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		var err error
		if counts, err = repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			States: []*entity.UniswapV4PoolState{state},
			Swaps:  []*entity.UniswapV4Swap{newUniswapV4TestSwap(poolID, blockNumber, 0, 9)},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	if want := (outbound.StateRowCounts{Attempted: 1, Persisted: 0}); counts != want {
		t.Errorf("state row counts = %+v, want %+v (the swap's tag must not be read into the state slot)", counts, want)
	}
}

func TestUniswapV4Repository_SaveBlock_NamesTheRejectedRowsBatchSection(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name          string
		discriminator byte
		reject        func(outbound.UniswapV4BlockWrites)
		wantNamed     string
	}{
		{
			name:          "liquidity_event",
			discriminator: 0x1c,
			reject: func(w outbound.UniswapV4BlockWrites) {
				w.LiquidityEvents[0].TickUpper = w.LiquidityEvents[0].TickLower
			},
			wantNamed: "batch liquidity event 0",
		},
		{
			name:          "pool_event",
			discriminator: 0x1d,
			// SaveBlock never calls Validate, so the event_name CHECK rejects this.
			reject: func(w outbound.UniswapV4BlockWrites) {
				w.PoolEvents[0].EventName = entity.UniswapV4PoolEventName("bogus")
			},
			wantNamed: "batch pool event 0",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			poolID := seedUniswapV4RepoTestPool(t, ctx, tc.discriminator)
			writes := newUniswapV4TestBlockWrites(t, poolID, 21800050, 0)
			tc.reject(writes)

			repo := newUniswapV4Repo(t)
			withUniswapV4RollbackTx(t, ctx, func(tx pgx.Tx) {
				_, err := repo.SaveBlock(ctx, tx, writes)
				if err == nil {
					t.Fatal("SaveBlock with a row the schema rejects: want error, got nil")
				}
				if !strings.Contains(err.Error(), tc.wantNamed) {
					t.Errorf("error %q does not name %q", err, tc.wantNamed)
				}
			})
		})
	}
}

func uniswapV4RowBuilds(t *testing.T, ctx context.Context, table string, poolID int64, blockNumber int64) [][2]int {
	t.Helper()
	rows, err := uniswapV4TestPool.Query(ctx, fmt.Sprintf(
		`SELECT processing_version, build_id FROM %s
		 WHERE pool_id=$1 AND block_number=$2 ORDER BY processing_version`, table),
		poolID, blockNumber)
	if err != nil {
		t.Fatalf("query %s builds: %v", table, err)
	}
	defer rows.Close()

	var builds [][2]int
	for rows.Next() {
		var processingVersion, buildID int
		if err := rows.Scan(&processingVersion, &buildID); err != nil {
			t.Fatalf("scan %s builds: %v", table, err)
		}
		builds = append(builds, [2]int{processingVersion, buildID})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s builds: %v", table, err)
	}
	return builds
}

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

func TestUniswapV4Repository_WriteTicks_SameBlockRedeliveryDoesNotAppend(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4TickFixture(t, ctx, 0x17)

	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))
	f.save(newUniswapV4TestTick(f.poolID, 60, 5000, 0, big.NewInt(100)))

	if got := f.rowCount(60); got != 1 {
		t.Fatalf("row count = %d, want 1 (a redelivery of one block at one version must not append)", got)
	}
}

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
			got, err := repo.TicksForPoolAtBlock(ctx, uniswapV4RepoSaveChainID, tc.poolID, 5100)
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
	saveTicks(newUniswapV4TestTick(poolID, -60, targetBlock, 1, big.NewInt(3)))
	saveTicks(newUniswapV4TestTick(poolID, 900, targetBlock+1, 0, big.NewInt(4)))

	got, err := repo.TicksForPoolAtBlock(ctx, uniswapV4RepoSaveChainID, poolID, targetBlock)
	if err != nil {
		t.Fatalf("TicksForPoolAtBlock: %v", err)
	}
	if want := []int32{-60, 300}; !slices.Equal(got, want) {
		t.Errorf("ticks = %v, want %v", got, want)
	}
}

func TestUniswapV4Repository_TicksForPoolAtBlock_ResolvesSupersededPoolForward(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(uniswapV4RepoSupersededChainID))

	fixture := newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoSupersededChainID, 0x1a)
	supersededID := seedUniswapV4RepoPool(t, ctx, fixture)

	const targetBlock = int64(7140000)
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Ticks: []*entity.UniswapV4Tick{
			newUniswapV4TestTick(supersededID, -120, targetBlock, 0, big.NewInt(1)),
			newUniswapV4TestTick(supersededID, 180, targetBlock, 0, big.NewInt(2)),
		}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	fixture.buildID = 1
	fixture.deployBlock = 2
	currentID := seedUniswapV4RepoPool(t, ctx, fixture)
	if currentID == supersededID {
		t.Fatalf("the corrected pool reused id %d; the fixture did not append a new version", currentID)
	}

	got, err := repo.TicksForPoolAtBlock(ctx, uniswapV4RepoSupersededChainID, currentID, targetBlock)
	if err != nil {
		t.Fatalf("TicksForPoolAtBlock: %v", err)
	}
	if want := []int32{-120, 180}; !slices.Equal(got, want) {
		t.Errorf("ticks = %v, want %v (the superseded %d must resolve forward)", got, want, supersededID)
	}
}

func TestUniswapV4Repository_TicksForPoolAtBlock_UnknownBlockIsEmpty(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x08)

	// The pool has to own a tick at another height: with nothing to exclude,
	// a read that ignored block_number entirely would still answer empty.
	const writtenBlock = int64(5300)
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			Ticks: []*entity.UniswapV4Tick{newUniswapV4TestTick(poolID, 120, writtenBlock, 0, big.NewInt(1))},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	got, err := repo.TicksForPoolAtBlock(ctx, uniswapV4RepoSaveChainID, poolID, writtenBlock+1)
	if err != nil {
		t.Fatalf("TicksForPoolAtBlock: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("ticks = %v, want none: the pool's tick at block %d must not answer for block %d", got, writtenBlock, writtenBlock+1)
	}
}

type uniswapV4PositionFixture struct {
	t      *testing.T
	ctx    context.Context
	repo   *UniswapV4Repository
	poolID int64
}

func newUniswapV4PositionFixture(t *testing.T, ctx context.Context, discriminator byte) uniswapV4PositionFixture {
	t.Helper()
	return uniswapV4PositionFixture{
		t:      t,
		ctx:    ctx,
		repo:   newUniswapV4Repo(t),
		poolID: seedUniswapV4RepoTestPool(t, ctx, discriminator),
	}
}

func (f uniswapV4PositionFixture) save(positions ...*entity.UniswapV4Position) {
	f.t.Helper()
	withUniswapV4Tx(f.t, f.ctx, func(tx pgx.Tx) {
		if _, err := f.repo.SaveBlock(f.ctx, tx, outbound.UniswapV4BlockWrites{Positions: positions}); err != nil {
			f.t.Fatalf("SaveBlock: %v", err)
		}
	})
}

func (f uniswapV4PositionFixture) rowCount(key entity.UniswapV4PositionKey) int {
	f.t.Helper()
	var count int
	if err := uniswapV4TestPool.QueryRow(f.ctx,
		`SELECT count(*) FROM uniswap_v4_position
		 WHERE pool_id=$1 AND owner=$2 AND tick_lower=$3 AND tick_upper=$4 AND salt=$5`,
		f.poolID, key.Owner.Bytes(), key.TickLower, key.TickUpper, key.Salt.Bytes(),
	).Scan(&count); err != nil {
		f.t.Fatalf("count positions for %+v: %v", key, err)
	}
	return count
}

func (f uniswapV4PositionFixture) latestValue(key entity.UniswapV4PositionKey, column string) string {
	f.t.Helper()
	var value string
	if err := uniswapV4TestPool.QueryRow(f.ctx, fmt.Sprintf(
		`SELECT %s::text FROM uniswap_v4_position
		 WHERE pool_id=$1 AND owner=$2 AND tick_lower=$3 AND tick_upper=$4 AND salt=$5
		 ORDER BY block_number DESC, block_version DESC, processing_version DESC LIMIT 1`, column),
		f.poolID, key.Owner.Bytes(), key.TickLower, key.TickUpper, key.Salt.Bytes(),
	).Scan(&value); err != nil {
		f.t.Fatalf("query latest %s for %+v: %v", column, key, err)
	}
	return value
}

func (f uniswapV4PositionFixture) position(key entity.UniswapV4PositionKey, blockNumber int64, blockVersion int, values uniswapV4PositionValues) *entity.UniswapV4Position {
	return newUniswapV4TestPosition(f.poolID, key, blockNumber, blockVersion, values)
}

func TestUniswapV4Repository_WritePositions_FirstWriteInserts(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x20)
	key := defaultUniswapV4PositionKey()

	f.save(f.position(key, 6000, 0, defaultUniswapV4PositionValues()))

	if got := f.rowCount(key); got != 1 {
		t.Fatalf("row count = %d, want 1", got)
	}
}

func TestUniswapV4Repository_WritePositions_UnchangedValuesDoNotAppend(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x21)
	key := defaultUniswapV4PositionKey()

	f.save(f.position(key, 6000, 0, defaultUniswapV4PositionValues()))
	f.save(f.position(key, 6001, 0, defaultUniswapV4PositionValues()))

	if got := f.rowCount(key); got != 1 {
		t.Fatalf("row count = %d, want 1 (unchanged values must not append)", got)
	}
}

func TestUniswapV4Repository_WritePositions_ChangedValueAppends(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		column        string
		discriminator byte
		set           func(*uniswapV4PositionValues, *big.Int)
	}{
		{"liquidity", 0x22, func(v *uniswapV4PositionValues, n *big.Int) { v.liquidity = n }},
		{"fee_growth_inside0_last_x128", 0x23, func(v *uniswapV4PositionValues, n *big.Int) { v.feeGrowthInside0LastX128 = n }},
		{"fee_growth_inside1_last_x128", 0x24, func(v *uniswapV4PositionValues, n *big.Int) { v.feeGrowthInside1LastX128 = n }},
	} {
		t.Run(tc.column, func(t *testing.T) {
			f := newUniswapV4PositionFixture(t, ctx, tc.discriminator)
			key := defaultUniswapV4PositionKey()

			f.save(f.position(key, 6000, 0, defaultUniswapV4PositionValues()))

			changed := defaultUniswapV4PositionValues()
			changedValue := big.NewInt(999)
			tc.set(&changed, changedValue)
			f.save(f.position(key, 6002, 0, changed))

			if got := f.rowCount(key); got != 2 {
				t.Fatalf("row count = %d, want 2", got)
			}
			if got := f.latestValue(key, tc.column); got != changedValue.String() {
				t.Errorf("latest %s = %q, want %q", tc.column, got, changedValue)
			}
		})
	}
}

func TestUniswapV4Repository_WritePositions_PokeMovesOnlyFeeGrowthAndStillAppends(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x25)
	key := defaultUniswapV4PositionKey()

	f.save(f.position(key, 6000, 0, defaultUniswapV4PositionValues()))

	poked := defaultUniswapV4PositionValues()
	poked.feeGrowthInside0LastX128 = big.NewInt(4242)
	poked.feeGrowthInside1LastX128 = big.NewInt(4343)
	f.save(f.position(key, 6003, 0, poked))

	if got := f.rowCount(key); got != 2 {
		t.Fatalf("row count = %d, want 2", got)
	}
	if got := f.latestValue(key, "liquidity"); got != "1000" {
		t.Errorf("latest liquidity = %q, want 1000 (a poke does not move liquidity)", got)
	}
}

func TestUniswapV4Repository_WritePositions_OutOfOrderWriteIsNotComparedAgainstANewerRow(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x33)
	key := defaultUniswapV4PositionKey()

	f.save(f.position(key, 6600, 0, defaultUniswapV4PositionValues()))
	f.save(f.position(key, 6599, 0, defaultUniswapV4PositionValues()))

	if got := f.rowCount(key); got != 2 {
		t.Fatalf("row count = %d, want 2 (the earlier block has no prior row of its own to match)", got)
	}
}

func TestUniswapV4Repository_WritePositions_ReorgReobservationAppends(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x26)
	key := defaultUniswapV4PositionKey()

	f.save(f.position(key, 6004, 0, defaultUniswapV4PositionValues()))
	f.save(f.position(key, 6004, 1, defaultUniswapV4PositionValues()))

	if got := f.rowCount(key); got != 2 {
		t.Fatalf("row count = %d, want 2 (a reorg re-observation appends even with identical values)", got)
	}
}

func TestUniswapV4Repository_WritePositions_MixedBatchSkipsOnlyUnchanged(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x27)

	unchanged := defaultUniswapV4PositionKey()
	changing := defaultUniswapV4PositionKey()
	changing.TickUpper = 120
	fresh := defaultUniswapV4PositionKey()
	fresh.Salt = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000009")

	f.save(
		f.position(unchanged, 6000, 0, defaultUniswapV4PositionValues()),
		f.position(changing, 6000, 0, defaultUniswapV4PositionValues()),
	)

	moved := defaultUniswapV4PositionValues()
	moved.liquidity = big.NewInt(777)
	f.save(
		f.position(unchanged, 6001, 0, defaultUniswapV4PositionValues()),
		f.position(changing, 6001, 0, moved),
		f.position(fresh, 6001, 0, defaultUniswapV4PositionValues()),
	)

	for _, tc := range []struct {
		name string
		key  entity.UniswapV4PositionKey
		want int
	}{
		{"unchanged", unchanged, 1},
		{"changed", changing, 2},
		{"new", fresh, 1},
	} {
		if got := f.rowCount(tc.key); got != tc.want {
			t.Errorf("%s position row count = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestUniswapV4Repository_WritePositions_EveryKeyComponentSeparatesHistories(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name          string
		discriminator byte
		vary          func(*entity.UniswapV4PositionKey)
	}{
		{"owner", 0x28, func(k *entity.UniswapV4PositionKey) {
			k.Owner = common.HexToAddress("0x000000000022D473030F116dDEE9F6B43aC78BA3")
		}},
		{"tick_lower", 0x29, func(k *entity.UniswapV4PositionKey) { k.TickLower = -120 }},
		{"tick_upper", 0x2a, func(k *entity.UniswapV4PositionKey) { k.TickUpper = 120 }},
		{"salt", 0x2b, func(k *entity.UniswapV4PositionKey) {
			k.Salt = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000002")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newUniswapV4PositionFixture(t, ctx, tc.discriminator)
			base := defaultUniswapV4PositionKey()
			other := base
			tc.vary(&other)

			f.save(f.position(base, 6005, 0, defaultUniswapV4PositionValues()))
			f.save(f.position(other, 6005, 0, defaultUniswapV4PositionValues()))

			if got := f.rowCount(base); got != 1 {
				t.Errorf("base row count = %d, want 1", got)
			}
			if got := f.rowCount(other); got != 1 {
				t.Errorf("varied-%s row count = %d, want 1 (it must not be folded into the base history)", tc.name, got)
			}
		})
	}
}

func TestUniswapV4Repository_WritePositions_RoundTripsExtremeNumerics(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x2c)
	key := defaultUniswapV4PositionKey()

	maxUint256 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	maxUint128 := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 128), big.NewInt(1))

	f.save(f.position(key, 6010, 0, uniswapV4PositionValues{
		liquidity:                maxUint128,
		feeGrowthInside0LastX128: maxUint256,
		feeGrowthInside1LastX128: maxUint256,
	}))

	if got := f.latestValue(key, "liquidity"); got != maxUint128.String() {
		t.Errorf("liquidity = %q, want %q", got, maxUint128)
	}
	for _, column := range []string{"fee_growth_inside0_last_x128", "fee_growth_inside1_last_x128"} {
		if got := f.latestValue(key, column); got != maxUint256.String() {
			t.Errorf("%s = %q, want %q", column, got, maxUint256)
		}
	}
}

func TestUniswapV4Repository_WritePositions_TwoPoolsInOneCall(t *testing.T) {
	ctx := context.Background()
	poolA := seedUniswapV4RepoTestPool(t, ctx, 0x2d)
	poolB := seedUniswapV4RepoTestPool(t, ctx, 0x2e)

	key := defaultUniswapV4PositionKey()
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Positions: []*entity.UniswapV4Position{
			newUniswapV4TestPosition(poolB, key, 6100, 0, defaultUniswapV4PositionValues()),
			newUniswapV4TestPosition(poolA, key, 6100, 0, defaultUniswapV4PositionValues()),
		}}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	for _, tc := range []struct {
		name   string
		poolID int64
	}{
		{"pool_a", poolA},
		{"pool_b", poolB},
	} {
		var rows int
		if err := uniswapV4TestPool.QueryRow(ctx,
			`SELECT count(*) FROM uniswap_v4_position
			 WHERE pool_id=$1 AND owner=$2 AND tick_lower=$3 AND tick_upper=$4 AND salt=$5`,
			tc.poolID, key.Owner.Bytes(), key.TickLower, key.TickUpper, key.Salt.Bytes(),
		).Scan(&rows); err != nil {
			t.Fatalf("counting %s positions: %v", tc.name, err)
		}
		if rows != 1 {
			t.Errorf("%s position row count = %d, want 1", tc.name, rows)
		}
	}
}

func TestUniswapV4Repository_PositionsForPoolAtBlock_ReturnsDistinctKeysInOrder(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x2f)

	const targetBlock = int64(6200)
	lowOwner := defaultUniswapV4PositionKey()
	lowOwner.Owner = common.HexToAddress("0x000000000022D473030F116dDEE9F6B43aC78BA3")
	lowTicks := defaultUniswapV4PositionKey()
	lowTicks.TickLower = -120
	lowSalt := defaultUniswapV4PositionKey()
	highSalt := defaultUniswapV4PositionKey()
	highSalt.Salt = common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ff")
	otherBlock := defaultUniswapV4PositionKey()
	otherBlock.TickLower = -180

	f.save(
		f.position(highSalt, targetBlock, 0, defaultUniswapV4PositionValues()),
		f.position(lowSalt, targetBlock, 0, defaultUniswapV4PositionValues()),
		f.position(lowTicks, targetBlock, 0, defaultUniswapV4PositionValues()),
		f.position(lowOwner, targetBlock, 0, defaultUniswapV4PositionValues()),
	)
	f.save(f.position(lowSalt, targetBlock, 1, defaultUniswapV4PositionValues()))
	f.save(f.position(otherBlock, targetBlock+1, 0, defaultUniswapV4PositionValues()))

	got, err := f.repo.PositionsForPoolAtBlock(ctx, f.poolID, targetBlock)
	if err != nil {
		t.Fatalf("PositionsForPoolAtBlock: %v", err)
	}
	if want := []entity.UniswapV4PositionKey{lowOwner, lowTicks, lowSalt, highSalt}; !slices.Equal(got, want) {
		t.Errorf("positions = %+v, want %+v", got, want)
	}
	// The port promises Compare order; SQL ORDER BY is what has to deliver it.
	if !slices.IsSortedFunc(got, entity.UniswapV4PositionKey.Compare) {
		t.Errorf("positions = %+v, want them sorted by UniswapV4PositionKey.Compare", got)
	}
}

func TestUniswapV4Repository_PositionsForPoolAtBlock_UnknownBlockIsEmpty(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x30)

	got, err := f.repo.PositionsForPoolAtBlock(ctx, f.poolID, 6300)
	if err != nil {
		t.Fatalf("PositionsForPoolAtBlock: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("positions = %+v, want none", got)
	}
}

func TestUniswapV4Repository_WritePositions_DuplicateSlotInOneBlockErrors(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x36)
	key := defaultUniswapV4PositionKey()

	other := defaultUniswapV4PositionValues()
	other.liquidity = big.NewInt(4242)

	withUniswapV4RollbackTx(t, ctx, func(tx pgx.Tx) {
		_, err := f.repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Positions: []*entity.UniswapV4Position{
			f.position(key, 6700, 0, defaultUniswapV4PositionValues()),
			f.position(key, 6700, 0, other),
		}})
		if err == nil {
			t.Fatal("SaveBlock with one slot twice: want error, got nil")
		}
		if !strings.Contains(err.Error(), "distinct slots") {
			t.Errorf("error %q does not name the duplicate slot", err)
		}
	})
}

func TestUniswapV4Repository_WritePositions_ValueDriftAtOneBlockVersionErrors(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x37)
	key := defaultUniswapV4PositionKey()

	const blockNumber = int64(6800)
	f.save(f.position(key, blockNumber, 0, defaultUniswapV4PositionValues()))

	drifted := defaultUniswapV4PositionValues()
	drifted.liquidity = big.NewInt(999_999)

	withUniswapV4RollbackTx(t, ctx, func(tx pgx.Tx) {
		_, err := f.repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Positions: []*entity.UniswapV4Position{
			f.position(key, blockNumber, 0, drifted),
		}})
		if err == nil {
			t.Fatal("SaveBlock re-writing one block version with different values: want error, got nil")
		}
		if !strings.Contains(err.Error(), "disagrees with itself") {
			t.Errorf("error %q does not name the read disagreement", err)
		}
	})

	if got := f.rowCount(key); got != 1 {
		t.Errorf("row count = %d, want 1 (the drifted write must not have landed)", got)
	}
}

func TestUniswapV4Repository_WritePositions_MixedBlockNumbersError(t *testing.T) {
	ctx := context.Background()
	f := newUniswapV4PositionFixture(t, ctx, 0x31)
	key := defaultUniswapV4PositionKey()

	withUniswapV4RollbackTx(t, ctx, func(tx pgx.Tx) {
		_, err := f.repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Positions: []*entity.UniswapV4Position{
			f.position(key, 6400, 0, defaultUniswapV4PositionValues()),
			f.position(key, 6401, 0, defaultUniswapV4PositionValues()),
		}})
		if err == nil {
			t.Fatal("SaveBlock with positions from two blocks: want error, got nil")
		}
		if !strings.Contains(err.Error(), "one SaveBlock is one block") {
			t.Errorf("error %q does not name the one-block-per-SaveBlock rule", err)
		}
	})
}

func TestUniswapV4Repository_WritePositions_NilNumericWritesNothing(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		column        string
		discriminator byte
		nilOut        func(*entity.UniswapV4Position)
	}{
		{"liquidity", 0x32, func(p *entity.UniswapV4Position) { p.Liquidity = nil }},
		{"fee_growth_inside0_last_x128", 0x34, func(p *entity.UniswapV4Position) { p.FeeGrowthInside0LastX128 = nil }},
		{"fee_growth_inside1_last_x128", 0x35, func(p *entity.UniswapV4Position) { p.FeeGrowthInside1LastX128 = nil }},
	} {
		t.Run(tc.column, func(t *testing.T) {
			f := newUniswapV4PositionFixture(t, ctx, tc.discriminator)

			const blockNumber = int64(6500)
			broken := f.position(defaultUniswapV4PositionKey(), blockNumber, 0, defaultUniswapV4PositionValues())
			tc.nilOut(broken)

			withUniswapV4RollbackTx(t, ctx, func(tx pgx.Tx) {
				if _, err := f.repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
					Positions: []*entity.UniswapV4Position{broken},
				}); err == nil {
					t.Fatalf("SaveBlock with a nil %s: want error, got nil", tc.column)
				}
			})

			var count int
			if err := uniswapV4TestPool.QueryRow(ctx,
				`SELECT count(*) FROM uniswap_v4_position WHERE pool_id=$1 AND block_number=$2`,
				f.poolID, blockNumber,
			).Scan(&count); err != nil {
				t.Fatalf("count positions: %v", err)
			}
			if count != 0 {
				t.Errorf("uniswap_v4_position has %d rows after a rolled-back SaveBlock, want 0", count)
			}
		})
	}
}

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

// An excluded pool stays in the result: dropping it here would stop its event
// indexing too; the service skips only its snapshots.
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

// protocol_id is a surrogate FK with nothing tying it to the row's own chain, so
// an unscoped join silently hands back another chain's PoolManager address.
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

// An inner join would drop the newest manager version and hand back the previous
// version's StateView, so the defect would read as a clean pool.
func TestUniswapV4Repository_LoadPools_RejectsCrossChainProtocolOnNewestManagerVersion(t *testing.T) {
	ctx := context.Background()

	manager := newUniswapV4RepoManagerFixture(uniswapV4RepoXChainMgrVerChainID)
	seedUniswapV4RepoPoolManager(t, ctx, manager)
	manager.buildID = 1
	manager.protocolChainID = uniswapV4RepoXChainMgrDonorChID
	seedUniswapV4RepoPoolManager(t, ctx, manager)
	poolID := seedUniswapV4RepoPool(t, ctx,
		newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoXChainMgrVerChainID, 0x42))

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoXChainMgrVerChainID)
	if err == nil {
		t.Fatalf("LoadPools with pool id=%d whose newest pool manager version references another chain's protocol: want error, got %+v", poolID, pools)
	}
	if !strings.Contains(err.Error(), "uniswap_v4_pool_manager") {
		t.Errorf("error %q does not name the offending uniswap_v4_pool_manager row", err)
	}
}

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

// Binds the entity's event-name constant to the column CHECK: a rename on either
// side must fail here, not at the first real ProtocolFeeUpdated log.
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

		// Counting on the pool after the rollback reads 0 under MVCC whatever
		// SaveBlock did, so the count that pins anything has to run inside tx.
		if got := uniswapV4TxRowCount(t, ctx, tx, "uniswap_v4_pool_state", poolID, blockNumber); got != 1 {
			t.Errorf("uniswap_v4_pool_state rows inside the failed transaction = %d, want 1: the batch must already have run when the tick conversion fails", got)
		}
		if got := uniswapV4TxRowCount(t, ctx, tx, "uniswap_v4_tick", poolID, blockNumber); got != 0 {
			t.Errorf("uniswap_v4_tick rows inside the failed transaction = %d, want 0", got)
		}
	})

	for _, table := range []string{"uniswap_v4_pool_state", "uniswap_v4_tick"} {
		if got := uniswapV4RowCount(t, ctx, table, poolID, blockNumber); got != 0 {
			t.Errorf("%s has %d rows after the rolled-back SaveBlock, want 0", table, got)
		}
	}
}

func uniswapV4TxRowCount(t *testing.T, ctx context.Context, tx pgx.Tx, table string, poolID int64, blockNumber int64) int {
	t.Helper()
	var count int
	if err := tx.QueryRow(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s WHERE pool_id=$1 AND block_number=$2`, table),
		poolID, blockNumber,
	).Scan(&count); err != nil {
		t.Fatalf("count %s inside the transaction: %v", table, err)
	}
	return count
}

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
	// PoolIds, bytewise ascending: poolB (0x10) sorts before poolA (0x12).
	want := []common.Hash{{0x10}, {0x12}}
	if !slices.Equal(got, want) {
		t.Errorf("pool ids = %v, want %v (pools %d and %d)", got, want, poolB, poolA)
	}
}

// The fact tables carry no chain_id; the scope comes from the registry join.
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
	if want := []common.Hash{{0x13}}; !slices.Equal(got, want) {
		t.Errorf("pool ids = %v, want %v (pool %d; the neighbouring chain's pool %d must not leak in)", got, want, homePool, neighbourPool)
	}
}

// A registry correction appends a new version with a new surrogate id; state rows
// written under the old one and the new one are the same pool, and a worker that
// booted on either version must resolve them to its own row.
func TestUniswapV4Repository_PoolIDsWithStateAtBlock_ResolvesEveryRegistryVersionToOnePoolId(t *testing.T) {
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
	if want := []common.Hash{fixture.poolID}; !slices.Equal(got, want) {
		t.Errorf("pool ids = %v, want %v (the one PoolId behind superseded row %d and current row %d)", got, want, supersededID, currentID)
	}
}

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

// PoolIDsWithStateAtBlock bounds its chunk scan around this timestamp.
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

// newUniswapV4TestBlockWrites builds one validated row for each of the five
// fact hypertables, sharing every key but blockVersion so two calls differ
// exactly as an original and its reorg re-observation do.
func newUniswapV4TestBlockWrites(t *testing.T, poolID int64, blockNumber int64, blockVersion int) outbound.UniswapV4BlockWrites {
	t.Helper()

	state := newUniswapV4TestState(poolID, blockNumber, blockVersion, 7)
	swap := newUniswapV4TestSwap(poolID, blockNumber, blockVersion, 1)
	liquidityEvent := newUniswapV4TestLiquidityEvent(poolID, blockNumber, blockVersion, 2)
	poolEvent := newUniswapV4TestPoolEvent(poolID, blockNumber, blockVersion, 3)

	position := newUniswapV4TestPosition(poolID, defaultUniswapV4PositionKey(), blockNumber, blockVersion, defaultUniswapV4PositionValues())

	for _, v := range []interface{ Validate() error }{state, swap, liquidityEvent, poolEvent, position} {
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	}

	return outbound.UniswapV4BlockWrites{
		States:          []*entity.UniswapV4PoolState{state},
		Swaps:           []*entity.UniswapV4Swap{swap},
		LiquidityEvents: []*entity.UniswapV4LiquidityEvent{liquidityEvent},
		PoolEvents:      []*entity.UniswapV4PoolEvent{poolEvent},
		Positions:       []*entity.UniswapV4Position{position},
	}
}

type uniswapV4PositionValues struct {
	liquidity                *big.Int
	feeGrowthInside0LastX128 *big.Int
	feeGrowthInside1LastX128 *big.Int
}

// defaultUniswapV4PositionValues are three distinct values, so a case mutating
// one column cannot pass on another column's value.
func defaultUniswapV4PositionValues() uniswapV4PositionValues {
	return uniswapV4PositionValues{
		liquidity:                big.NewInt(1000),
		feeGrowthInside0LastX128: big.NewInt(2),
		feeGrowthInside1LastX128: big.NewInt(3),
	}
}

func defaultUniswapV4PositionKey() entity.UniswapV4PositionKey {
	return entity.UniswapV4PositionKey{
		Owner:     common.HexToAddress("0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"),
		TickLower: -60,
		TickUpper: 60,
		Salt:      common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000001"),
	}
}

func newUniswapV4TestPosition(poolID int64, key entity.UniswapV4PositionKey, blockNumber int64, blockVersion int, values uniswapV4PositionValues) *entity.UniswapV4Position {
	return &entity.UniswapV4Position{
		PoolID:                   poolID,
		Owner:                    key.Owner,
		TickLower:                key.TickLower,
		TickUpper:                key.TickUpper,
		Salt:                     key.Salt,
		BlockNumber:              blockNumber,
		BlockVersion:             blockVersion,
		BlockTimestamp:           time.Unix(1740000000+blockNumber, 0).UTC(),
		Liquidity:                values.liquidity,
		FeeGrowthInside0LastX128: values.feeGrowthInside0LastX128,
		FeeGrowthInside1LastX128: values.feeGrowthInside1LastX128,
	}
}

var (
	uniswapV4TestTxHash = common.HexToHash("0x11ccddeeff00112233445566778899aabbccddeeff00112233445566778899aa")
	uniswapV4TestSender = common.HexToAddress("0x66a9893cc07d91d95644aedd05d03f95e1dba8af")
)

func newUniswapV4TestSwap(poolID int64, blockNumber int64, blockVersion, logIndex int) *entity.UniswapV4Swap {
	return &entity.UniswapV4Swap{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: blockVersion,
		BlockTimestamp: uniswapV4TestBlockTime(blockNumber),
		TxHash:         uniswapV4TestTxHash, LogIndex: logIndex, Sender: uniswapV4TestSender,
		Amount0: big.NewInt(-100), Amount1: big.NewInt(100),
		SqrtPriceX96: big.NewInt(1), Liquidity: big.NewInt(1), Tick: 0, Fee: 3000,
	}
}

func newUniswapV4TestLiquidityEvent(poolID int64, blockNumber int64, blockVersion, logIndex int) *entity.UniswapV4LiquidityEvent {
	return &entity.UniswapV4LiquidityEvent{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: blockVersion,
		BlockTimestamp: uniswapV4TestBlockTime(blockNumber),
		TxHash:         uniswapV4TestTxHash, LogIndex: logIndex, Sender: uniswapV4TestSender,
		TickLower: -60, TickUpper: 60,
		LiquidityDelta: big.NewInt(1000),
		Salt:           common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ff"),
	}
}

func newUniswapV4TestPoolEvent(poolID int64, blockNumber int64, blockVersion, logIndex int) *entity.UniswapV4PoolEvent {
	return &entity.UniswapV4PoolEvent{
		PoolID: poolID, BlockNumber: blockNumber, BlockVersion: blockVersion,
		BlockTimestamp: uniswapV4TestBlockTime(blockNumber),
		TxHash:         uniswapV4TestTxHash, LogIndex: logIndex,
		EventName: entity.UniswapV4PoolEventInitialize,
		Params:    json.RawMessage(`{"sqrtPriceX96":"1000","tick":10}`),
	}
}

type uniswapV4TickValues struct {
	liquidityGross        *big.Int
	liquidityNet          *big.Int
	feeGrowthOutside0X128 *big.Int
	feeGrowthOutside1X128 *big.Int
}

// Four distinct values, so a case mutating one column cannot pass on another's.
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

func containsPoolID(msg string, poolID int64) bool {
	return strings.Contains(msg, strconv.FormatInt(poolID, 10))
}

// The ever-snapshotted reads are unbounded in block range, so each scenario needs
// a chain of its own.
func seedUniswapV4EverIndexedPool(t *testing.T, ctx context.Context, chainID int, discriminator byte) int64 {
	t.Helper()
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainID))
	return seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainID, discriminator))
}

// Neither table alone is enough: a pool with no initialized ticks writes only a
// state row, and a baseline enumeration can write only tick rows.
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

func TestUniswapV4Repository_LoadPools_ReturnsThePositionManager(t *testing.T) {
	ctx := context.Background()
	poolID := seedUniswapV4RepoTestPool(t, ctx, 0x51)
	manager := newUniswapV4RepoManagerFixture(uniswapV4RepoSaveChainID)
	wantID := currentUniswapV4RepoPositionManagerID(t, ctx, uniswapV4RepoSaveChainID)

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoSaveChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	idx := slices.IndexFunc(pools, func(p outbound.UniswapV4PoolRow) bool { return p.ID == poolID })
	if idx < 0 {
		t.Fatalf("pool %d missing from LoadPools result", poolID)
	}
	if got := pools[idx].PositionManager; got != manager.positionManager {
		t.Errorf("PositionManager = %s, want %s", got, manager.positionManager)
	}
	if got := pools[idx].PositionManagerID; got != wantID {
		t.Errorf("PositionManagerID = %d, want %d", got, wantID)
	}
}

// A nil PositionManager address would make the decoder match address(0)'s logs,
// so an absent registry row has to be a named error rather than a zero value.
func TestUniswapV4Repository_LoadPools_RejectsChainWithPoolsButNoPositionManager(t *testing.T) {
	ctx := context.Background()

	manager := newUniswapV4RepoManagerFixture(uniswapV4RepoNoPosmChainID)
	manager.positionManager = common.Address{}
	seedUniswapV4RepoPoolManager(t, ctx, manager)
	poolID := seedUniswapV4RepoPool(t, ctx,
		newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoNoPosmChainID, 0x52))

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoNoPosmChainID)
	if err == nil {
		t.Fatalf("LoadPools on a chain with pool id=%d but no position manager: want error, got %d pools", poolID, len(pools))
	}
	if !strings.Contains(err.Error(), "uniswap_v4_position_manager") {
		t.Errorf("error %q does not name the missing uniswap_v4_position_manager row", err)
	}
}

// The posm registry is versioned like the PoolManager's, so a correction must
// re-point the whole chain at the new address and surrogate id.
func TestUniswapV4Repository_LoadPools_UsesLatestPositionManagerVersion(t *testing.T) {
	ctx := context.Background()

	manager := newUniswapV4RepoManagerFixture(uniswapV4RepoPosmVerChainID)
	supersededPosm := manager.positionManager
	seedUniswapV4RepoPoolManager(t, ctx, manager)

	correctedPosm := common.HexToAddress("0x00000000000000000000000000000000000c0de1")
	manager.positionManager = correctedPosm
	manager.buildID = 1
	seedUniswapV4RepoPoolManager(t, ctx, manager)
	seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoPosmVerChainID, 0x53))

	wantID := currentUniswapV4RepoPositionManagerID(t, ctx, uniswapV4RepoPosmVerChainID)

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoPosmVerChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	if len(pools) == 0 {
		t.Fatal("LoadPools returned no pools")
	}
	for _, p := range pools {
		if p.PositionManager != correctedPosm {
			t.Errorf("pool %d PositionManager = %s, want %s (the superseded %s must not win)",
				p.ID, p.PositionManager, correctedPosm, supersededPosm)
		}
		if p.PositionManagerID != wantID {
			t.Errorf("pool %d PositionManagerID = %d, want %d", p.ID, p.PositionManagerID, wantID)
		}
	}
}

// An inner join would drop the newest posm version whose protocol row is on
// another chain and hand back the previous version's address, so the defect has
// to surface as an error naming the table.
func TestUniswapV4Repository_LoadPools_RejectsCrossChainPositionManagerProtocol(t *testing.T) {
	ctx := context.Background()

	manager := newUniswapV4RepoManagerFixture(uniswapV4RepoXChainPosmChainID)
	manager.posmProtocolChainID = uniswapV4RepoXChainMgrDonorChID
	seedUniswapV4RepoPoolManager(t, ctx, manager)
	poolID := seedUniswapV4RepoPool(t, ctx,
		newUniswapV4RepoPoolFixture(t, ctx, uniswapV4RepoXChainPosmChainID, 0x54))

	repo := newUniswapV4Repo(t)
	pools, err := repo.LoadPools(ctx, uniswapV4RepoXChainPosmChainID)
	if err == nil {
		t.Fatalf("LoadPools with pool id=%d whose position manager protocol row lives on another chain: want error, got %d pools", poolID, len(pools))
	}
	if !strings.Contains(err.Error(), "uniswap_v4_position_manager") {
		t.Errorf("error %q does not name the offending uniswap_v4_position_manager row", err)
	}
}

// The two fixtures are verbatim mainnet posm Transfer logs: token 1's mint at
// block 21695956 (from = address(0)) and token 388720 changing hands at block
// 25873334.
var (
	uniswapV4MintFixtureTx   = common.HexToHash("0x4e63fcc0dd42a2b317e77d17e236cadf77464a08ccece33a354bd8648b5f7419")
	uniswapV4MintFixtureTo   = common.HexToAddress("0x4423B0D6955aF39B48cf215577a79Ce574299D3f")
	uniswapV4MoveFixtureTx   = common.HexToHash("0x41904e8dc4f2218019baaf8a7195e264ccd1530f5f56ae0db0027c1f0772c6e4")
	uniswapV4MoveFixtureFrom = common.HexToAddress("0x3b0a17a75A14EAaEF42002a4891AcF8F9fD8A72E")
	uniswapV4MoveFixtureTo   = common.HexToAddress("0xe588dDd13a8bDBee578eAa7c4Fd9780180b2f10C")
)

func newUniswapV4RepoNFTTransfer(managerID, blockNumber int64, blockVersion, logIndex int, tokenID int64, from, to common.Address) *entity.UniswapV4PositionNFTTransfer {
	return &entity.UniswapV4PositionNFTTransfer{
		PositionManagerID: managerID,
		TokenID:           big.NewInt(tokenID),
		BlockNumber:       blockNumber,
		BlockVersion:      blockVersion,
		BlockTimestamp:    time.Unix(1740000000, 0).UTC(),
		TxHash:            uniswapV4MoveFixtureTx,
		LogIndex:          logIndex,
		From:              from,
		To:                to,
	}
}

// holderOfUniswapV4Token runs the holder-at-block query the table's COMMENT
// documents, so the ordering the schema promises is exercised rather than
// re-derived by the test.
func holderOfUniswapV4Token(t *testing.T, ctx context.Context, managerID int64, tokenID int64, atBlock int64) common.Address {
	t.Helper()
	var to []byte
	if err := uniswapV4TestPool.QueryRow(ctx, `
		SELECT to_address
		FROM uniswap_v4_position_nft_transfer
		WHERE position_manager_id = $1 AND token_id = $2 AND block_number <= $3
		ORDER BY block_number DESC, block_version DESC, log_index DESC, processing_version DESC
		LIMIT 1`, managerID, tokenID, atBlock).Scan(&to); err != nil {
		t.Fatalf("reading holder of token %d at block %d: %v", tokenID, atBlock, err)
	}
	return common.BytesToAddress(to)
}

func TestUniswapV4Repository_SaveBlock_RoundTripsNFTTransfers(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoTestPool(t, ctx, 0x55)
	managerID := currentUniswapV4RepoPositionManagerID(t, ctx, uniswapV4RepoSaveChainID)

	mint := &entity.UniswapV4PositionNFTTransfer{
		PositionManagerID: managerID,
		TokenID:           big.NewInt(1),
		BlockNumber:       21695956,
		BlockTimestamp:    time.Unix(1737790055, 0).UTC(),
		TxHash:            uniswapV4MintFixtureTx,
		LogIndex:          67,
		From:              common.Address{},
		To:                uniswapV4MintFixtureTo,
	}
	move := &entity.UniswapV4PositionNFTTransfer{
		PositionManagerID: managerID,
		TokenID:           big.NewInt(388720),
		BlockNumber:       25873334,
		BlockTimestamp:    time.Unix(1787000000, 0).UTC(),
		TxHash:            uniswapV4MoveFixtureTx,
		LogIndex:          1219,
		From:              uniswapV4MoveFixtureFrom,
		To:                uniswapV4MoveFixtureTo,
	}
	for _, v := range []*entity.UniswapV4PositionNFTTransfer{mint, move} {
		if err := v.Validate(); err != nil {
			t.Fatalf("Validate: %v", err)
		}
	}

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			NFTTransfers: []*entity.UniswapV4PositionNFTTransfer{mint, move},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	for _, want := range []*entity.UniswapV4PositionNFTTransfer{mint, move} {
		t.Run(want.TokenID.String(), func(t *testing.T) {
			var (
				tokenID              string
				txHash, from, to     []byte
				gotLogIndex, buildID int
				gotTimestamp         time.Time
			)
			if err := uniswapV4TestPool.QueryRow(ctx, `
				SELECT token_id::text, tx_hash, log_index, from_address, to_address,
				       block_timestamp, build_id
				FROM uniswap_v4_position_nft_transfer
				WHERE position_manager_id = $1 AND block_number = $2`,
				managerID, want.BlockNumber,
			).Scan(&tokenID, &txHash, &gotLogIndex, &from, &to, &gotTimestamp, &buildID); err != nil {
				t.Fatalf("read back transfer: %v", err)
			}
			if tokenID != want.TokenID.String() {
				t.Errorf("token_id = %q, want %q", tokenID, want.TokenID)
			}
			if got := common.BytesToHash(txHash); got != want.TxHash {
				t.Errorf("tx_hash = %s, want %s", got, want.TxHash)
			}
			if gotLogIndex != want.LogIndex {
				t.Errorf("log_index = %d, want %d", gotLogIndex, want.LogIndex)
			}
			if got := common.BytesToAddress(from); got != want.From {
				t.Errorf("from_address = %s, want %s", got, want.From)
			}
			if got := common.BytesToAddress(to); got != want.To {
				t.Errorf("to_address = %s, want %s", got, want.To)
			}
			if !gotTimestamp.UTC().Equal(want.BlockTimestamp) {
				t.Errorf("block_timestamp = %s, want %s", gotTimestamp.UTC(), want.BlockTimestamp)
			}
			if buildID != int(testUniswapV4BuildID) {
				t.Errorf("build_id = %d, want %d", buildID, testUniswapV4BuildID)
			}
		})
	}
}

// A reorg redelivery re-decodes the new fork's logs; the transfer table has no
// state to re-read, so the whole correction is the appended (N, v1) row set.
func TestUniswapV4Repository_SaveBlock_ReorgAppendsASecondNFTTransferRowSet(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoTestPool(t, ctx, 0x56)
	managerID := currentUniswapV4RepoPositionManagerID(t, ctx, uniswapV4RepoSaveChainID)

	const blockNumber = int64(21800100)
	orphaned := newUniswapV4RepoNFTTransfer(managerID, blockNumber, 0, 7, 4242,
		uniswapV4MoveFixtureFrom, uniswapV4MintFixtureTo)
	canonical := newUniswapV4RepoNFTTransfer(managerID, blockNumber, 1, 7, 4242,
		uniswapV4MoveFixtureFrom, uniswapV4MoveFixtureTo)

	repo := newUniswapV4Repo(t)
	for _, transfer := range []*entity.UniswapV4PositionNFTTransfer{orphaned, canonical} {
		withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
			if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
				NFTTransfers: []*entity.UniswapV4PositionNFTTransfer{transfer},
			}); err != nil {
				t.Fatalf("SaveBlock at block_version %d: %v", transfer.BlockVersion, err)
			}
		})
	}

	var rows int
	if err := uniswapV4TestPool.QueryRow(ctx,
		`SELECT count(*) FROM uniswap_v4_position_nft_transfer
		 WHERE position_manager_id = $1 AND block_number = $2`,
		managerID, blockNumber).Scan(&rows); err != nil {
		t.Fatalf("counting transfer versions: %v", err)
	}
	if rows != 2 {
		t.Errorf("rows at block %d = %d, want 2 (the orphaned fork's row is superseded, never replaced)", blockNumber, rows)
	}
	if got := holderOfUniswapV4Token(t, ctx, managerID, 4242, blockNumber); got != canonical.To {
		t.Errorf("holder at block %d = %s, want %s (block_version DESC must pick the reorg re-observation)", blockNumber, got, canonical.To)
	}
}

// A token can change hands twice inside one block, so log_index is part of both
// the key and the holder ordering: without it the earlier log wins.
func TestUniswapV4Repository_NFTTransferHolderAtBlockPicksTheLastLogInTheBlock(t *testing.T) {
	ctx := context.Background()
	seedUniswapV4RepoTestPool(t, ctx, 0x57)
	managerID := currentUniswapV4RepoPositionManagerID(t, ctx, uniswapV4RepoSaveChainID)

	// Both logs are the real mainnet pair on token 113383 at block 25873296:
	// log 4325 moves it out and log 4363 moves it straight back.
	const (
		blockNumber = int64(25873296)
		tokenID     = int64(113383)
	)
	owner := common.HexToAddress("0x66BF88E42A01EFF49A9f22Cae6E46bb2412916cD")
	custodian := common.HexToAddress("0x542298e710b32b49883577883B75B39eF18883ce")

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{
			NFTTransfers: []*entity.UniswapV4PositionNFTTransfer{
				newUniswapV4RepoNFTTransfer(managerID, blockNumber, 0, 4325, tokenID, owner, custodian),
				newUniswapV4RepoNFTTransfer(managerID, blockNumber, 0, 4363, tokenID, custodian, owner),
			},
		}); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	if got := holderOfUniswapV4Token(t, ctx, managerID, tokenID, blockNumber); got != owner {
		t.Errorf("holder at block %d = %s, want %s (log_index DESC must pick log 4363, not 4325)", blockNumber, got, owner)
	}
}
