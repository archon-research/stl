//go:build integration

package postgres

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// outbound.UniswapV4PositionValuationReader (VEC-829): the allocation
// tracker's read path for valuing a posm-managed V4 LP position as of a
// historical block. One dedicated chain per test, per the file's convention,
// so these never share fixtures with the SaveBlock/LoadPools tests in
// uniswap_v4_repository_integration_test.go.

const (
	uniswapV4RepoValuationHeldChainID    = 490026
	uniswapV4RepoValuationPosChainID     = 490027
	uniswapV4RepoValuationStateChainID   = 490028
	uniswapV4RepoValuationIsolationChID  = 490029
	uniswapV4RepoValuationIsolationChID2 = 490030
	uniswapV4RepoValuationOrphanChID     = 490031
)

func uniswapV4TokenIDSetEqual(got []*big.Int, want ...*big.Int) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, g := range got {
		seen[g.String()] = true
	}
	for _, w := range want {
		if !seen[w.String()] {
			return false
		}
	}
	return true
}

// TestUniswapV4Repository_HeldTokenIDsAtBlock_TracksHolderAcrossATransfer is
// the wallet-side read UniV4Source needs: it must answer "as of block N",
// including a point before a later transfer moved a token elsewhere.
func TestUniswapV4Repository_HeldTokenIDsAtBlock_TracksHolderAcrossATransfer(t *testing.T) {
	ctx := context.Background()
	chainID := uniswapV4RepoValuationHeldChainID
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainID))
	managerID := currentUniswapV4RepoPositionManagerID(t, ctx, chainID)

	walletA := common.HexToAddress("0x000000000000000000000000000000000AAAAA")
	walletB := common.HexToAddress("0x000000000000000000000000000000000BBBBB")
	movedToken := big.NewInt(555)
	stayingToken := big.NewInt(556)

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		transfers := []*entity.UniswapV4PositionNFTTransfer{
			newUniswapV4RepoNFTTransfer(managerID, 100, 0, 1, movedToken.Int64(), common.Address{}, walletA),
			newUniswapV4RepoNFTTransfer(managerID, 100, 0, 2, stayingToken.Int64(), common.Address{}, walletA),
			newUniswapV4RepoNFTTransfer(managerID, 200, 0, 1, movedToken.Int64(), walletA, walletB),
		}
		if _, err := repo.SaveNFTTransfers(ctx, tx, transfers); err != nil {
			t.Fatalf("SaveNFTTransfers: %v", err)
		}
	})

	tests := []struct {
		name        string
		wallet      common.Address
		blockNumber int64
		want        []*big.Int
	}{
		{"A before the move holds both", walletA, 150, []*big.Int{movedToken, stayingToken}},
		{"B before the move holds nothing", walletB, 150, nil},
		{"A after the move keeps only the staying token", walletA, 250, []*big.Int{stayingToken}},
		{"B after the move holds the moved token", walletB, 250, []*big.Int{movedToken}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := repo.HeldTokenIDsAtBlock(ctx, int64(chainID), tc.wallet, tc.blockNumber)
			if err != nil {
				t.Fatalf("HeldTokenIDsAtBlock: %v", err)
			}
			if !uniswapV4TokenIDSetEqual(got, tc.want...) {
				t.Errorf("held = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestUniswapV4Repository_HeldTokenIDsAtBlock_SkipsAnOrphanedTransfer proves
// the candidates-CTE rewrite of the runbook's holder-at-block recipe kept its
// block_states exclusion: nothing is re-read from chain state, so a transfer
// decoded on a fork the watcher later orphaned must not answer.
func TestUniswapV4Repository_HeldTokenIDsAtBlock_SkipsAnOrphanedTransfer(t *testing.T) {
	ctx := context.Background()
	chainID := uniswapV4RepoValuationOrphanChID
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainID))
	managerID := currentUniswapV4RepoPositionManagerID(t, ctx, chainID)

	wallet := common.HexToAddress("0x00000000000000000000000000000000CCCCCC")
	orphanedRecipient := common.HexToAddress("0x00000000000000000000000000000000DDDDDD")
	const (
		blockNumber = int64(300)
		tokenID     = int64(777)
	)

	repo := newUniswapV4Repo(t)
	mint := newUniswapV4RepoNFTTransfer(managerID, blockNumber-10, 0, 1, tokenID, common.Address{}, wallet)
	orphanedMove := newUniswapV4RepoNFTTransfer(managerID, blockNumber, 0, 1, tokenID, wallet, orphanedRecipient)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SaveNFTTransfers(ctx, tx, []*entity.UniswapV4PositionNFTTransfer{mint, orphanedMove}); err != nil {
			t.Fatalf("SaveNFTTransfers: %v", err)
		}
	})

	// assign_block_version numbers same-(chain,number) rows in insertion order:
	// this orphaned fork claims version 0, matching orphanedMove's BlockVersion.
	for _, b := range []struct {
		hash     string
		orphaned bool
	}{{"0xorphan-300", true}, {"0xcanon-300", false}} {
		if _, err := uniswapV4TestPool.Exec(ctx, `
			INSERT INTO block_states (chain_id, number, hash, parent_hash, received_at, is_orphaned, created_at)
			VALUES ($1, $2, $3, '0xparent', 0, $4, now())`,
			chainID, blockNumber, b.hash, b.orphaned); err != nil {
			t.Fatalf("seeding block_states %s: %v", b.hash, err)
		}
	}

	got, err := repo.HeldTokenIDsAtBlock(ctx, int64(chainID), wallet, blockNumber+10)
	if err != nil {
		t.Fatalf("HeldTokenIDsAtBlock: %v", err)
	}
	if !uniswapV4TokenIDSetEqual(got, big.NewInt(tokenID)) {
		t.Errorf("held by original wallet = %v, want [%d]: the orphaned move must not count", got, tokenID)
	}

	gotOrphanRecipient, err := repo.HeldTokenIDsAtBlock(ctx, int64(chainID), orphanedRecipient, blockNumber+10)
	if err != nil {
		t.Fatalf("HeldTokenIDsAtBlock: %v", err)
	}
	if len(gotOrphanRecipient) != 0 {
		t.Errorf("held by the orphaned fork's recipient = %v, want none", gotOrphanRecipient)
	}
}

// TestUniswapV4Repository_HeldTokenIDsAtBlock_ChainIsolation guards the
// candidates-CTE rewrite's chain_id joins: a same-wallet, same-token-id
// transfer on a different chain must never leak in.
func TestUniswapV4Repository_HeldTokenIDsAtBlock_ChainIsolation(t *testing.T) {
	ctx := context.Background()
	chainA, chainB := uniswapV4RepoValuationIsolationChID, uniswapV4RepoValuationIsolationChID2
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainA))
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainB))
	managerA := currentUniswapV4RepoPositionManagerID(t, ctx, chainA)
	managerB := currentUniswapV4RepoPositionManagerID(t, ctx, chainB)

	wallet := common.HexToAddress("0x00000000000000000000000000000000EEEEEE")
	tokenID := int64(4242)

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		transfers := []*entity.UniswapV4PositionNFTTransfer{
			newUniswapV4RepoNFTTransfer(managerA, 100, 0, 1, tokenID, common.Address{}, wallet),
		}
		if _, err := repo.SaveNFTTransfers(ctx, tx, transfers); err != nil {
			t.Fatalf("SaveNFTTransfers chain A: %v", err)
		}
	})

	gotA, err := repo.HeldTokenIDsAtBlock(ctx, int64(chainA), wallet, 200)
	if err != nil {
		t.Fatalf("HeldTokenIDsAtBlock chain A: %v", err)
	}
	if !uniswapV4TokenIDSetEqual(gotA, big.NewInt(tokenID)) {
		t.Errorf("chain A held = %v, want [%d]", gotA, tokenID)
	}

	gotB, err := repo.HeldTokenIDsAtBlock(ctx, int64(chainB), wallet, 200)
	if err != nil {
		t.Fatalf("HeldTokenIDsAtBlock chain B: %v", err)
	}
	if len(gotB) != 0 {
		t.Errorf("chain B held = %v, want none: chain A's transfer (manager %d) must not leak via manager %d's chain", gotB, managerA, managerB)
	}
}

// TestUniswapV4Repository_PositionForTokenAtBlock_ResolvesAcrossPoolsAndAtBlock
// is the other half of the posm identity: a token id names no pool, so the
// read must search every pool on the chain, and it must answer "as of block
// N" against a position whose liquidity later changed.
func TestUniswapV4Repository_PositionForTokenAtBlock_ResolvesAcrossPoolsAndAtBlock(t *testing.T) {
	ctx := context.Background()
	chainID := uniswapV4RepoValuationPosChainID
	mgr := newUniswapV4RepoManagerFixture(chainID)
	seedUniswapV4RepoPoolManager(t, ctx, mgr)
	poolA := seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainID, 0xA1))
	poolB := seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainID, 0xB2))

	tokenA := big.NewInt(11)
	tokenB := big.NewInt(22)
	saltA := common.BigToHash(tokenA)
	saltB := common.BigToHash(tokenB)
	keyA := entity.UniswapV4PositionKey{Owner: mgr.positionManager, TickLower: -60, TickUpper: 60, Salt: saltA}
	keyB := entity.UniswapV4PositionKey{Owner: mgr.positionManager, TickLower: -120, TickUpper: 120, Salt: saltB}

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		positions := []*entity.UniswapV4Position{
			newUniswapV4TestPosition(poolA, keyA, 100, 0, defaultUniswapV4PositionValues()),
			newUniswapV4TestPosition(poolB, keyB, 100, 0, defaultUniswapV4PositionValues()),
		}
		if _, err := repo.SavePositions(ctx, tx, positions); err != nil {
			t.Fatalf("SavePositions: %v", err)
		}
	})

	increased := defaultUniswapV4PositionValues()
	increased.liquidity = big.NewInt(9999)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SavePositions(ctx, tx, []*entity.UniswapV4Position{
			newUniswapV4TestPosition(poolA, keyA, 300, 0, increased),
		}); err != nil {
			t.Fatalf("SavePositions (later liquidity change): %v", err)
		}
	})

	tests := []struct {
		name          string
		tokenID       *big.Int
		blockNumber   int64
		wantPoolID    int64
		wantTickLower int
		wantTickUpper int
		wantLiquidity int64
		wantNotFound  bool
	}{
		{"tokenA before the liquidity change", tokenA, 150, poolA, -60, 60, 1000, false},
		{"tokenA after the liquidity change", tokenA, 350, poolA, -60, 60, 9999, false},
		{"tokenB resolves to its own pool", tokenB, 150, poolB, -120, 120, 1000, false},
		{"a token id never touched", big.NewInt(999), 150, 0, 0, 0, 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := repo.PositionForTokenAtBlock(ctx, int64(chainID), mgr.positionManager, tc.tokenID, tc.blockNumber)
			if err != nil {
				t.Fatalf("PositionForTokenAtBlock: %v", err)
			}
			if tc.wantNotFound {
				if got != nil {
					t.Errorf("got %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("got nil, want a position")
			}
			if got.PoolID != tc.wantPoolID || got.TickLower != tc.wantTickLower || got.TickUpper != tc.wantTickUpper || got.Liquidity.Cmp(big.NewInt(tc.wantLiquidity)) != 0 {
				t.Errorf("got (pool=%d, [%d,%d], liq=%s), want (pool=%d, [%d,%d], liq=%d)",
					got.PoolID, got.TickLower, got.TickUpper, got.Liquidity,
					tc.wantPoolID, tc.wantTickLower, tc.wantTickUpper, tc.wantLiquidity)
			}
		})
	}
}

// TestUniswapV4Repository_PositionForTokenAtBlock_ChainIsolation guards the
// chain_id join: the same owner/salt pair on a different chain's pool must
// not resolve into this chain's answer.
func TestUniswapV4Repository_PositionForTokenAtBlock_ChainIsolation(t *testing.T) {
	ctx := context.Background()
	chainA, chainB := uniswapV4RepoValuationIsolationChID, uniswapV4RepoValuationIsolationChID2
	mgrA := newUniswapV4RepoManagerFixture(chainA)
	seedUniswapV4RepoPoolManager(t, ctx, mgrA)
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainB))
	poolA := seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainA, 0xC1))

	tokenID := big.NewInt(33)
	key := entity.UniswapV4PositionKey{Owner: mgrA.positionManager, TickLower: -60, TickUpper: 60, Salt: common.BigToHash(tokenID)}

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		if _, err := repo.SavePositions(ctx, tx, []*entity.UniswapV4Position{
			newUniswapV4TestPosition(poolA, key, 100, 0, defaultUniswapV4PositionValues()),
		}); err != nil {
			t.Fatalf("SavePositions: %v", err)
		}
	})

	gotA, err := repo.PositionForTokenAtBlock(ctx, int64(chainA), mgrA.positionManager, tokenID, 200)
	if err != nil {
		t.Fatalf("PositionForTokenAtBlock chain A: %v", err)
	}
	if gotA == nil || gotA.PoolID != poolA {
		t.Errorf("chain A position = %+v, want pool %d", gotA, poolA)
	}

	gotB, err := repo.PositionForTokenAtBlock(ctx, int64(chainB), mgrA.positionManager, tokenID, 200)
	if err != nil {
		t.Fatalf("PositionForTokenAtBlock chain B: %v", err)
	}
	if gotB != nil {
		t.Errorf("chain B position = %+v, want nil: chain A's pool must not resolve under chain B", gotB)
	}
}

// TestUniswapV4Repository_PoolStateAtBlock_ReturnsLatestAtOrBefore locks the
// same "value as of a point" convention readLatestPositionsV4 uses.
func TestUniswapV4Repository_PoolStateAtBlock_ReturnsLatestAtOrBefore(t *testing.T) {
	ctx := context.Background()
	chainID := uniswapV4RepoValuationStateChainID
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainID))
	poolID := seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainID, 0xC3))

	newState := func(blockNumber int64, sqrtPriceX96 *big.Int) *entity.UniswapV4PoolState {
		return &entity.UniswapV4PoolState{
			PoolID:               poolID,
			BlockNumber:          blockNumber,
			BlockTimestamp:       time.Unix(1740000000+blockNumber, 0).UTC(),
			SqrtPriceX96:         sqrtPriceX96,
			Tick:                 0,
			LpFee:                3000,
			Liquidity:            big.NewInt(1000),
			FeeGrowthGlobal0X128: big.NewInt(0),
			FeeGrowthGlobal1X128: big.NewInt(0),
		}
	}
	priceAt100 := new(big.Int).Lsh(big.NewInt(1), 96)
	priceAt300 := new(big.Int).Lsh(big.NewInt(2), 96)

	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		writes := outbound.UniswapV4BlockWrites{States: []*entity.UniswapV4PoolState{
			newState(100, priceAt100),
			newState(300, priceAt300),
		}}
		if _, err := repo.SaveBlock(ctx, tx, writes); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	tests := []struct {
		name        string
		blockNumber int64
		want        *big.Int
	}{
		{"between the two snapshots returns the earlier one", 150, priceAt100},
		{"after the later snapshot returns it", 350, priceAt300},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := repo.PoolStateAtBlock(ctx, poolID, tc.blockNumber)
			if err != nil {
				t.Fatalf("PoolStateAtBlock: %v", err)
			}
			if got == nil || got.Cmp(tc.want) != 0 {
				t.Errorf("PoolStateAtBlock(%d) = %v, want %s", tc.blockNumber, got, tc.want)
			}
		})
	}

	got, err := repo.PoolStateAtBlock(ctx, poolID, 50)
	if err != nil {
		t.Fatalf("PoolStateAtBlock(50): %v", err)
	}
	if got != nil {
		t.Errorf("PoolStateAtBlock(50) = %s, want nil (before the pool's first snapshot)", got)
	}
}

// TestUniswapV4Repository_PoolStateAtBlock_ChainIsolation is implicit in the
// query (it filters by the surrogate pool_id alone, which is already
// chain-scoped by construction), but a distinct pool id on another chain must
// still answer independently rather than accidentally aliasing.
func TestUniswapV4Repository_PoolStateAtBlock_ChainIsolation(t *testing.T) {
	ctx := context.Background()
	chainA, chainB := uniswapV4RepoValuationIsolationChID, uniswapV4RepoValuationIsolationChID2
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainA))
	seedUniswapV4RepoPoolManager(t, ctx, newUniswapV4RepoManagerFixture(chainB))
	poolA := seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainA, 0xD1))
	poolB := seedUniswapV4RepoPool(t, ctx, newUniswapV4RepoPoolFixture(t, ctx, chainB, 0xD2))

	priceA := new(big.Int).Lsh(big.NewInt(3), 96)
	repo := newUniswapV4Repo(t)
	withUniswapV4Tx(t, ctx, func(tx pgx.Tx) {
		writes := outbound.UniswapV4BlockWrites{States: []*entity.UniswapV4PoolState{{
			PoolID: poolA, BlockNumber: 100, BlockTimestamp: time.Unix(1740000100, 0).UTC(),
			SqrtPriceX96: priceA, Tick: 0, LpFee: 3000, Liquidity: big.NewInt(1000),
			FeeGrowthGlobal0X128: big.NewInt(0), FeeGrowthGlobal1X128: big.NewInt(0),
		}}}
		if _, err := repo.SaveBlock(ctx, tx, writes); err != nil {
			t.Fatalf("SaveBlock: %v", err)
		}
	})

	gotB, err := repo.PoolStateAtBlock(ctx, poolB, 200)
	if err != nil {
		t.Fatalf("PoolStateAtBlock poolB: %v", err)
	}
	if gotB != nil {
		t.Errorf("poolB state = %s, want nil: poolA's state must not answer for poolB", gotB)
	}
}
