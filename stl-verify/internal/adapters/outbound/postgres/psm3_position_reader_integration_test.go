//go:build integration

package postgres

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// outbound.Psm3PositionReader (VEC-833): the allocation tracker's read path
// for valuing a Spark PSM3 ALM stake as of a historical block.

func seedPsm3AlmShare(
	t *testing.T, ctx context.Context, repo *PSM3ReservesRepository,
	chainID int64, psm3 common.Address, prime string, alm common.Address,
	shares, assetValue *big.Int, blockNumber int64, blockTimestamp time.Time,
) {
	t.Helper()
	err := repo.SaveReserves(ctx, &entity.PSM3Reserves{
		ChainID: chainID, Address: psm3,
		State: entity.PSM3State{
			USDSBalance: big.NewInt(1), SUSDSBalance: big.NewInt(1), USDCBalance: big.NewInt(1),
			TotalAssets: assetValue, ConversionRate: big.NewInt(1), TotalShares: shares,
			ALMPositions: []entity.PSM3ALMPosition{{
				Prime: prime, Address: alm, Shares: shares, AssetValue: assetValue,
			}},
		},
		BlockNumber: blockNumber, BlockVersion: 0,
		BlockTimestamp: blockTimestamp, Source: "sweep",
	})
	if err != nil {
		t.Fatalf("SaveReserves: %v", err)
	}
}

// TestPsm3PositionReaderRepo_AlmShareAtBlock_ReturnsLatestAtOrBefore locks the
// same "value as of a point" convention PoolStateAtBlock uses.
func TestPsm3PositionReaderRepo_AlmShareAtBlock_ReturnsLatestAtOrBefore(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	seedReferencePrime(t, ctx, pool, "spark-psm3-reader")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	writer := NewPSM3ReservesRepository(newReferenceRepoTxm(t, pool), nil, buildID, runID)

	psm3 := common.HexToAddress("0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266")
	alm := common.HexToAddress("0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709")
	const chainID = 42161

	baseTime := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	seedPsm3AlmShare(t, ctx, writer, chainID, psm3, "spark-psm3-reader", alm,
		big.NewInt(1_000_000), big.NewInt(1_010_000), 100, baseTime)
	seedPsm3AlmShare(t, ctx, writer, chainID, psm3, "spark-psm3-reader", alm,
		big.NewInt(2_000_000), big.NewInt(2_040_000), 300, baseTime.Add(time.Hour))

	reader := NewPsm3PositionReaderRepo(pool)

	tests := []struct {
		name           string
		blockNumber    int64
		wantShares     int64
		wantAssetValue int64
	}{
		{"between the two snapshots returns the earlier one", 150, 1_000_000, 1_010_000},
		{"after the later snapshot returns it", 350, 2_000_000, 2_040_000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := reader.AlmShareAtBlock(ctx, chainID, psm3, alm, tc.blockNumber, baseTime.Add(2*time.Hour))
			if err != nil {
				t.Fatalf("AlmShareAtBlock: %v", err)
			}
			if got == nil {
				t.Fatal("got nil, want a snapshot")
			}
			if got.Shares.Cmp(big.NewInt(tc.wantShares)) != 0 {
				t.Errorf("Shares = %s, want %d", got.Shares, tc.wantShares)
			}
			if got.AssetValue.Cmp(big.NewInt(tc.wantAssetValue)) != 0 {
				t.Errorf("AssetValue = %s, want %d", got.AssetValue, tc.wantAssetValue)
			}
		})
	}

	got, err := reader.AlmShareAtBlock(ctx, chainID, psm3, alm, 50, baseTime)
	if err != nil {
		t.Fatalf("AlmShareAtBlock(50): %v", err)
	}
	if got != nil {
		t.Errorf("AlmShareAtBlock(50) = %+v, want nil (before the ALM's first snapshot)", got)
	}
}

// TestPsm3PositionReaderRepo_AlmShareAtBlock_TimestampBandExcludesFarBlocks
// locks the +/-1 day block_timestamp band (VEC-541 chunk pruning): a
// blockTimestamp far from the actual snapshot's own timestamp must not
// answer with it, even though block_number <= blockNumber alone would match.
func TestPsm3PositionReaderRepo_AlmShareAtBlock_TimestampBandExcludesFarBlocks(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	seedReferencePrime(t, ctx, pool, "spark-psm3-band")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	writer := NewPSM3ReservesRepository(newReferenceRepoTxm(t, pool), nil, buildID, runID)

	psm3 := common.HexToAddress("0xe0F9978b907853F354d79188A3dEfbD41978af62")
	alm := common.HexToAddress("0x876664f0c9Ff24D1aa355Ce9f1680AE1A5bf36fB")
	const chainID = 10

	baseTime := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seedPsm3AlmShare(t, ctx, writer, chainID, psm3, "spark-psm3-band", alm,
		big.NewInt(500_000), big.NewInt(505_000), 100, baseTime)

	reader := NewPsm3PositionReaderRepo(pool)

	// blockNumber <= 200 still matches; only the +/-1 day band should exclude it.
	got, err := reader.AlmShareAtBlock(ctx, chainID, psm3, alm, 200, baseTime.Add(30*24*time.Hour))
	if err != nil {
		t.Fatalf("AlmShareAtBlock: %v", err)
	}
	if got != nil {
		t.Errorf("AlmShareAtBlock = %+v, want nil: a blockTimestamp 30 days out must miss the snapshot's chunk", got)
	}
}

// TestPsm3PositionReaderRepo_AlmShareAtBlock_ChainAndAlmIsolation guards the
// chain_id/alm_address filters: a different ALM or a different chain must not
// leak into the answer.
func TestPsm3PositionReaderRepo_AlmShareAtBlock_ChainAndAlmIsolation(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	seedReferencePrime(t, ctx, pool, "spark-psm3-iso")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	writer := NewPSM3ReservesRepository(newReferenceRepoTxm(t, pool), nil, buildID, runID)

	psm3 := common.HexToAddress("0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f")
	almA := common.HexToAddress("0x345E368fcCd62266B3f5F37C9a131FD1c39f5869")
	almB := common.HexToAddress("0x00000000000000000000000000000000009999")
	const chainID = 130

	baseTime := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	seedPsm3AlmShare(t, ctx, writer, chainID, psm3, "spark-psm3-iso", almA,
		big.NewInt(1_500_000), big.NewInt(1_500_500), 100, baseTime)

	reader := NewPsm3PositionReaderRepo(pool)

	gotB, err := reader.AlmShareAtBlock(ctx, chainID, psm3, almB, 200, baseTime)
	if err != nil {
		t.Fatalf("AlmShareAtBlock almB: %v", err)
	}
	if gotB != nil {
		t.Errorf("almB share = %+v, want nil: almA's snapshot must not answer for almB", gotB)
	}

	gotOtherChain, err := reader.AlmShareAtBlock(ctx, chainID+1, psm3, almA, 200, baseTime)
	if err != nil {
		t.Fatalf("AlmShareAtBlock other chain: %v", err)
	}
	if gotOtherChain != nil {
		t.Errorf("other-chain share = %+v, want nil: chain %d's snapshot must not answer for chain %d", gotOtherChain, chainID, chainID+1)
	}
}
