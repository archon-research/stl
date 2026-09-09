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

func TestPSM3SaveReserves_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	seedReferencePrime(t, ctx, pool, "spark-psm3-run")
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo := NewPSM3ReservesRepository(newReferenceRepoTxm(t, pool), nil, buildID, runID)

	psm3 := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	alm := common.HexToAddress("0x2222222222222222222222222222222222222222")
	err := repo.SaveReserves(ctx, &entity.PSM3Reserves{
		ChainID: 1, Address: psm3,
		State: entity.PSM3State{
			USDSBalance: big.NewInt(1), SUSDSBalance: big.NewInt(1), USDCBalance: big.NewInt(1),
			TotalAssets: big.NewInt(2_000_000), ConversionRate: big.NewInt(1), TotalShares: big.NewInt(2_000_000),
			ALMPositions: []entity.PSM3ALMPosition{{
				Prime: "spark-psm3-run", Address: alm, Shares: big.NewInt(1_000_000), AssetValue: big.NewInt(1_000_000),
			}},
		},
		BlockNumber: 24_000_000, BlockVersion: 0,
		BlockTimestamp: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), Source: "sweep",
	})
	if err != nil {
		t.Fatalf("SaveReserves: %v", err)
	}

	var reservesRunID, sharesRunID *int64
	if err := pool.QueryRow(ctx, `SELECT run_id FROM psm3_reserves WHERE address = $1`, psm3.Bytes()).Scan(&reservesRunID); err != nil {
		t.Fatalf("reading back psm3_reserves: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT run_id FROM psm3_alm_shares WHERE alm_address = $1`, alm.Bytes()).Scan(&sharesRunID); err != nil {
		t.Fatalf("reading back psm3_alm_shares: %v", err)
	}
	if reservesRunID == nil || *reservesRunID != int64(runID) {
		t.Errorf("psm3_reserves.run_id = %v, want %d", reservesRunID, runID)
	}
	if sharesRunID == nil || *sharesRunID != int64(runID) {
		t.Errorf("psm3_alm_shares.run_id = %v, want %d", sharesRunID, runID)
	}
}
