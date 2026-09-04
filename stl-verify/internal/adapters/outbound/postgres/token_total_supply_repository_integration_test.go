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
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestTokenTotalSupplySaveSupplies_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	tokenRepo, err := NewTokenRepository(pool, nil, 0, runID)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	txm := newReferenceRepoTxm(t, pool)
	repo := NewTokenTotalSupplyRepository(pool, txm, tokenRepo, nil, buildID, runID)

	tokenAddr := common.HexToAddress("0xe7df13b8e3d6740fe17cbe928c7334243d86c92f")
	err = txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return repo.SaveSupplies(ctx, tx, []*entity.TokenTotalSupply{{
			ChainID: 1, TokenAddress: tokenAddr, TokenSymbol: "spUSDT", TokenDecimals: 6,
			TotalSupply: big.NewInt(1_000_000), BlockNumber: 24_000_000, BlockVersion: 0,
			BlockTimestamp: time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), Source: "sweep", CreatedAtBlock: 23_000_000,
		}})
	})
	if err != nil {
		t.Fatalf("SaveSupplies: %v", err)
	}

	var gotRunID *int64
	if err := pool.QueryRow(ctx, `
		SELECT s.run_id FROM token_total_supply s JOIN token t ON t.id = s.token_id
		WHERE t.chain_id = 1 AND t.address = $1`, tokenAddr.Bytes()).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
}
