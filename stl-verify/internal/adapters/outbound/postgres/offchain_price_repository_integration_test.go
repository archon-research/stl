//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestPriceRepositoryUpsertPrices_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewPriceRepository(pool, nil, buildID, runID, 0)
	if err != nil {
		t.Fatalf("NewPriceRepository: %v", err)
	}

	price, err := entity.NewTokenPrice(1, 1, 1.5, nil, nil, time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewTokenPrice: %v", err)
	}
	if err := repo.UpsertPrices(ctx, []*entity.TokenPrice{price}); err != nil {
		t.Fatalf("UpsertPrices: %v", err)
	}

	var gotRunID *int64
	if err := pool.QueryRow(ctx, `SELECT run_id FROM offchain_token_price WHERE token_id = 1 AND source_id = 1`).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if gotRunID == nil || *gotRunID != int64(runID) {
		t.Errorf("run_id = %v, want %d", gotRunID, runID)
	}
}
