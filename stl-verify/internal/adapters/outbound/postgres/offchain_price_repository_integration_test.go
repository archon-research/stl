//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func newPriceRepositoryRunFixture(t *testing.T, ctx context.Context) (*pgxpool.Pool, *PriceRepository, buildregistry.RunID) {
	t.Helper()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	repo, err := NewPriceRepository(pool, nil, buildID, runID, 0)
	if err != nil {
		t.Fatalf("NewPriceRepository: %v", err)
	}
	return pool, repo, runID
}

func TestPriceRepositoryUpsertPrices_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, repo, runID := newPriceRepositoryRunFixture(t, ctx)

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
	testutil.RequireRunID(t, gotRunID, runID)
}

func TestPriceRepositoryUpsertAssetPrices_WrittenRowsCarryTheRunID(t *testing.T) {
	ctx := context.Background()
	pool, repo, runID := newPriceRepositoryRunFixture(t, ctx)

	var sourceID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO offchain_price_source (name, display_name)
		VALUES ('run-id-test', 'Run ID test')
		RETURNING id`).Scan(&sourceID); err != nil {
		t.Fatalf("creating price source: %v", err)
	}

	var assetID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO offchain_price_asset (source_id, source_asset_id, tokenless, name, symbol)
		VALUES ($1, 'run-id-test', true, 'Run ID test asset', 'RID')
		RETURNING id`, sourceID).Scan(&assetID); err != nil {
		t.Fatalf("creating price asset: %v", err)
	}

	price, err := entity.NewAssetPrice(assetID, int16(sourceID), 1.5, nil, nil, time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewAssetPrice: %v", err)
	}
	if err := repo.UpsertAssetPrices(ctx, []*entity.AssetPrice{price}); err != nil {
		t.Fatalf("UpsertAssetPrices: %v", err)
	}

	var gotRunID *int64
	if err := pool.QueryRow(ctx, `SELECT run_id FROM asset_price WHERE asset_id = $1 AND source_id = $2`, assetID, sourceID).Scan(&gotRunID); err != nil {
		t.Fatalf("reading back: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
}
