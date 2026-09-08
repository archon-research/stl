//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/testsuite"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}

// The real WETH mainnet address: the migrations already seed this token, so the
// natural key is the only safe way to reach it.
const wethAddress = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

// coinGeckoFixtureServer serves market_chart/range with one point per hour of the
// requested window, in the live API's array-of-[millis, value] shape.
//
// Inclusive at both ends, like the real endpoint: a 30-day range returns 721
// points, not 720. An exclusive fixture cannot observe a seam hour being fetched
// by two adjacent chunks, which is exactly the bug chunkWindows guards against.
func coinGeckoFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/coins/", func(w http.ResponseWriter, r *http.Request) {
		var fromUnix, toUnix int64
		fmt.Sscanf(r.URL.Query().Get("from"), "%d", &fromUnix)
		fmt.Sscanf(r.URL.Query().Get("to"), "%d", &toUnix)

		response := struct {
			Prices       [][]float64 `json:"prices"`
			MarketCaps   [][]float64 `json:"market_caps"`
			TotalVolumes [][]float64 `json:"total_volumes"`
		}{}

		price := 1500.0
		for ts := fromUnix; ts <= toUnix; ts += 3600 {
			millis := float64(ts) * 1000
			response.Prices = append(response.Prices, []float64{millis, price})
			response.MarketCaps = append(response.MarketCaps, []float64{millis, price * 1e6})
			response.TotalVolumes = append(response.TotalVolumes, []float64{millis, price * 1e3})
			price++
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Errorf("encoding fixture response: %v", err)
		}
	})

	return httptest.NewServer(mux)
}

// seedWETHAsset makes sure the token and its CoinGecko mapping exist, and returns
// the resolved token id. Sibling tests wipe shared tables, so every test seeds what
// it needs; the upserts key on the natural key because the migrations may already
// have seeded this token with an id of their own choosing.
func seedWETHAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()

	addressBytes, err := testutil.HexToBytes(wethAddress)
	if err != nil {
		t.Fatalf("parsing WETH address: %v", err)
	}

	var tokenID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals, updated_at)
		VALUES (1, $1, 'WETH', 18, NOW())
		ON CONFLICT (chain_id, address) DO UPDATE SET updated_at = NOW()
		RETURNING id
	`, addressBytes).Scan(&tokenID); err != nil {
		t.Fatalf("seeding token: %v", err)
	}

	var sourceID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID); err != nil {
		t.Fatalf("looking up the coingecko source: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, symbol, name, enabled, created_at, updated_at)
		VALUES ($1, 'weth', $2, 'WETH', 'Wrapped Ether', true, NOW(), NOW())
		ON CONFLICT (source_id, source_asset_id) DO UPDATE SET token_id = $2
	`, sourceID, tokenID); err != nil {
		t.Fatalf("seeding price asset: %v", err)
	}

	return tokenID
}

// newActivityEnv runs the real composition root and returns a Temporal activity
// environment with the wired activity registered, so these tests drive the same
// object the worker registers, through the same harness Temporal uses.
//
// The activity must run inside this environment rather than being called directly:
// activity.GetLogger panics without an activity context.
func newActivityEnv(t *testing.T, ctx context.Context, pool *pgxpool.Pool, baseURL string) *testsuite.TestActivityEnvironment {
	t.Helper()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("COINGECKO_API_KEY", "test-api-key")
	t.Setenv("COINGECKO_BASE_URL", baseURL)
	// The build registry refuses to register a build it cannot identify, and a
	// `go test` binary carries no VCS stamp.
	t.Setenv("BUILD_GIT_HASH", "integration-test")

	service, err := newPriceFetcher(ctx, temporal.Dependencies{Pool: pool, Logger: testutil.DiscardLogger()})
	if err != nil {
		t.Fatalf("wiring the price fetcher: %v", err)
	}

	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	env.RegisterActivity(&backfillActivities{service: service})
	return env
}

// newWorkflowEnv drives the real composition root: it registers through register()
// itself, so the workflow type name and activity wiring under test are the ones
// the deployed worker installs, not a re-declaration of them.
func newWorkflowEnv(t *testing.T, ctx context.Context, pool *pgxpool.Pool, baseURL string) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("COINGECKO_API_KEY", "test-api-key")
	t.Setenv("COINGECKO_BASE_URL", baseURL)
	t.Setenv("BUILD_GIT_HASH", "integration-test")

	env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
	deps := temporal.Dependencies{Pool: pool, Logger: testutil.DiscardLogger()}
	if err := register(ctx, deps, env); err != nil {
		t.Fatalf("running the production registration: %v", err)
	}
	return env
}

// fetchChunk runs the activity and decodes the number of points it stored.
func fetchChunk(t *testing.T, env *testsuite.TestActivityEnvironment, w chunkWindow) (int, error) {
	t.Helper()

	var activities *backfillActivities
	encoded, err := env.ExecuteActivity(activities.FetchChunk, w)
	if err != nil {
		return 0, err
	}

	var stored int
	if err := encoded.Get(&stored); err != nil {
		t.Fatalf("decoding the activity result: %v", err)
	}
	return stored, nil
}

func countPrices(t *testing.T, ctx context.Context, pool *pgxpool.Pool, tokenID int64) int {
	t.Helper()

	var n int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM offchain_token_price WHERE token_id = $1`, tokenID).Scan(&n); err != nil {
		t.Fatalf("counting stored prices: %v", err)
	}
	return n
}

// The Workflow Type is an operator-facing contract: the runbook tells the on-call
// to start `--type OffchainPriceBackfill`, and Temporal resolves it by string. So
// the literal is spelled out here rather than referencing workflowTypeName —
// using the constant would rename both sides together and pin nothing.
func TestIntegration_Register_ExposesTheDocumentedWorkflowType(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := coinGeckoFixtureServer(t)
	t.Cleanup(server.Close)

	tokenID := seedWETHAsset(t, ctx, pool)
	env := newWorkflowEnv(t, ctx, pool, server.URL)

	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	env.ExecuteWorkflow("OffchainPriceBackfill", BackfillParams{
		Assets: []string{"weth"},
		From:   from,
		To:     from.Add(24 * time.Hour),
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected the workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("running the workflow by its documented type name: %v", err)
	}
	if got := countPrices(t, ctx, pool, tokenID); got == 0 {
		t.Error("the run stored no prices, so it did not reach the real activity")
	}
}

// seedTokenlessAsset registers a catalog row with no token (tokenless = true) and
// returns its id. Idempotent: the migration already seeds ripple/hyperliquid.
func seedTokenlessAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceAssetID, symbol string) int64 {
	t.Helper()

	var sourceID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID); err != nil {
		t.Fatalf("looking up the coingecko source: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, tokenless, symbol, name, enabled, created_at, updated_at)
		VALUES ($1, $2, NULL, true, $3, $3, true, NOW(), NOW())
		ON CONFLICT (source_id, source_asset_id) DO NOTHING
	`, sourceID, sourceAssetID, symbol); err != nil {
		t.Fatalf("seeding tokenless price asset: %v", err)
	}
	var assetID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM offchain_price_asset WHERE source_id = $1 AND source_asset_id = $2`,
		sourceID, sourceAssetID).Scan(&assetID); err != nil {
		t.Fatalf("reading back tokenless price asset: %v", err)
	}
	return assetID
}

// The same operator-facing workflow must serve a token-less asset end to end:
// the catalog row declares tokenless, the activity routes the points through
// UpsertAssetPrices, and the rows land in asset_price — the path VEC-652 adds
// for XRP/HYPE. Driven through the production register(), like the WETH test.
func TestIntegration_Backfill_TokenlessAssetLandsInAssetPrice(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := coinGeckoFixtureServer(t)
	t.Cleanup(server.Close)

	assetID := seedTokenlessAsset(t, ctx, pool, "ripple", "XRP")
	env := newWorkflowEnv(t, ctx, pool, server.URL)

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	env.ExecuteWorkflow("OffchainPriceBackfill", BackfillParams{
		Assets: []string{"ripple"},
		From:   from,
		To:     from.Add(24 * time.Hour),
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected the workflow to complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("running the workflow for a token-less asset: %v", err)
	}

	// 25 points (inclusive 24h window) with the fixture's values read back, so a
	// column transposition cannot pass as success.
	var rows int
	var firstPrice, firstMarketCap float64
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*) OVER (), price_usd, market_cap_usd
		FROM asset_price WHERE asset_id = $1
		ORDER BY timestamp ASC LIMIT 1`, assetID,
	).Scan(&rows, &firstPrice, &firstMarketCap); err != nil {
		t.Fatalf("reading stored asset prices: %v", err)
	}
	if rows != 25 {
		t.Errorf("rows in asset_price = %d, want 25 hourly points", rows)
	}
	if firstPrice != 1500.0 {
		t.Errorf("first price = %v, want the fixture's 1500.0", firstPrice)
	}
	if firstMarketCap != 1500.0*1e6 {
		t.Errorf("first market cap = %v, want the fixture's 1.5e9", firstMarketCap)
	}
	var tokenRows int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price`).Scan(&tokenRows); err != nil {
		t.Fatalf("counting token-keyed prices: %v", err)
	}
	if tokenRows != 0 {
		t.Errorf("offchain_token_price gained %d rows for a token-less run", tokenRows)
	}
}

func TestIntegration_FetchChunk_StoresHourlyPrices(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := coinGeckoFixtureServer(t)
	t.Cleanup(server.Close)

	tokenID := seedWETHAsset(t, ctx, pool)
	env := newActivityEnv(t, ctx, pool, server.URL)

	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	window := chunkWindow{Asset: "weth", From: from, To: from.Add(48 * time.Hour)}

	stored, err := fetchChunk(t, env, window)
	if err != nil {
		t.Fatalf("FetchChunk: %v", err)
	}

	// 49, not 48: the window is inclusive at both ends, matching the live API.
	if stored != 49 {
		t.Errorf("stored = %d, want 49 hourly points", stored)
	}
	if got := countPrices(t, ctx, pool, tokenID); got != 49 {
		t.Errorf("rows in offchain_token_price = %d, want 49", got)
	}
}

// Re-running the same window must add nothing: an operator will overlap ranges,
// and Temporal retries activities, so the write path has to be idempotent.
func TestIntegration_FetchChunk_IsIdempotentAcrossRuns(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := coinGeckoFixtureServer(t)
	t.Cleanup(server.Close)

	tokenID := seedWETHAsset(t, ctx, pool)
	env := newActivityEnv(t, ctx, pool, server.URL)

	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	window := chunkWindow{Asset: "weth", From: from, To: from.Add(24 * time.Hour)}

	if _, err := fetchChunk(t, env, window); err != nil {
		t.Fatalf("first FetchChunk: %v", err)
	}
	afterFirst := countPrices(t, ctx, pool, tokenID)
	// Without this the test passes vacuously on 0 == 0 if the fixture or the write
	// path ever stops producing rows, proving nothing about idempotency.
	if afterFirst != 25 {
		t.Fatalf("first run stored %d rows, want 25; the comparison below would prove nothing", afterFirst)
	}

	if _, err := fetchChunk(t, env, window); err != nil {
		t.Fatalf("second FetchChunk: %v", err)
	}

	if afterSecond := countPrices(t, ctx, pool, tokenID); afterSecond != afterFirst {
		t.Errorf("row count changed on re-run: %d then %d; the write path is not idempotent",
			afterFirst, afterSecond)
	}
}

// A mistyped CoinGecko ID resolves to no row in offchain_price_asset. It must fail
// rather than look like a clean run that had nothing to do.
func TestIntegration_FetchChunk_ErrorsOnUnregisteredAsset(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := coinGeckoFixtureServer(t)
	t.Cleanup(server.Close)

	wethTokenID := seedWETHAsset(t, ctx, pool)
	env := newActivityEnv(t, ctx, pool, server.URL)

	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := fetchChunk(t, env, chunkWindow{
		Asset: "not-a-registered-coin", From: from, To: from.Add(24 * time.Hour),
	})

	if err == nil {
		t.Fatal("expected an error for a CoinGecko ID that is not registered")
	}
	// Erroring is only half of it: a rejected ID must not have written anything
	// under some other token on the way to failing.
	if got := countPrices(t, ctx, pool, wethTokenID); got != 0 {
		t.Errorf("stored %d rows while failing on an unregistered ID, want 0", got)
	}
}

// The window cap is a correctness bound, not a preference: CoinGecko silently
// drops to daily resolution beyond it.
func TestIntegration_FetchChunk_RejectsWindowWiderThanHourlyLimit(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	server := coinGeckoFixtureServer(t)
	t.Cleanup(server.Close)

	tokenID := seedWETHAsset(t, ctx, pool)
	env := newActivityEnv(t, ctx, pool, server.URL)

	from := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	_, err := fetchChunk(t, env, chunkWindow{
		Asset: "weth", From: from, To: from.AddDate(1, 0, 0),
	})

	if err == nil {
		t.Fatal("expected an error for a window wider than the hourly-resolution limit")
	}
	if got := countPrices(t, ctx, pool, tokenID); got != 0 {
		t.Errorf("stored %d rows for a rejected window, want 0", got)
	}
}
