//go:build integration

package offchain_price_fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// setupMockCoinGeckoServer creates an HTTP test server that simulates CoinGecko API responses.
func setupMockCoinGeckoServer(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	// /simple/price endpoint - current prices
	mux.HandleFunc("/simple/price", func(w http.ResponseWriter, r *http.Request) {
		ids := r.URL.Query().Get("ids")
		now := time.Now().Unix()

		// Return mock prices for requested IDs
		response := make(map[string]map[string]any)
		for _, id := range splitIDs(ids) {
			response[id] = map[string]any{
				"usd":             mockPrice(id),
				"usd_market_cap":  mockMarketCap(id),
				"last_updated_at": now,
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	// /coins/{id}/market_chart/range endpoint - historical data
	mux.HandleFunc("/coins/", func(w http.ResponseWriter, r *http.Request) {
		// Parse the coin ID from path like /coins/ethereum/market_chart/range
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")

		var fromTime, toTime int64
		fmt.Sscanf(from, "%d", &fromTime)
		fmt.Sscanf(to, "%d", &toTime)

		// Generate hourly data points
		response := struct {
			Prices       [][]float64 `json:"prices"`
			MarketCaps   [][]float64 `json:"market_caps"`
			TotalVolumes [][]float64 `json:"total_volumes"`
		}{
			Prices:       make([][]float64, 0),
			MarketCaps:   make([][]float64, 0),
			TotalVolumes: make([][]float64, 0),
		}

		// Generate hourly data points from 'from' to 'to'
		for ts := fromTime; ts <= toTime; ts += 3600 {
			tsMs := float64(ts * 1000)
			response.Prices = append(response.Prices, []float64{tsMs, 1000.0 + float64(ts%1000)})
			response.MarketCaps = append(response.MarketCaps, []float64{tsMs, 50000000000.0})
			response.TotalVolumes = append(response.TotalVolumes, []float64{tsMs, 1000000000.0})
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
	})

	return httptest.NewServer(mux)
}

func splitIDs(ids string) []string {
	if ids == "" {
		return nil
	}
	return strings.Split(ids, ",")
}

func mockPrice(id string) float64 {
	switch id {
	case "ethereum":
		return 3500.50
	case "bitcoin":
		return 65000.00
	case "usd-coin":
		return 1.00
	case "dai":
		return 1.00
	default:
		return 100.0
	}
}

func mockMarketCap(id string) float64 {
	switch id {
	case "ethereum":
		return 420000000000.0
	case "bitcoin":
		return 1200000000000.0
	case "usd-coin":
		return 25000000000.0
	case "dai":
		return 5000000000.0
	default:
		return 1000000000.0
	}
}

// insertTestToken inserts a test token record for linking price assets.
func insertTestToken(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id int64, chainID int, address, symbol string) {
	t.Helper()
	// Convert hex address string to bytes (remove 0x prefix)
	addressBytes, err := testutil.HexToBytes(address)
	if err != nil {
		t.Fatalf("failed to parse address %s: %v", address, err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO token (id, chain_id, address, symbol, decimals, updated_at)
		VALUES ($1, $2, $3, $4, 18, NOW())
		ON CONFLICT (id) DO NOTHING
	`, id, chainID, addressBytes, symbol)
	if err != nil {
		t.Fatalf("failed to insert test token: %v", err)
	}
}

// insertTestPriceAsset links a CoinGecko asset ID to a token.
func insertTestPriceAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceID int64, sourceAssetID string, tokenID int64, symbol, name string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, symbol, name, enabled, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, true, NOW(), NOW())
		ON CONFLICT (source_id, source_asset_id) DO UPDATE SET token_id = $3
	`, sourceID, sourceAssetID, tokenID, symbol, name)
	if err != nil {
		t.Fatalf("failed to insert test price asset: %v", err)
	}
}

func TestIntegration_FetchCurrentPrices(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID from migration seed data
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID)
	if err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}

	// Insert test token
	insertTestToken(t, ctx, pool, 1, 1, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH")

	// Link it to CoinGecko's ethereum ID
	insertTestPriceAsset(t, ctx, pool, sourceID, "ethereum", 1, "WETH", "Wrapped Ether")

	// Create CoinGecko client pointing to mock server
	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         mockServer.URL,
		RateLimitPerMin: 10000, // High limit for tests
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}

	// Create repository
	repo, err := postgres.NewPriceRepository(pool, nil, 0, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	// Create service
	service, err := NewService(ServiceConfig{
		ChainID:     1,
		Concurrency: 2,
	}, client, repo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Fetch current prices for ethereum
	err = service.FetchCurrentPrices(ctx, []string{"ethereum"})
	if err != nil {
		t.Fatalf("FetchCurrentPrices failed: %v", err)
	}

	// Verify price was stored
	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price WHERE token_id = 1`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query token_price: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 price record, got %d", count)
	}

	// Verify price value
	var priceUSD float64
	err = pool.QueryRow(ctx, `SELECT price_usd FROM offchain_token_price WHERE token_id = 1 ORDER BY timestamp DESC LIMIT 1`).Scan(&priceUSD)
	if err != nil {
		t.Fatalf("failed to query price_usd: %v", err)
	}

	if priceUSD != 3500.50 {
		t.Errorf("expected price 3500.50, got %f", priceUSD)
	}
}

func TestIntegration_FetchCurrentPrices_AllEnabledAssets(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID)
	if err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}

	// Count enabled assets from seed data (migration seeds SparkLend tokens)
	var enabledAssetCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_price_asset WHERE source_id = $1 AND enabled = true AND token_id IS NOT NULL`, sourceID).Scan(&enabledAssetCount)
	if err != nil {
		t.Fatalf("failed to count enabled assets: %v", err)
	}

	// Create client and service
	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         mockServer.URL,
		RateLimitPerMin: 10000,
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}

	repo, err := postgres.NewPriceRepository(pool, nil, 0, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := NewService(ServiceConfig{
		ChainID:     1,
		Concurrency: 2,
	}, client, repo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Fetch prices for all enabled assets (empty slice = all)
	err = service.FetchCurrentPrices(ctx, nil)
	if err != nil {
		t.Fatalf("FetchCurrentPrices failed: %v", err)
	}

	// Verify prices were stored for all enabled assets
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price`).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	// Should have one price record for each enabled asset with a token_id
	if priceCount != enabledAssetCount {
		t.Errorf("expected %d price records (one per enabled asset), got %d", enabledAssetCount, priceCount)
	}

	// Verify at least the expected minimum (SparkLend seeds 18 tokens)
	if priceCount < 15 {
		t.Errorf("expected at least 15 price records from seed data, got %d", priceCount)
	}

	// The migration also seeds two token-less assets (ripple, hyperliquid);
	// the same sweep must land their prices in the asset-keyed store.
	var assetPriceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_asset_price`).Scan(&assetPriceCount)
	if err != nil {
		t.Fatalf("failed to query asset_price count: %v", err)
	}
	if assetPriceCount != 2 {
		t.Errorf("expected 2 asset-keyed price records (ripple, hyperliquid), got %d", assetPriceCount)
	}
}

func TestIntegration_FetchHistoricalData(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID)
	if err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}

	// Insert test token
	insertTestToken(t, ctx, pool, 1, 1, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH")
	insertTestPriceAsset(t, ctx, pool, sourceID, "ethereum", 1, "WETH", "Wrapped Ether")

	// Create client and service
	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         mockServer.URL,
		RateLimitPerMin: 10000,
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}

	repo, err := postgres.NewPriceRepository(pool, nil, 0, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := NewService(ServiceConfig{
		ChainID:     1,
		Concurrency: 2,
	}, client, repo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Fetch 3 days of historical data
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 3, 23, 59, 59, 0, time.UTC)

	err = service.FetchHistoricalData(ctx, []string{"ethereum"}, from, to)
	if err != nil {
		t.Fatalf("FetchHistoricalData failed: %v", err)
	}

	// Verify prices were stored (3 days * 24 hours = ~72 data points)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price WHERE token_id = 1`).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	if priceCount < 70 {
		t.Errorf("expected at least 70 price records, got %d", priceCount)
	}

	// Verify volumes were stored alongside prices
	var volumeCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price WHERE token_id = 1 AND volume_usd IS NOT NULL`).Scan(&volumeCount)
	if err != nil {
		t.Fatalf("failed to query volume count: %v", err)
	}

	if volumeCount < 70 {
		t.Errorf("expected at least 70 price records with volume_usd, got %d", volumeCount)
	}
}

func TestIntegration_FetchHistoricalData_MultipleAssetsConcurrently(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID)
	if err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}

	// Insert multiple test tokens
	insertTestToken(t, ctx, pool, 1, 1, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH")
	insertTestToken(t, ctx, pool, 2, 1, "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "WBTC")

	insertTestPriceAsset(t, ctx, pool, sourceID, "ethereum", 1, "WETH", "Wrapped Ether")
	insertTestPriceAsset(t, ctx, pool, sourceID, "bitcoin", 2, "WBTC", "Wrapped Bitcoin")

	// Create client and service with concurrency = 2
	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         mockServer.URL,
		RateLimitPerMin: 10000,
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}

	repo, err := postgres.NewPriceRepository(pool, nil, 0, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := NewService(ServiceConfig{
		ChainID:     1,
		Concurrency: 2, // Fetch both assets concurrently
	}, client, repo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Fetch 2 days of historical data for both assets
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 2, 23, 59, 59, 0, time.UTC)

	err = service.FetchHistoricalData(ctx, []string{"ethereum", "bitcoin"}, from, to)
	if err != nil {
		t.Fatalf("FetchHistoricalData failed: %v", err)
	}

	// Verify both assets have prices stored (token_id 1=WETH, 2=WBTC)
	for _, tokenID := range []int64{1, 2} {
		var count int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price WHERE token_id = $1`, tokenID).Scan(&count)
		if err != nil {
			t.Fatalf("failed to query token_price count for token_id %d: %v", tokenID, err)
		}

		if count < 40 {
			t.Errorf("expected at least 40 price records for token_id %d, got %d", tokenID, count)
		}
	}
}

func TestIntegration_UpsertIdempotency(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID)
	if err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}

	// Insert test token
	insertTestToken(t, ctx, pool, 1, 1, "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH")
	insertTestPriceAsset(t, ctx, pool, sourceID, "ethereum", 1, "WETH", "Wrapped Ether")

	// Create client and service
	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         mockServer.URL,
		RateLimitPerMin: 10000,
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}

	repo, err := postgres.NewPriceRepository(pool, nil, 0, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := NewService(ServiceConfig{
		ChainID:     1,
		Concurrency: 2,
	}, client, repo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Fetch historical data
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	err = service.FetchHistoricalData(ctx, []string{"ethereum"}, from, to)
	if err != nil {
		t.Fatalf("FetchHistoricalData (first) failed: %v", err)
	}

	// Get count after first fetch
	var countAfterFirst int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price WHERE token_id = 1`).Scan(&countAfterFirst)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	// Fetch again (should be idempotent due to ON CONFLICT DO NOTHING)
	err = service.FetchHistoricalData(ctx, []string{"ethereum"}, from, to)
	if err != nil {
		t.Fatalf("FetchHistoricalData (second) failed: %v", err)
	}

	// Count should be the same (no duplicates)
	var countAfterSecond int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price WHERE token_id = 1`).Scan(&countAfterSecond)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	if countAfterFirst != countAfterSecond {
		t.Errorf("expected idempotent upsert: first=%d, second=%d", countAfterFirst, countAfterSecond)
	}
}

// insertTestTokenlessPriceAsset registers a CoinGecko asset with no token row
// (token_id NULL) and returns its id. Idempotent: the migration may already
// seed the same source_asset_id.
func insertTestTokenlessPriceAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceID int64, sourceAssetID, symbol, name string) int64 {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, symbol, name, enabled, created_at, updated_at)
		VALUES ($1, $2, NULL, $3, $4, true, NOW(), NOW())
		ON CONFLICT (source_id, source_asset_id) DO NOTHING
	`, sourceID, sourceAssetID, symbol, name)
	if err != nil {
		t.Fatalf("failed to insert token-less price asset: %v", err)
	}
	var assetID int64
	if err := pool.QueryRow(ctx,
		`SELECT id FROM offchain_price_asset WHERE source_id = $1 AND source_asset_id = $2`,
		sourceID, sourceAssetID,
	).Scan(&assetID); err != nil {
		t.Fatalf("failed to read back token-less price asset: %v", err)
	}
	return assetID
}

func newIntegrationService(t *testing.T, pool *pgxpool.Pool, baseURL string) *Service {
	t.Helper()
	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         baseURL,
		RateLimitPerMin: 10000,
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}
	repo, err := postgres.NewPriceRepository(pool, nil, 0, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}
	service, err := NewService(ServiceConfig{ChainID: 1, Concurrency: 2}, client, repo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	return service
}

func TestIntegration_FetchCurrentPrices_TokenlessAsset(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	var sourceID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID); err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}
	assetID := insertTestTokenlessPriceAsset(t, ctx, pool, sourceID, "ripple", "XRP", "XRP")

	service := newIntegrationService(t, pool, mockServer.URL)

	if err := service.FetchCurrentPrices(ctx, []string{"ripple"}); err != nil {
		t.Fatalf("FetchCurrentPrices failed: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_asset_price WHERE asset_id = $1`, assetID).Scan(&count); err != nil {
		t.Fatalf("failed to query offchain_asset_price: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 asset price record, got %d", count)
	}
}

func TestIntegration_AssetPriceUpsertIdempotency(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	var sourceID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID); err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}
	assetID := insertTestTokenlessPriceAsset(t, ctx, pool, sourceID, "hyperliquid", "HYPE", "Hyperliquid")

	service := newIntegrationService(t, pool, mockServer.URL)

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	if err := service.FetchHistoricalData(ctx, []string{"hyperliquid"}, from, to); err != nil {
		t.Fatalf("FetchHistoricalData (first) failed: %v", err)
	}
	var countAfterFirst int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_asset_price WHERE asset_id = $1`, assetID).Scan(&countAfterFirst); err != nil {
		t.Fatalf("failed to query offchain_asset_price count: %v", err)
	}
	if countAfterFirst == 0 {
		t.Fatal("expected historical asset prices to be stored")
	}

	if err := service.FetchHistoricalData(ctx, []string{"hyperliquid"}, from, to); err != nil {
		t.Fatalf("FetchHistoricalData (second) failed: %v", err)
	}
	var countAfterSecond int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_asset_price WHERE asset_id = $1`, assetID).Scan(&countAfterSecond); err != nil {
		t.Fatalf("failed to query offchain_asset_price count: %v", err)
	}
	if countAfterFirst != countAfterSecond {
		t.Errorf("expected idempotent upsert: first=%d, second=%d", countAfterFirst, countAfterSecond)
	}
}

// A replay from a different build must land correction rows at
// processing_version+1, not vanish into ON CONFLICT DO NOTHING — the contract
// next_processing_version_offchain_asset_price exists to keep (ADR-0002 §3).
func TestIntegration_AssetPriceCrossBuildReplayAppendsNewVersions(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	var sourceID int64
	if err := pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID); err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}
	assetID := insertTestTokenlessPriceAsset(t, ctx, pool, sourceID, "ripple", "XRP", "XRP")

	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         mockServer.URL,
		RateLimitPerMin: 10000,
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}

	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 1, 6, 0, 0, 0, time.UTC)

	runWithBuild := func(buildID int) {
		t.Helper()
		repo, err := postgres.NewPriceRepository(pool, nil, buildregistry.BuildID(buildID), 100)
		if err != nil {
			t.Fatalf("failed to create price repository: %v", err)
		}
		service, err := NewService(ServiceConfig{ChainID: 1, Concurrency: 2}, client, repo)
		if err != nil {
			t.Fatalf("failed to create service: %v", err)
		}
		if err := service.FetchHistoricalData(ctx, []string{"ripple"}, from, to); err != nil {
			t.Fatalf("FetchHistoricalData (build %d) failed: %v", buildID, err)
		}
	}

	runWithBuild(0)
	var v0Count int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_asset_price WHERE asset_id = $1`, assetID).Scan(&v0Count); err != nil {
		t.Fatalf("failed to count rows: %v", err)
	}
	if v0Count == 0 {
		t.Fatal("expected the first build's rows to be stored")
	}

	runWithBuild(1)
	var total, maxVersion int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*), MAX(processing_version) FROM offchain_asset_price WHERE asset_id = $1`, assetID,
	).Scan(&total, &maxVersion); err != nil {
		t.Fatalf("failed to count rows after replay: %v", err)
	}
	if total != 2*v0Count {
		t.Errorf("expected the replay to append a full second version set: first=%d, total=%d", v0Count, total)
	}
	if maxVersion != 1 {
		t.Errorf("expected max processing_version 1 after a new-build replay, got %d", maxVersion)
	}
}

func TestIntegration_NoEnabledAssets(t *testing.T) {
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Don't insert any price assets - database has seed data but no tokens linked

	// Create client and service
	client, err := coingecko.NewClient(coingecko.ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         mockServer.URL,
		RateLimitPerMin: 10000,
	})
	if err != nil {
		t.Fatalf("failed to create coingecko client: %v", err)
	}

	repo, err := postgres.NewPriceRepository(pool, nil, 0, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := NewService(ServiceConfig{
		ChainID:     1,
		Concurrency: 2,
	}, client, repo)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}

	// Fetch for a non-existent asset ID
	err = service.FetchCurrentPrices(ctx, []string{"nonexistent-coin"})
	if err != nil {
		t.Fatalf("FetchCurrentPrices should not error with no matching assets: %v", err)
	}

	// Verify no prices stored
	var count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 price records, got %d", count)
	}
}
