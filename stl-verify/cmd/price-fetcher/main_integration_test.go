//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coingecko"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/services/price_fetcher"
)

// setupTestDatabase creates a PostgreSQL container with TimescaleDB and returns a connection pool.
func setupTestDatabase(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "test",
			"POSTGRES_PASSWORD": "test",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://test:test@%s:%s/testdb?sslmode=disable", host, port.Port())

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to connect to database: %v", err)
	}

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := pool.Ping(ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Run migrations
	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	cleanup := func() {
		pool.Close()
		container.Terminate(ctx)
	}

	return pool, cleanup
}

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
	var result []string
	start := 0
	for i := 0; i <= len(ids); i++ {
		if i == len(ids) || ids[i] == ',' {
			if start < i {
				result = append(result, ids[start:i])
			}
			start = i + 1
		}
	}
	return result
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
	addressBytes, err := hexToBytes(address)
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

// hexToBytes converts a hex string (with or without 0x prefix) to bytes.
func hexToBytes(s string) ([]byte, error) {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		s = s[2:]
	}
	if len(s)%2 != 0 {
		s = "0" + s
	}
	result := make([]byte, len(s)/2)
	for i := 0; i < len(result); i++ {
		b, err := hexByte(s[i*2], s[i*2+1])
		if err != nil {
			return nil, err
		}
		result[i] = b
	}
	return result, nil
}

func hexByte(hi, lo byte) (byte, error) {
	h, err := hexNibble(hi)
	if err != nil {
		return 0, err
	}
	l, err := hexNibble(lo)
	if err != nil {
		return 0, err
	}
	return h<<4 | l, nil
}

func hexNibble(c byte) (byte, error) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', nil
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, nil
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, nil
	default:
		return 0, fmt.Errorf("invalid hex character: %c", c)
	}
}

// insertTestPriceAsset links a CoinGecko asset ID to a token.
func insertTestPriceAsset(t *testing.T, ctx context.Context, pool *pgxpool.Pool, sourceID int64, sourceAssetID string, tokenID int64, symbol, name string) {
	t.Helper()
	_, err := pool.Exec(ctx, `
		INSERT INTO price_asset (source_id, source_asset_id, token_id, symbol, name, enabled, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, true, NOW(), NOW())
		ON CONFLICT (source_id, source_asset_id) DO UPDATE SET token_id = $3
	`, sourceID, sourceAssetID, tokenID, symbol, name)
	if err != nil {
		t.Fatalf("failed to insert test price asset: %v", err)
	}
}

func TestIntegration_FetchCurrentPrices(t *testing.T) {
	pool, cleanup := setupTestDatabase(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID from migration seed data
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM price_source WHERE name = 'coingecko'`).Scan(&sourceID)
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
	repo, err := postgres.NewPriceRepository(pool, nil, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	// Create service
	service, err := price_fetcher.NewService(price_fetcher.ServiceConfig{
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_price WHERE source_asset_id = 'ethereum'`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query token_price: %v", err)
	}

	if count != 1 {
		t.Errorf("expected 1 price record, got %d", count)
	}

	// Verify price value
	var priceUSD float64
	err = pool.QueryRow(ctx, `SELECT price_usd FROM token_price WHERE source_asset_id = 'ethereum'`).Scan(&priceUSD)
	if err != nil {
		t.Fatalf("failed to query price_usd: %v", err)
	}

	if priceUSD != 3500.50 {
		t.Errorf("expected price 3500.50, got %f", priceUSD)
	}
}

func TestIntegration_FetchCurrentPrices_AllEnabledAssets(t *testing.T) {
	pool, cleanup := setupTestDatabase(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM price_source WHERE name = 'coingecko'`).Scan(&sourceID)
	if err != nil {
		t.Fatalf("failed to get coingecko source: %v", err)
	}

	// Count enabled assets from seed data (migration seeds SparkLend tokens)
	var enabledAssetCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM price_asset WHERE source_id = $1 AND enabled = true AND token_id IS NOT NULL`, sourceID).Scan(&enabledAssetCount)
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

	repo, err := postgres.NewPriceRepository(pool, nil, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := price_fetcher.NewService(price_fetcher.ServiceConfig{
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_price`).Scan(&priceCount)
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
}

func TestIntegration_FetchHistoricalData(t *testing.T) {
	pool, cleanup := setupTestDatabase(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM price_source WHERE name = 'coingecko'`).Scan(&sourceID)
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

	repo, err := postgres.NewPriceRepository(pool, nil, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := price_fetcher.NewService(price_fetcher.ServiceConfig{
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_price WHERE source_asset_id = 'ethereum'`).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	if priceCount < 70 {
		t.Errorf("expected at least 70 price records, got %d", priceCount)
	}

	// Verify volumes were stored
	var volumeCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_volume WHERE source_asset_id = 'ethereum'`).Scan(&volumeCount)
	if err != nil {
		t.Fatalf("failed to query token_volume count: %v", err)
	}

	if volumeCount < 70 {
		t.Errorf("expected at least 70 volume records, got %d", volumeCount)
	}
}

func TestIntegration_FetchHistoricalData_MultipleAssetsConcurrently(t *testing.T) {
	pool, cleanup := setupTestDatabase(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM price_source WHERE name = 'coingecko'`).Scan(&sourceID)
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

	repo, err := postgres.NewPriceRepository(pool, nil, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := price_fetcher.NewService(price_fetcher.ServiceConfig{
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

	// Verify both assets have prices stored
	for _, assetID := range []string{"ethereum", "bitcoin"} {
		var count int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_price WHERE source_asset_id = $1`, assetID).Scan(&count)
		if err != nil {
			t.Fatalf("failed to query token_price count for %s: %v", assetID, err)
		}

		if count < 40 {
			t.Errorf("expected at least 40 price records for %s, got %d", assetID, count)
		}
	}
}

func TestIntegration_UpsertIdempotency(t *testing.T) {
	pool, cleanup := setupTestDatabase(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	mockServer := setupMockCoinGeckoServer(t)
	t.Cleanup(mockServer.Close)

	// Get the coingecko source ID
	var sourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM price_source WHERE name = 'coingecko'`).Scan(&sourceID)
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

	repo, err := postgres.NewPriceRepository(pool, nil, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := price_fetcher.NewService(price_fetcher.ServiceConfig{
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_price WHERE source_asset_id = 'ethereum'`).Scan(&countAfterFirst)
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_price WHERE source_asset_id = 'ethereum'`).Scan(&countAfterSecond)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	if countAfterFirst != countAfterSecond {
		t.Errorf("expected idempotent upsert: first=%d, second=%d", countAfterFirst, countAfterSecond)
	}
}

func TestIntegration_NoEnabledAssets(t *testing.T) {
	pool, cleanup := setupTestDatabase(t)
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

	repo, err := postgres.NewPriceRepository(pool, nil, 100)
	if err != nil {
		t.Fatalf("failed to create price repository: %v", err)
	}

	service, err := price_fetcher.NewService(price_fetcher.ServiceConfig{
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM token_price`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query token_price count: %v", err)
	}

	if count != 0 {
		t.Errorf("expected 0 price records, got %d", count)
	}
}

func TestIntegration_RunHelper_ParseAssetIDs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single asset",
			input:    "ethereum",
			expected: []string{"ethereum"},
		},
		{
			name:     "multiple assets",
			input:    "ethereum,bitcoin,usd-coin",
			expected: []string{"ethereum", "bitcoin", "usd-coin"},
		},
		{
			name:     "with spaces",
			input:    " ethereum , bitcoin , usd-coin ",
			expected: []string{"ethereum", "bitcoin", "usd-coin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseAssetIDs(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d assets, got %d", len(tt.expected), len(result))
				return
			}

			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("asset %d: expected %q, got %q", i, expected, result[i])
				}
			}
		})
	}
}

func TestIntegration_RunHelper_GetChainID(t *testing.T) {
	// Save original env and restore after test
	originalChainID := env.Get("CHAIN_ID", "")
	t.Cleanup(func() {
		if originalChainID != "" {
			t.Setenv("CHAIN_ID", originalChainID)
		}
	})

	// Test default value
	t.Setenv("CHAIN_ID", "")
	chainID, err := getChainID()
	if err != nil {
		t.Fatalf("getChainID failed: %v", err)
	}
	if chainID != 1 {
		t.Errorf("expected default chainID 1, got %d", chainID)
	}

	// Test custom value
	t.Setenv("CHAIN_ID", "42")
	chainID, err = getChainID()
	if err != nil {
		t.Fatalf("getChainID failed: %v", err)
	}
	if chainID != 42 {
		t.Errorf("expected chainID 42, got %d", chainID)
	}

	// Test invalid value
	t.Setenv("CHAIN_ID", "not-a-number")
	_, err = getChainID()
	if err == nil {
		t.Error("expected error for invalid CHAIN_ID")
	}
}
