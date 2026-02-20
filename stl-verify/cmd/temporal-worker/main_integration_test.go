//go:build integration

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"

	temporaladapter "github.com/archon-research/stl/stl-verify/internal/adapters/inbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Mock CoinGecko server
// ---------------------------------------------------------------------------

func startMockCoinGecko(t *testing.T) *httptest.Server {
	t.Helper()

	mux := http.NewServeMux()

	mux.HandleFunc("/simple/price", func(w http.ResponseWriter, r *http.Request) {
		ids := r.URL.Query().Get("ids")

		response := make(map[string]map[string]any)
		for _, id := range splitIDs(ids) {
			response[id] = map[string]any{
				"usd":             mockPrice(id),
				"usd_market_cap":  mockMarketCap(id),
				"last_updated_at": time.Now().Unix(),
			}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "failed to encode mock CoinGecko response", http.StatusInternalServerError)
			t.Fatalf("encoding mock CoinGecko response: %v", err)
		}
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
	default:
		return 1000000000.0
	}
}

// ---------------------------------------------------------------------------
// Seed helpers
// ---------------------------------------------------------------------------

func seedTestData(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	addressBytes, err := testutil.HexToBytes("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	require.NoError(t, err)

	_, err = pool.Exec(ctx, `
		INSERT INTO token (id, chain_id, address, symbol, decimals, updated_at)
		VALUES ($1, $2, $3, $4, 18, NOW())
		ON CONFLICT (id) DO NOTHING
	`, 1, 1, addressBytes, "WETH")
	require.NoError(t, err)

	var sourceID int64
	err = pool.QueryRow(ctx, `SELECT id FROM offchain_price_source WHERE name = 'coingecko'`).Scan(&sourceID)
	require.NoError(t, err, "coingecko source must be seeded by migrations")

	_, err = pool.Exec(ctx, `
		INSERT INTO offchain_price_asset (source_id, source_asset_id, token_id, symbol, name, enabled, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, true, NOW(), NOW())
		ON CONFLICT (source_id, source_asset_id) DO UPDATE SET token_id = $3
	`, sourceID, "ethereum", 1, "WETH", "Wrapped Ether")
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_HappyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start Temporal dev server
	devServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ClientOptions: &client.Options{Namespace: "sentinel"},
		LogLevel:      "error",
	})
	require.NoError(t, err, "failed to start Temporal dev server (requires temporal CLI)")
	t.Cleanup(func() { devServer.Stop() })

	// Start TimescaleDB via testcontainers
	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(dbCleanup)

	// Seed test data
	seedTestData(t, ctx, pool)

	// Start mock CoinGecko server
	coingeckoServer := startMockCoinGecko(t)
	t.Cleanup(coingeckoServer.Close)

	// Configure environment for run()
	t.Setenv("TEMPORAL_HOST_PORT", devServer.FrontendHostPort())
	t.Setenv("TEMPORAL_NAMESPACE", "sentinel")
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("COINGECKO_API_KEY", "test-api-key")
	t.Setenv("COINGECKO_BASE_URL", coingeckoServer.URL)
	t.Setenv("CHAIN_ID", "1")

	// Run worker in goroutine
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(workerCtx)
	}()

	// Create a separate Temporal client for testing
	tc, err := client.Dial(client.Options{
		HostPort:  devServer.FrontendHostPort(),
		Namespace: "sentinel",
	})
	require.NoError(t, err)
	defer tc.Close()

	// Wait for worker to register and start polling the task queue.
	require.Eventually(t, func() bool {
		resp, err := tc.DescribeTaskQueue(ctx, temporaladapter.TaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		return err == nil && len(resp.Pollers) > 0
	}, 15*time.Second, 250*time.Millisecond, "worker did not start polling within 15s")

	workflowRun, err := tc.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        "test-price-fetch",
		TaskQueue: temporaladapter.TaskQueue,
	}, temporaladapter.PriceFetchWorkflow, temporaladapter.PriceFetchWorkflowInput{
		AssetIDs: []string{"ethereum"},
	})
	require.NoError(t, err)

	// Wait for workflow to complete
	var result temporaladapter.PriceFetchWorkflowOutput
	err = workflowRun.Get(ctx, &result)
	require.NoError(t, err)
	assert.True(t, result.Success)

	// Verify price was saved to DB
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM offchain_token_price`).Scan(&priceCount)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, priceCount, 1, "expected at least 1 price record in DB")

	// Cancel worker and verify graceful shutdown
	workerCancel()

	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not shut down within 10 seconds")
	}
}

func TestRunIntegration_StartupErrors(t *testing.T) {
	tests := []struct {
		name        string
		env         map[string]string
		errContains string
	}{
		{
			name: "bad database URL",
			env: map[string]string{
				"TEMPORAL_HOST_PORT": "localhost:7233",
				"DATABASE_URL":       "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
				"COINGECKO_API_KEY":  "test-api-key",
				"CHAIN_ID":           "1",
			},
			errContains: "database",
		},
		{
			name: "missing CoinGecko API key",
			env: map[string]string{
				"TEMPORAL_HOST_PORT": "localhost:7233",
				"DATABASE_URL":       "postgres://postgres:postgres@localhost:5432/test?connect_timeout=1",
				"COINGECKO_API_KEY":  "",
				"CHAIN_ID":           "1",
			},
			errContains: "COINGECKO_API_KEY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			err := run(ctx)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}
