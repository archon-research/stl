//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// mockAnchorageAPI returns a mock server that serves both /packages and /operations.
func mockAnchorageAPI(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := r.Header.Get("Api-Access-Key")
		if apiKey != "test-key" {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"errorType":"Forbidden","message":"invalid api key"}`)
			return
		}

		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.HasPrefix(r.URL.Path, "/v2/collateral_management/packages"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"active":         true,
						"packageId":      "pkg-test-001",
						"pledgorId":      "pledgor-1",
						"securedPartyId": "sp-1",
						"state":          "HEALTHY",
						"currentLtv":     "0.5",
						"exposureValue":  "50000000",
						"packageValue":   "100000000",
						"ltvTimestamp":   time.Now().UTC().Format(time.RFC3339),
						"collateralAssets": []map[string]interface{}{
							{
								"asset":         map[string]string{"assetType": "BTC", "type": "AnchorageCustody"},
								"price":         "100000",
								"quantity":      "1000",
								"weight":        "1",
								"weightedValue": "100000000",
							},
						},
						"marginCall":   map[string]string{"action": "PARTIAL", "ltv": "0.8", "returnToLtv": "0.7", "warningLtv": "0.75"},
						"critical":     map[string]interface{}{"action": "PARTIAL", "ltv": "0.9", "returnToLtv": "0.7", "warningLtv": "0.85", "defaultNotice": false},
						"marginReturn": map[string]string{"action": "NONE", "ltv": "0.6", "returnToLtv": "0.7"},
					},
				},
				"page": map[string]interface{}{"next": nil},
			})

		case strings.HasPrefix(r.URL.Path, "/v2/collateral_management/operations"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"data": []map[string]interface{}{
					{
						"id":        "op-test-001",
						"action":    "INITIAL_DEPOSIT",
						"type":      "COLLATERAL_PACKAGE",
						"typeId":    "pkg-test-001",
						"asset":     map[string]string{"assetType": "BTC", "type": "ANCHORAGECUSTODY"},
						"quantity":  "1000",
						"notes":     "Test deposit",
						"createdAt": "2025-12-19T12:00:00.000000Z",
						"updatedAt": "2025-12-19T12:00:00.000000Z",
					},
				},
				"page": map[string]interface{}{"next": nil},
			})

		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"errorType":"NotFound","message":"unknown endpoint"}`)
		}
	}))
}

// ---------------------------------------------------------------------------
// Integration tests
// ---------------------------------------------------------------------------

func TestRunIntegration_BadConnectionConfig(t *testing.T) {
	apiServer := mockAnchorageAPI(t)
	defer apiServer.Close()

	err := run(context.Background(), []string{
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
		"-api-url", apiServer.URL,
		"-api-key", "test-key",
		"-prime", "spark",
	})
	if err == nil {
		t.Fatal("expected error for bad connection config")
	}
	if !strings.Contains(err.Error(), "db") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected database connection error, got: %v", err)
	}
}

func TestRunIntegration_MissingAPIURL(t *testing.T) {
	err := run(context.Background(), []string{
		"-db", "postgres://localhost/test",
		"-api-key", "test-key",
	})
	if err == nil {
		t.Fatal("expected error for missing API URL")
	}
	if !strings.Contains(err.Error(), "API URL") {
		t.Errorf("expected API URL error, got: %v", err)
	}
}

func TestRunIntegration_MissingAPIKey(t *testing.T) {
	err := run(context.Background(), []string{
		"-db", "postgres://localhost/test",
		"-api-url", "https://api.example.com",
	})
	if err == nil {
		t.Fatal("expected error for missing API key")
	}
	if !strings.Contains(err.Error(), "API key") {
		t.Errorf("expected API key error, got: %v", err)
	}
}

func TestRunIntegration_InvalidPollInterval(t *testing.T) {
	_, _, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	apiServer := mockAnchorageAPI(t)
	defer apiServer.Close()

	err := run(context.Background(), []string{
		"-db", "postgres://localhost/test",
		"-api-url", apiServer.URL,
		"-api-key", "test-key",
		"-poll-interval", "not-a-duration",
	})
	if err == nil {
		t.Fatal("expected error for invalid poll interval")
	}
	if !strings.Contains(err.Error(), "poll interval") {
		t.Errorf("expected poll interval error, got: %v", err)
	}
}

func TestRunIntegration_InvalidPrime(t *testing.T) {
	ctx := context.Background()

	_, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	apiServer := mockAnchorageAPI(t)
	defer apiServer.Close()

	err := run(ctx, []string{
		"-db", dbURL,
		"-api-url", apiServer.URL,
		"-api-key", "test-key",
		"-prime", "nonexistent-prime",
	})
	if err == nil {
		t.Fatal("expected error for unknown prime")
	}
	if !strings.Contains(err.Error(), "nonexistent-prime") {
		t.Errorf("expected prime not found error, got: %v", err)
	}
}

func TestRunIntegration_StartupAndShutdown(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	// Seed a prime.
	_, err := pool.Exec(ctx, `
		INSERT INTO prime (name, vault_address)
		VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
		ON CONFLICT (name) DO NOTHING
	`)
	if err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	apiServer := mockAnchorageAPI(t)
	defer apiServer.Close()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx, []string{
			"-db", dbURL,
			"-api-url", apiServer.URL,
			"-api-key", "test-key",
			"-prime", "spark",
			"-poll-interval", "200ms",
		})
	}()

	// Wait for at least one snapshot and one operation to be written.
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var snapshotCount, opCount int
	for snapshotCount == 0 || opCount == 0 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for data (snapshots=%d, operations=%d)", snapshotCount, opCount)
		case err := <-errCh:
			t.Fatalf("run() returned early with error: %v", err)
		case <-ticker.C:
			pool.QueryRow(ctx, "SELECT count(*) FROM anchorage_package_snapshot").Scan(&snapshotCount)
			pool.QueryRow(ctx, "SELECT count(*) FROM anchorage_operation").Scan(&opCount)
		}
	}

	t.Logf("snapshots written: %d", snapshotCount)
	t.Logf("operations written: %d", opCount)

	// Trigger shutdown.
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() returned error on shutdown: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
}

func TestRunIntegration_BackfillMode(t *testing.T) {
	ctx := context.Background()

	pool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	// Seed a prime.
	_, err := pool.Exec(ctx, `
		INSERT INTO prime (name, vault_address)
		VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
		ON CONFLICT (name) DO NOTHING
	`)
	if err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	apiServer := mockAnchorageAPI(t)
	defer apiServer.Close()

	// Run in backfill mode — should fetch operations and exit.
	err = run(ctx, []string{
		"-db", dbURL,
		"-api-url", apiServer.URL,
		"-api-key", "test-key",
		"-prime", "spark",
		"--backfill",
	})
	if err != nil {
		t.Fatalf("backfill failed: %v", err)
	}

	// Verify operations were stored.
	var opCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM anchorage_operation").Scan(&opCount); err != nil {
		t.Fatalf("query operation count: %v", err)
	}

	if opCount == 0 {
		t.Error("expected at least 1 operation after backfill")
	}
	t.Logf("operations backfilled: %d", opCount)

	// Run backfill again — should be idempotent (0 new operations due to ON CONFLICT).
	err = run(ctx, []string{
		"-db", dbURL,
		"-api-url", apiServer.URL,
		"-api-key", "test-key",
		"-prime", "spark",
		"--backfill",
	})
	if err != nil {
		t.Fatalf("second backfill failed: %v", err)
	}

	var opCountAfter int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM anchorage_operation").Scan(&opCountAfter); err != nil {
		t.Fatalf("query operation count after second backfill: %v", err)
	}

	if opCountAfter != opCount {
		t.Errorf("expected same operation count after idempotent backfill, got %d (was %d)", opCountAfter, opCount)
	}
}
