//go:build integration

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/temporal"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	code := m.Run()

	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}

// mapleFixtureServer serves one pool, one loan, one strategy, and globals in
// live-API shapes, so the wired runner can complete a full sync cycle.
func mapleFixtureServer(t *testing.T) *httptest.Server {
	t.Helper()
	const (
		pool     = "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"
		usdc     = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
		loan     = "0x0009bff1fcb8c767e5894164124d3e42aaca0542"
		borrower = "0xfba4bc924ba50c3b3dd0c1aa6d2f499b4fa55c81"
		strategy = "0x859c9980931fa0a63765fd8ef2e29918af5b038c"
	)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
		}
		query := string(body)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.Contains(query, "poolV2S"):
			_, _ = w.Write([]byte(`{"data": {"poolV2S": [{"id": "` + pool + `", "name": "Syrup USDC", "monthlyApy": "0", "spotApy": "0", "assets": "400", "collateralValue": "500", "principalOut": "600", "tvl": "1000", "asset": {"id": "` + usdc + `", "symbol": "USDC", "decimals": 6}, "syrupRouter": null}]}}`))
		case strings.Contains(query, "openTermLoans"):
			_, _ = w.Write([]byte(`{"data": {"openTermLoans": [{"id": "` + loan + `", "borrower": {"id": "` + borrower + `"}, "state": "Active", "principalOwed": "100", "acmRatio": "1000000", "collateral": null, "loanMeta": null, "fundingPool": {"id": "` + pool + `"}}]}}`))
		case strings.Contains(query, "GetFixedTermLoans"):
			// The FTL book is dormant today; the wiring test only needs the phase
			// to run cleanly. Detailed FTL behavior is covered in the service
			// integration test.
			_, _ = w.Write([]byte(`{"data": {"loans": []}}`))
		case strings.Contains(query, "skyStrategies"):
			_, _ = w.Write([]byte(`{"data": {"skyStrategies": [{"id": "` + strategy + `", "state": "Active", "currentlyDeployed": "0", "depositedAssets": "1", "withdrawnAssets": "0", "strategyFeeRate": null, "totalFeesCollected": null, "version": 100, "pool": {"id": "` + pool + `", "name": "Syrup USDC"}}]}}`))
		case strings.Contains(query, "syrupGlobals"):
			_, _ = w.Write([]byte(`{"data": {"syrupGlobals": {"apy": "1", "collateralApy": "2", "poolApy": "3", "dripsYieldBoost": "0", "tvl": "4"}}}`))
		default:
			t.Errorf("unexpected query: %s", query)
		}
	}))
}

func TestSetupRunner_WiresService(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	server := mapleFixtureServer(t)
	defer server.Close()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("BUILD_GIT_HASH", "test-hash")
	t.Setenv("MAPLE_GRAPHQL_ENDPOINT", server.URL)

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	// Invoke the runner end to end: this proves the endpoint env var,
	// buildID, repository, and tx manager are all actually wired.
	if err := runner.Run(ctx); err != nil {
		t.Fatalf("runner.Run: %v", err)
	}

	for _, table := range []string{"maple_pool", "maple_pool_state", "maple_loan", "maple_loan_state",
		"maple_sky_strategy", "maple_sky_strategy_state", "maple_syrup_global_state"} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("%s rows = %d, want 1", table, count)
		}
	}
}

// TestSetupRunner_UsesScheduledAtFromContext pins the production path: under
// Temporal the activity stamps the schedule time into the context, and the
// runner must use it as synced_at so retries of the same run dedupe instead
// of multiplying snapshots.
func TestSetupRunner_UsesScheduledAtFromContext(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	server := mapleFixtureServer(t)
	defer server.Close()

	t.Setenv("CHAIN_ID", "1")
	t.Setenv("BUILD_GIT_HASH", "test-hash")
	t.Setenv("MAPLE_GRAPHQL_ENDPOINT", server.URL)

	runner, err := setupRunner(ctx, temporal.Dependencies{Pool: pool})
	if err != nil {
		t.Fatalf("setupRunner: %v", err)
	}

	scheduledAt := time.Date(2026, 6, 11, 9, 0, 0, 0, time.UTC)
	stampedCtx := temporal.ContextWithScheduledAt(ctx, scheduledAt)

	// Two runs with the same schedule time simulate a Temporal activity
	// retry: the second must dedupe via the processing-version trigger.
	for range 2 {
		if err := runner.Run(stampedCtx); err != nil {
			t.Fatalf("runner.Run: %v", err)
		}
	}

	var gotSyncedAt time.Time
	var count int
	if err := pool.QueryRow(ctx,
		`SELECT MIN(synced_at), COUNT(*) FROM maple_pool_state`).Scan(&gotSyncedAt, &count); err != nil {
		t.Fatalf("querying pool state: %v", err)
	}
	if !gotSyncedAt.Equal(scheduledAt) {
		t.Errorf("synced_at = %v, want the context schedule time %v", gotSyncedAt, scheduledAt)
	}
	if count != 1 {
		t.Errorf("pool state rows after retry = %d, want 1 (same scheduledAt must dedupe)", count)
	}

	for _, table := range []string{"maple_loan_state", "maple_sky_strategy_state", "maple_syrup_global_state"} {
		if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("counting %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("%s rows after retry = %d, want 1", table, count)
		}
	}
}

func TestSetupRunner_RejectsNonMainnetChain(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	defer cleanup()

	t.Setenv("CHAIN_ID", "137")
	t.Setenv("BUILD_GIT_HASH", "test-hash")

	_, err := setupRunner(ctx, temporal.Dependencies{Pool: pool})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "chainID must be 1") {
		t.Errorf("error = %q", err.Error())
	}
}
