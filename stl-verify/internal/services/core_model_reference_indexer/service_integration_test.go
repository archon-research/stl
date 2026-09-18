//go:build integration

package core_model_reference_indexer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/coremodelfeed"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// The live /core/overview/ payload of 17 Sep 2026: 32 markets on ethereum,
// base and robinhood, 10 vaults of which one (groveUSDG) is method=override
// with no standard error and no expected shortfall.
const recordedOverview = "testdata/overview_2026-09-17.json"

// serveRecordedOverview replays the recorded payload on the route the client
// requests, so the whole path from HTTP decode to committed rows runs against
// exactly what upstream served.
func serveRecordedOverview(t *testing.T) string {
	t.Helper()
	payload, err := os.ReadFile(recordedOverview)
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/overview/" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	t.Cleanup(server.Close)
	return server.URL
}

// newWiredService builds the service as main.go does, against the given feed
// URL and database, with the cycle clock injected.
func newWiredService(t *testing.T, ctx context.Context, pool *pgxpool.Pool, feedURL string, now Clock) (*Service, buildregistry.RunID) {
	t.Helper()
	client, err := coremodelfeed.NewClient(coremodelfeed.ClientConfig{BaseURL: feedURL, MaxRetries: 1})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	txm, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		t.Fatalf("NewTxManager() = %v", err)
	}
	buildID, runID := testutil.OpenTestRun(t, ctx, pool)
	service, err := NewService(Deps{
		Provider:   client,
		MarketRepo: postgres.NewCoreModelReferenceMarketResultRepository(nil, runID),
		VaultRepo:  postgres.NewCoreModelReferenceVaultResultRepository(nil, runID),
		TxManager:  txm,
	}, int(buildID), now, nil, nil)
	if err != nil {
		t.Fatalf("NewService() = %v", err)
	}
	return service, runID
}

func countRows(t *testing.T, ctx context.Context, pool *pgxpool.Pool, table string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("counting %s: %v", table, err)
	}
	return n
}

func TestRunPersistsTheRecordedOverviewEndToEnd(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	syncedAt := time.Date(2026, 9, 17, 12, 30, 0, 0, time.UTC)
	service, runID := newWiredService(t, ctx, pool, serveRecordedOverview(t), func() time.Time { return syncedAt })

	if err := service.Run(ctx); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	if got := countRows(t, ctx, pool, "core_model_reference_market_result"); got != 32 {
		t.Errorf("market rows = %d, want the 32 markets of the recorded overview", got)
	}
	if got := countRows(t, ctx, pool, "core_model_reference_vault_result"); got != 10 {
		t.Errorf("vault rows = %d, want the 10 vaults of the recorded overview", got)
	}

	var crrEL, crrFloor, modelDate, source string
	var chainID, gotRunID *int64
	if err := pool.QueryRow(ctx, `
		SELECT crr_el::text, crr_floor::text, model_date::text, source, chain_id, run_id
		FROM core_model_reference_market_result
		WHERE protocol_name = 'sparklend' AND market_symbol = 'spUSDS' AND synced_at = $1`, syncedAt,
	).Scan(&crrEL, &crrFloor, &modelDate, &source, &chainID, &gotRunID); err != nil {
		t.Fatalf("reading spUSDS: %v", err)
	}
	testutil.RequireRunID(t, gotRunID, runID)
	if crrFloor != "0.020000000000000000" || crrEL == "" || crrEL >= crrFloor {
		t.Errorf("spUSDS crr_el/crr_floor = %s/%s, want the raw EL under the 0.02 floor and the floor stored apart", crrEL, crrFloor)
	}
	if modelDate != "2026-09-17" || chainID == nil || *chainID != 1 || source != "coremodel:dashboard" {
		t.Errorf("spUSDS model_date/chain_id/source = %s/%v/%s", modelDate, chainID, source)
	}
}

func TestRunKeepsTheOverrideVaultsSimulationFiguresNullEndToEnd(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	service, _ := newWiredService(t, ctx, pool, serveRecordedOverview(t), nil)

	if err := service.Run(ctx); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	var method string
	var se, es *string
	var chainID *int64
	if err := pool.QueryRow(ctx, `
		SELECT method, crr_el_se::text, crr_es::text, chain_id
		FROM core_model_reference_vault_result WHERE vault_symbol = 'groveUSDG'`).Scan(&method, &se, &es, &chainID); err != nil {
		t.Fatalf("reading groveUSDG: %v", err)
	}
	if method != "override" || se != nil || es != nil {
		t.Errorf("groveUSDG = method %q, se %v, es %v; want override with both NULL", method, se, es)
	}
	if chainID == nil || *chainID != 4663 {
		t.Errorf("groveUSDG chain_id = %v, want 4663 (robinhood)", chainID)
	}
}

func TestRunAppendsANewCyclePerTickAndNeverRewritesOne(t *testing.T) {
	ctx := context.Background()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ticks := []time.Time{
		time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 17, 12, 30, 0, 0, time.UTC),
	}
	var tick int
	service, _ := newWiredService(t, ctx, pool, serveRecordedOverview(t), func() time.Time { return ticks[tick] })

	for tick = range ticks {
		if err := service.Run(ctx); err != nil {
			t.Fatalf("Run() tick %d = %v", tick, err)
		}
	}

	var cycles int
	if err := pool.QueryRow(ctx, `SELECT count(DISTINCT synced_at) FROM core_model_reference_market_result`).Scan(&cycles); err != nil {
		t.Fatalf("counting cycles: %v", err)
	}
	if cycles != 2 {
		t.Errorf("distinct synced_at = %d, want 2: the repeated tick conflicted away, the later tick appended", cycles)
	}
	if got := countRows(t, ctx, pool, "core_model_reference_market_result"); got != 64 {
		t.Errorf("market rows = %d, want 64 (32 per distinct cycle)", got)
	}
}
