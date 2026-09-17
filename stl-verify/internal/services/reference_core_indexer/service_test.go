package reference_core_indexer

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var (
	errProvider = errors.New("provider boom")
	errRepo     = errors.New("repo boom")
	syncedAt    = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
)

type mockProvider struct {
	overview outbound.ReferenceCoreOverview
	err      error
}

func (m *mockProvider) FetchOverview(_ context.Context) (outbound.ReferenceCoreOverview, error) {
	return m.overview, m.err
}

type mockMarketRepo struct {
	saved []entity.ReferenceCoreMarketResult
	err   error
}

func (m *mockMarketRepo) SaveMarketResults(_ context.Context, _ pgx.Tx, results []entity.ReferenceCoreMarketResult) error {
	if m.err != nil {
		return m.err
	}
	m.saved = append(m.saved, results...)
	return nil
}

type mockVaultRepo struct {
	saved []entity.ReferenceCoreVaultResult
	err   error
}

func (m *mockVaultRepo) SaveVaultResults(_ context.Context, _ pgx.Tx, results []entity.ReferenceCoreVaultResult) error {
	if m.err != nil {
		return m.err
	}
	m.saved = append(m.saved, results...)
	return nil
}

// fakeTxManager calls fn with a nil pgx.Tx; sufficient since the mock repos
// ignore the tx argument. rolledBack records whether fn failed, so a test can
// assert nothing of a failed cycle counts as committed.
type fakeTxManager struct{ rolledBack bool }

func (m *fakeTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	err := fn(nil)
	m.rolledBack = err != nil
	return err
}

func marketRow(uid, date string) outbound.ReferenceCoreMarketRow {
	chainID := int64(1)
	return outbound.ReferenceCoreMarketRow{
		Network:              "ethereum",
		ChainID:              &chainID,
		Protocol:             "sparklend",
		MarketUID:            uid,
		MarketSymbol:         "spUSDS",
		LoanTokenSymbol:      "USDS",
		LoanTokenAddress:     "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
		Date:                 date,
		NScenarios:           10000,
		HorizonDays:          15,
		EffectiveHorizonDays: 1,
		TotalSupply:          "1309071023.791548000000000000",
		ProbNoBadDebt:        "0.694299999999999900",
		CRREL:                "0.001214738329317449",
		CRRVaR:               "0.010683027397348378",
		CRRES:                "0.030818438149105926",
		CRRELSE:              "0.000077018244182130",
		CRRVaRSE:             "0.000694722112615200",
		CRRESSE:              "0.001220932121971280",
		CRRFloor:             "0.020000000000000000",
		ExternalFlowEnabled:  true,
	}
}

func vaultRow(address, date string) outbound.ReferenceCoreVaultRow {
	chainID := int64(8453)
	return outbound.ReferenceCoreVaultRow{
		Network:          "base",
		ChainID:          &chainID,
		Protocol:         "morpho",
		VaultAddress:     address,
		VaultSymbol:      "steakUSDC",
		VaultName:        "Steakhouse Prime USDC",
		Version:          "v2",
		LoanTokenSymbol:  "USDC",
		LoanTokenAddress: "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
		Method:           "model",
		Date:             date,
		NMarkets:         5,
		TotalAssets:      "434521303.920751150000000000",
		IdleAssets:       "3201.013246119022400000",
		CRREL:            "0.000288698101040592",
		CRRELSE:          new("0.000028834308921715"),
		CRRES:            new("0.009880342305281255"),
	}
}

func overview(markets []outbound.ReferenceCoreMarketRow, vaults []outbound.ReferenceCoreVaultRow) outbound.ReferenceCoreOverview {
	return outbound.ReferenceCoreOverview{Markets: markets, Vaults: vaults}
}

func healthyOverview() outbound.ReferenceCoreOverview {
	return overview(
		[]outbound.ReferenceCoreMarketRow{marketRow("0xaaa", "2026-09-17"), marketRow("0xbbb", "2026-09-17")},
		[]outbound.ReferenceCoreVaultRow{vaultRow("0xbeef", "2026-09-17")},
	)
}

type harness struct {
	provider *mockProvider
	markets  *mockMarketRepo
	vaults   *mockVaultRepo
	txm      *fakeTxManager
}

func newHarness(ov outbound.ReferenceCoreOverview) *harness {
	return &harness{
		provider: &mockProvider{overview: ov},
		markets:  &mockMarketRepo{},
		vaults:   &mockVaultRepo{},
		txm:      &fakeTxManager{},
	}
}

func (h *harness) deps() Deps {
	return Deps{Provider: h.provider, MarketRepo: h.markets, VaultRepo: h.vaults, TxManager: h.txm}
}

func (h *harness) run(t *testing.T, tel *Telemetry) error {
	t.Helper()
	service, err := NewService(h.deps(), 7, func() time.Time { return syncedAt }, tel, nil)
	if err != nil {
		t.Fatalf("NewService() = %v", err)
	}
	return service.Run(context.Background())
}

func TestRunPersistsEveryMarketAndVaultOfTheOverview(t *testing.T) {
	h := newHarness(healthyOverview())

	if err := h.run(t, nil); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if len(h.markets.saved) != 2 || len(h.vaults.saved) != 1 {
		t.Fatalf("saved %d markets and %d vaults, want 2 and 1", len(h.markets.saved), len(h.vaults.saved))
	}
}

func TestRunStampsEveryRowOfACycleIdentically(t *testing.T) {
	h := newHarness(healthyOverview())

	if err := h.run(t, nil); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	for _, m := range h.markets.saved {
		if !m.SyncedAt.Equal(syncedAt) || m.BuildID != 7 || m.Source != entity.ReferenceCoreDataSource {
			t.Errorf("market %s stamped %v/%d/%s, want %v/7/%s", m.MarketUID, m.SyncedAt, m.BuildID, m.Source, syncedAt, entity.ReferenceCoreDataSource)
		}
	}
	for _, v := range h.vaults.saved {
		if !v.SyncedAt.Equal(syncedAt) || v.BuildID != 7 || v.Source != entity.ReferenceCoreDataSource {
			t.Errorf("vault %s stamped %v/%d/%s, want %v/7/%s", v.VaultAddress, v.SyncedAt, v.BuildID, v.Source, syncedAt, entity.ReferenceCoreDataSource)
		}
	}
}

func TestRunCarriesEveryMarketFigureOntoTheResult(t *testing.T) {
	row := marketRow("0xaaa", "2026-09-16")
	h := newHarness(overview([]outbound.ReferenceCoreMarketRow{row}, []outbound.ReferenceCoreVaultRow{vaultRow("0xbeef", "2026-09-17")}))

	if err := h.run(t, nil); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	got := h.markets.saved[0]
	want := entity.ReferenceCoreMarketResult{
		Network: "ethereum", ChainID: row.ChainID, ProtocolName: "sparklend", MarketUID: "0xaaa", MarketSymbol: "spUSDS",
		LoanTokenSymbol: "USDS", LoanTokenAddress: row.LoanTokenAddress,
		ModelDate: time.Date(2026, 9, 16, 0, 0, 0, 0, time.UTC), SyncedAt: syncedAt,
		NScenarios: 10000, HorizonDays: 15, EffectiveHorizonDays: 1,
		TotalSupplyUSD: row.TotalSupply, ProbNoBadDebt: row.ProbNoBadDebt,
		CRREL: row.CRREL, CRRVaR: row.CRRVaR, CRRES: row.CRRES,
		CRRELSE: row.CRRELSE, CRRVaRSE: row.CRRVaRSE, CRRESSE: row.CRRESSE, CRRFloor: row.CRRFloor,
		ExternalFlowEnabled: true, Source: entity.ReferenceCoreDataSource, BuildID: 7,
	}
	if got != want {
		t.Errorf("market result =\n%+v\nwant\n%+v", got, want)
	}
}

func TestRunCarriesEveryVaultFigureOntoTheResult(t *testing.T) {
	row := vaultRow("0xbeef", "2026-09-17")
	h := newHarness(overview([]outbound.ReferenceCoreMarketRow{marketRow("0xaaa", "2026-09-17")}, []outbound.ReferenceCoreVaultRow{row}))

	if err := h.run(t, nil); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	got := h.vaults.saved[0]
	want := entity.ReferenceCoreVaultResult{
		Network: "base", ChainID: row.ChainID, ProtocolName: "morpho", VaultAddress: "0xbeef", VaultSymbol: "steakUSDC",
		VaultName: "Steakhouse Prime USDC", VersionLabel: "v2", LoanTokenSymbol: "USDC", LoanTokenAddress: row.LoanTokenAddress,
		Method: "model", ModelDate: time.Date(2026, 9, 17, 0, 0, 0, 0, time.UTC), SyncedAt: syncedAt,
		NMarkets: 5, TotalAssetsUSD: row.TotalAssets, IdleAssetsUSD: row.IdleAssets,
		CRREL: row.CRREL, CRRELSE: row.CRRELSE, CRRES: row.CRRES,
		Source: entity.ReferenceCoreDataSource, BuildID: 7,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("vault result =\n%+v\nwant\n%+v", got, want)
	}
}

func TestRunKeepsAnOverrideVaultsAbsentFiguresNilRatherThanZero(t *testing.T) {
	row := vaultRow("0xbeef", "2026-09-17")
	row.Method = "override"
	row.CRRELSE, row.CRRES = nil, nil
	h := newHarness(overview([]outbound.ReferenceCoreMarketRow{marketRow("0xaaa", "2026-09-17")}, []outbound.ReferenceCoreVaultRow{row}))

	if err := h.run(t, nil); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	got := h.vaults.saved[0]
	if got.Method != "override" || got.CRRELSE != nil || got.CRRES != nil {
		t.Errorf("override vault = method %q, se %v, es %v; want override with both nil", got.Method, got.CRRELSE, got.CRRES)
	}
}

func TestRunFailsWhenTheOverviewHasNoMarkets(t *testing.T) {
	h := newHarness(overview(nil, []outbound.ReferenceCoreVaultRow{vaultRow("0xbeef", "2026-09-17")}))

	err := h.run(t, nil)
	if err == nil || !strings.Contains(err.Error(), "no markets") {
		t.Fatalf("Run() = %v, want a no-markets error", err)
	}
	if len(h.vaults.saved) != 0 {
		t.Errorf("saved %d vaults from a failed cycle, want 0", len(h.vaults.saved))
	}
}

func TestRunFailsWhenTheOverviewHasNoVaults(t *testing.T) {
	h := newHarness(overview([]outbound.ReferenceCoreMarketRow{marketRow("0xaaa", "2026-09-17")}, nil))

	err := h.run(t, nil)
	if err == nil || !strings.Contains(err.Error(), "no vaults") {
		t.Fatalf("Run() = %v, want a no-vaults error", err)
	}
	if len(h.markets.saved) != 0 {
		t.Errorf("saved %d markets from a failed cycle, want 0", len(h.markets.saved))
	}
}

func TestRunFailsOnAnUnparseableMarketDateBeforePersistingAnything(t *testing.T) {
	h := newHarness(overview(
		[]outbound.ReferenceCoreMarketRow{marketRow("0xaaa", "17/09/2026")},
		[]outbound.ReferenceCoreVaultRow{vaultRow("0xbeef", "2026-09-17")}))

	err := h.run(t, nil)
	if err == nil || !strings.Contains(err.Error(), "17/09/2026") {
		t.Fatalf("Run() = %v, want a date parse error naming the value", err)
	}
	if len(h.markets.saved)+len(h.vaults.saved) != 0 {
		t.Error("a cycle that failed conversion must persist nothing")
	}
}

func TestRunFailsOnAnUnparseableVaultDate(t *testing.T) {
	h := newHarness(overview(
		[]outbound.ReferenceCoreMarketRow{marketRow("0xaaa", "2026-09-17")},
		[]outbound.ReferenceCoreVaultRow{vaultRow("0xbeef", "")}))

	if err := h.run(t, nil); err == nil {
		t.Fatal("Run() = nil, want a date parse error")
	}
}

func TestRunPropagatesAProviderFailure(t *testing.T) {
	h := newHarness(healthyOverview())
	h.provider.err = errProvider

	if err := h.run(t, nil); !errors.Is(err, errProvider) {
		t.Fatalf("Run() = %v, want %v", err, errProvider)
	}
}

func TestRunRollsBackTheCycleWhenMarketsFailToPersist(t *testing.T) {
	h := newHarness(healthyOverview())
	h.markets.err = errRepo

	if err := h.run(t, nil); !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want %v", err, errRepo)
	}
	if !h.txm.rolledBack {
		t.Error("the transaction must roll back when the market save fails")
	}
	if len(h.vaults.saved) != 0 {
		t.Errorf("saved %d vaults after the market save failed, want 0", len(h.vaults.saved))
	}
}

func TestRunRollsBackTheCycleWhenVaultsFailToPersist(t *testing.T) {
	h := newHarness(healthyOverview())
	h.vaults.err = errRepo

	if err := h.run(t, nil); !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want %v", err, errRepo)
	}
	if !h.txm.rolledBack {
		t.Error("the transaction must roll back when the vault save fails")
	}
}

func TestNewServiceRejectsAMissingPort(t *testing.T) {
	h := newHarness(healthyOverview())
	deps := h.deps()
	deps.VaultRepo = nil

	_, err := NewService(deps, 7, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "VaultRepo") {
		t.Fatalf("NewService() = %v, want an error naming VaultRepo", err)
	}
}

func TestNewServiceDefaultsTheClockAndLogger(t *testing.T) {
	h := newHarness(healthyOverview())

	service, err := NewService(h.deps(), 7, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewService() = %v", err)
	}
	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	if h.markets.saved[0].SyncedAt.IsZero() {
		t.Error("a nil Clock must default to time.Now, not the zero time")
	}
}

func TestRunRecordsWrittenAndStaleCountsThroughACycle(t *testing.T) {
	reader := metric.NewManualReader()
	tel, err := NewTelemetryWithProvider(context.Background(), metric.NewMeterProvider(metric.WithReader(reader)))
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider() = %v", err)
	}
	// syncedAt is 17 Sep: yesterday's date is within the allowance, the 14th is not.
	h := newHarness(overview(
		[]outbound.ReferenceCoreMarketRow{marketRow("0xaaa", "2026-09-17"), marketRow("0xbbb", "2026-09-16"), marketRow("0xccc", "2026-09-14")},
		[]outbound.ReferenceCoreVaultRow{vaultRow("0xbeef", "2026-09-01")}))

	if err := h.run(t, tel); err != nil {
		t.Fatalf("Run() = %v", err)
	}

	got := counterValues(t, reader)
	if got["reference_core.sync.markets.written.total"] != 3 {
		t.Errorf("markets.written.total = %d, want 3", got["reference_core.sync.markets.written.total"])
	}
	if got["reference_core.sync.vaults.written.total"] != 1 {
		t.Errorf("vaults.written.total = %d, want 1", got["reference_core.sync.vaults.written.total"])
	}
	if got["reference_core.sync.stale_rows.total"] != 2 {
		t.Errorf("stale_rows.total = %d, want 2 — the 14 Sep market and the 1 Sep vault", got["reference_core.sync.stale_rows.total"])
	}
	if got["reference_core.sync.stale_cycles.total"] != 0 {
		t.Errorf("stale_cycles.total = %d, want 0 — one fresh row means upstream is still publishing", got["reference_core.sync.stale_cycles.total"])
	}
}

func TestRunCountsAStaleCycleOnlyWhenNoRowIsFresh(t *testing.T) {
	// syncedAt is 17 Sep; 16 Sep is within the one-day allowance, 15 Sep is not.
	for _, tc := range []struct {
		name        string
		marketDates []string
		vaultDate   string
		wantCycles  int64
	}{
		{"all rows behind the allowance", []string{"2026-09-15", "2026-09-10"}, "2026-09-15", 1},
		{"one market row fresh", []string{"2026-09-16", "2026-09-10"}, "2026-09-15", 0},
		{"only the vault row fresh", []string{"2026-09-15", "2026-09-10"}, "2026-09-17", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := metric.NewManualReader()
			tel, err := NewTelemetryWithProvider(context.Background(), metric.NewMeterProvider(metric.WithReader(reader)))
			if err != nil {
				t.Fatalf("NewTelemetryWithProvider() = %v", err)
			}
			markets := make([]outbound.ReferenceCoreMarketRow, 0, len(tc.marketDates))
			for i, d := range tc.marketDates {
				markets = append(markets, marketRow("0x"+strings.Repeat("a", i+1), d))
			}
			h := newHarness(overview(markets, []outbound.ReferenceCoreVaultRow{vaultRow("0xbeef", tc.vaultDate)}))

			if err := h.run(t, tel); err != nil {
				t.Fatalf("Run() = %v; staleness must never fail the cycle", err)
			}
			if got := counterValues(t, reader)["reference_core.sync.stale_cycles.total"]; got != tc.wantCycles {
				t.Errorf("stale_cycles.total = %d, want %d", got, tc.wantCycles)
			}
		})
	}
}

func TestRunRecordsNothingWrittenWhenTheCycleFails(t *testing.T) {
	reader := metric.NewManualReader()
	tel, err := NewTelemetryWithProvider(context.Background(), metric.NewMeterProvider(metric.WithReader(reader)))
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider() = %v", err)
	}
	h := newHarness(healthyOverview())
	h.vaults.err = errRepo

	if err := h.run(t, tel); !errors.Is(err, errRepo) {
		t.Fatalf("Run() = %v, want %v", err, errRepo)
	}
	got := counterValues(t, reader)
	if got["reference_core.sync.markets.written.total"] != 0 || got["reference_core.sync.vaults.written.total"] != 0 {
		t.Errorf("written counters = %v, want both 0 after a rolled-back cycle", got)
	}
}

// counterValues collects every int64 counter's single data point by name.
func counterValues(t *testing.T, reader *metric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() = %v", err)
	}
	values := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || len(sum.DataPoints) != 1 {
				t.Fatalf("%s = %#v, want exactly one int64 data point", m.Name, m.Data)
			}
			values[m.Name] = sum.DataPoints[0].Value
		}
	}
	return values
}
