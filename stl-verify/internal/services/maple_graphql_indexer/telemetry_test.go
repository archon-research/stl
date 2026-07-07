package maple_graphql_indexer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry()
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	if tel == nil {
		t.Fatal("telemetry is nil")
	}
}

func TestTelemetry_RecordsMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})

	tel, err := NewTelemetryWithProviders(tp, mp)
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}

	ctx := context.Background()
	cycleCtx, cycleSpan := tel.StartCycleSpan(ctx, time.Now())
	phaseCtx, phaseSpan := tel.StartPhaseSpan(cycleCtx, "pools")

	tel.RecordPhase(phaseCtx, "pools", 250*time.Millisecond, nil)
	tel.RecordPhase(phaseCtx, "loans", time.Second, errors.New("boom"))
	tel.RecordRowsWritten(phaseCtx, "maple_pool_state", 21)
	tel.RecordNullDowngrade(phaseCtx, "pool_tvl")
	tel.RecordNullDowngrade(phaseCtx, "pool_tvl")
	tel.RecordNullDowngrade(phaseCtx, "collateral_asset_amount")
	tel.RecordCycle(cycleCtx, nil)
	tel.RecordCycle(cycleCtx, errors.New("boom"))

	telemetry.SetSpanError(phaseSpan, errors.New("boom"), "phase failed")
	telemetry.SetSpanError(cycleSpan, nil, "ignored for nil error")
	phaseSpan.End()
	cycleSpan.End()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	// Index data points by instrument name + status/table/phase attributes.
	sums := map[string]int64{}
	histograms := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					key := m.Name
					if v, ok := dp.Attributes.Value("status"); ok {
						key += "|status=" + v.AsString()
					}
					if v, ok := dp.Attributes.Value("table"); ok {
						key += "|table=" + v.AsString()
					}
					if v, ok := dp.Attributes.Value("phase"); ok {
						key += "|phase=" + v.AsString()
					}
					if v, ok := dp.Attributes.Value("field"); ok {
						key += "|field=" + v.AsString()
					}
					sums[key] = dp.Value
				}
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					key := m.Name
					if v, ok := dp.Attributes.Value("phase"); ok {
						key += "|phase=" + v.AsString()
					}
					histograms[key] = dp.Count
				}
			}
		}
	}

	wantSums := map[string]int64{
		"maple.sync.cycles.total|status=success":                         1,
		"maple.sync.cycles.total|status=error":                           1,
		"maple.sync.phases.total|status=success|phase=pools":             1,
		"maple.sync.phases.total|status=error|phase=loans":               1,
		"maple.sync.rows.written|table=maple_pool_state":                 21,
		"maple.sync.null_downgrades.total|field=pool_tvl":                2,
		"maple.sync.null_downgrades.total|field=collateral_asset_amount": 1,
	}
	for key, want := range wantSums {
		if got := sums[key]; got != want {
			t.Errorf("sum %q = %d, want %d (all sums: %v)", key, got, want, sums)
		}
	}
	for _, phase := range []string{"pools", "loans"} {
		if got := histograms["maple.sync.phase.duration_seconds|phase="+phase]; got != 1 {
			t.Errorf("duration histogram for %s count = %d, want 1", phase, got)
		}
	}
}

// TestSync_NullDowngradesRecorded drives Sync end-to-end with API data
// containing every null-downgrade case and asserts the per-field counter.
func TestSync_NullDowngradesRecorded(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})

	tel, err := NewTelemetryWithProviders(tp, mp)
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}

	client := happyClient()
	client.GetPoolsFn = func(context.Context) ([]outbound.MaplePool, error) {
		pools := fixturePools()
		pools[0].TVL = nil
		pools[0].CollateralUSD = nil
		pools[1].TVL = nil
		return pools, nil
	}
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		loans := fixtureLoans()
		loans[0].Collateral.AssetAmount = nil
		loans[0].Collateral.AssetValueUSD = nil
		loans[2].Collateral.AssetAmount = nil
		return loans, nil
	}
	client.GetSkyStrategiesFn = func(context.Context) ([]outbound.MapleSkyStrategy, error) {
		strategies := fixtureStrategies()
		strategies[0].StrategyFeeRate = nil
		strategies[0].TotalFeesCollected = nil
		return strategies, nil
	}
	client.GetSyrupGlobalsFn = func(context.Context) (*outbound.MapleSyrupGlobals, error) {
		globals := fixtureGlobals()
		globals.DripsYieldBoost = nil
		return globals, nil
	}

	repo := newMockRepo()
	service, err := NewService(ServiceConfig{ChainID: 1}, client, repo, repo.tokenRepo, repo.userRepo, &testutil.MockTxManager{}, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	service.now = func() time.Time { return time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC) }

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "maple.sync.null_downgrades.total" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("null_downgrades data type = %T, want Sum[int64]", m.Data)
			}
			for _, dp := range data.DataPoints {
				field, _ := dp.Attributes.Value("field")
				got[field.AsString()] = dp.Value
			}
		}
	}
	want := map[string]int64{
		"pool_tvl":                      2,
		"pool_collateral_value_usd":     1,
		"pool_monthly_apy":              1, // fixture pool 2 has no APYs
		"pool_spot_apy":                 1,
		"collateral_asset_amount":       2,
		"collateral_asset_value_usd":    1,
		"loan_acm_ratio":                1, // fixture loan 2 is uncollateralized
		"collateral_liquidation_level":  1, // fixture loan 3 collateral has none
		"strategy_fee_rate":             1,
		"strategy_total_fees_collected": 1,
		"syrup_drips_yield_boost":       1,
	}
	for field, wantCount := range want {
		if got[field] != wantCount {
			t.Errorf("null_downgrades[%s] = %d, want %d (all: %v)", field, got[field], wantCount, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("unexpected null_downgrade fields: %v", got)
	}
}

// TestSync_CollateralPriceNullReason drives Sync with two loans whose
// collateral price is null — one DepositPending (reason=pending) and one
// Deposited (reason=unpriceable, an upstream oracle gap) — and asserts the
// null-downgrade counter splits by reason and token.
func TestSync_CollateralPriceNullReason(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})

	tel, err := NewTelemetryWithProviders(tp, mp)
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}

	client := happyClient()
	client.GetActiveLoansFn = func(context.Context) ([]outbound.MapleActiveLoan, error) {
		loans := fixtureLoans()
		loans[0].Collateral.AssetValueUSD = nil // BTC, state "Deposited" -> unpriceable
		loans[2].Collateral.State = "DepositPending"
		loans[2].Collateral.AssetValueUSD = nil // SOL, still pending -> pending
		return loans, nil
	}

	repo := newMockRepo()
	service, err := NewService(ServiceConfig{ChainID: 1}, client, repo, repo.tokenRepo, repo.userRepo, &testutil.MockTxManager{}, tel)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	service.now = func() time.Time { return time.Date(2026, 6, 10, 10, 0, 0, 0, time.UTC) }

	if err := service.Sync(context.Background()); err != nil {
		t.Fatalf("Sync: %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "maple.sync.null_downgrades.total" {
				continue
			}
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("null_downgrades data type = %T, want Sum[int64]", m.Data)
			}
			for _, dp := range data.DataPoints {
				field, _ := dp.Attributes.Value("field")
				if field.AsString() != "collateral_asset_value_usd" {
					continue
				}
				reason, _ := dp.Attributes.Value("reason")
				token, _ := dp.Attributes.Value("token")
				got[reason.AsString()+"|"+token.AsString()] = dp.Value
			}
		}
	}
	want := map[string]int64{
		"unpriceable|BTC": 1,
		"pending|SOL":     1,
	}
	for key, wantCount := range want {
		if got[key] != wantCount {
			t.Errorf("collateral price null[%s] = %d, want %d (all: %v)", key, got[key], wantCount, got)
		}
	}
	if len(got) != len(want) {
		t.Errorf("unexpected collateral-price-null series: %v", got)
	}
}

func TestTelemetry_NilReceiverSafe(t *testing.T) {
	var tel *Telemetry
	ctx := context.Background()

	cycleCtx, cycleSpan := tel.StartCycleSpan(ctx, time.Now())
	if cycleCtx == nil || cycleSpan == nil {
		t.Fatal("nil telemetry must return usable ctx/span")
	}
	phaseCtx, phaseSpan := tel.StartPhaseSpan(ctx, "pools")
	if phaseCtx == nil || phaseSpan == nil {
		t.Fatal("nil telemetry must return usable ctx/span")
	}

	// None of these may panic on a nil receiver.
	tel.RecordCycle(ctx, nil)
	tel.RecordPhase(ctx, "pools", time.Second, nil)
	tel.RecordRowsWritten(ctx, "maple_pool_state", 1)
	tel.RecordNullDowngrade(ctx, "pool_tvl")
	tel.RecordCollateralPriceNull(ctx, "unpriceable", "PYUSD")
	cycleSpan.End()
	phaseSpan.End()
}

// failingMeter errors on the instrument whose name matches failOn.
type failingMeter struct {
	noop.Meter
	failOn string
}

func (m failingMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == m.failOn {
		return nil, errors.New("instrument creation failed")
	}
	return m.Meter.Int64Counter(name, options...)
}

func (m failingMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == m.failOn {
		return nil, errors.New("instrument creation failed")
	}
	return m.Meter.Float64Histogram(name, options...)
}

type failingMeterProvider struct {
	noop.MeterProvider
	failOn string
}

func (p failingMeterProvider) Meter(name string, options ...metric.MeterOption) metric.Meter {
	return failingMeter{failOn: p.failOn}
}

func TestNewTelemetryWithProviders_InstrumentErrors(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	instruments := []string{
		"maple.sync.cycles.total",
		"maple.sync.phases.total",
		"maple.sync.rows.written",
		"maple.sync.null_downgrades.total",
		"maple.sync.phase.duration_seconds",
	}
	for _, name := range instruments {
		t.Run(name, func(t *testing.T) {
			_, err := NewTelemetryWithProviders(tp, failingMeterProvider{failOn: name})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "instrument creation failed") {
				t.Errorf("error = %q", err.Error())
			}
		})
	}
}

// Guards the startup seeds: VectorMapleIndexerStalled (cycles, rate==0) and
// VectorMaplePoolWritesZero (rows_written{table="maple_pool_state"}, increase==0)
// must be computable from process start. See telemetry.SeedCounter.
func TestNewTelemetry_SeedsAlertedSeriesAtZero(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	if _, err := NewTelemetryWithProviders(tracenoop.NewTracerProvider(), mp); err != nil {
		t.Fatalf("NewTelemetryWithProviders() error: %v", err)
	}

	cycleDPs := testutil.CollectSumDataPoints(t, reader, "maple.sync.cycles.total")
	cycleStatuses := map[string]int64{}
	for _, dp := range cycleDPs {
		if chain := testutil.AttrValue(dp, "chain"); chain != "ethereum" {
			t.Errorf("maple.sync.cycles.total chain attr = %q, want %q", chain, "ethereum")
		}
		cycleStatuses[testutil.AttrValue(dp, "status")] = dp.Value
	}
	for _, status := range []string{"success", "error"} {
		v, ok := cycleStatuses[status]
		if !ok {
			t.Errorf("maple.sync.cycles.total missing status=%q series before any cycle", status)
			continue
		}
		if v != 0 {
			t.Errorf("maple.sync.cycles.total{status=%q} = %d, want 0", status, v)
		}
	}

	var poolRows *metricdata.DataPoint[int64]
	for _, dp := range testutil.CollectSumDataPoints(t, reader, "maple.sync.rows.written") {
		if testutil.AttrValue(dp, "table") == maplePoolStateTable {
			dp := dp
			poolRows = &dp
		}
	}
	if poolRows == nil {
		t.Errorf("maple.sync.rows.written missing table=%q series before any cycle", maplePoolStateTable)
		return
	}
	if chain := testutil.AttrValue(*poolRows, "chain"); chain != "ethereum" {
		t.Errorf("maple.sync.rows.written{table=%q} chain attr = %q, want %q", maplePoolStateTable, chain, "ethereum")
	}
	if poolRows.Value != 0 {
		t.Errorf("maple.sync.rows.written{table=%q} = %d, want 0", maplePoolStateTable, poolRows.Value)
	}
}
