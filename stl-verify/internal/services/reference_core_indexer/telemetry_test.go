package reference_core_indexer

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The alert rules key on these exact names; renaming one silently stops
// VectorReferenceCoreIndexerWritesZero / StaleUpstream / ResultGrowthHigh from
// ever firing.
func TestTelemetryEmitsTheMetricNamesTheAlertsQuery(t *testing.T) {
	reader := metric.NewManualReader()
	tel, err := NewTelemetryWithProvider(context.Background(), metric.NewMeterProvider(metric.WithReader(reader)))
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider() = %v", err)
	}

	ctx := context.Background()
	tel.RecordMarketsWritten(ctx, 32)
	tel.RecordVaultsWritten(ctx, 10)
	tel.RecordStaleRows(ctx, 1)
	tel.RecordStaleCycle(ctx)

	for name, want := range map[string]int64{
		"reference_core.sync.markets.written.total": 32,
		"reference_core.sync.vaults.written.total":  10,
		"reference_core.sync.stale_rows.total":      1,
		"reference_core.sync.stale_cycles.total":    1,
	} {
		if got := counterValues(t, reader)[name]; got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

// An `unless … > 0` rule needs the series to exist from process start, or a
// worker that never writes a row emits nothing and the rule stays silent.
func TestTelemetrySeedsEveryCounterAtZeroOnConstruction(t *testing.T) {
	reader := metric.NewManualReader()
	if _, err := NewTelemetryWithProvider(context.Background(), metric.NewMeterProvider(metric.WithReader(reader))); err != nil {
		t.Fatalf("NewTelemetryWithProvider() = %v", err)
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect() = %v", err)
	}
	seeded := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			seeded[m.Name] = true
		}
	}
	for _, want := range []string{
		"reference_core.sync.markets.written.total",
		"reference_core.sync.vaults.written.total",
		"reference_core.sync.stale_rows.total",
		"reference_core.sync.stale_cycles.total",
	} {
		if !seeded[want] {
			t.Errorf("%s not emitted before any record call; got %v", want, seeded)
		}
	}
}

// The service constructs Telemetry optionally, so every recorder must tolerate
// a nil receiver rather than panicking a cycle that would otherwise succeed.
func TestTelemetryRecordersAreNilSafe(t *testing.T) {
	var tel *Telemetry
	tel.RecordMarketsWritten(context.Background(), 1)
	tel.RecordVaultsWritten(context.Background(), 1)
	tel.RecordStaleRows(context.Background(), 1)
	tel.RecordStaleCycle(context.Background())
}
