package reference_capital_indexer

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// The alert rules key on these exact names; renaming one silently stops
// VectorReferenceCapitalIndexerWritesZero / PrimeUncovered / AllocationsZero /
// PositionsZero / BalanceSheetStalled / BalanceSheetPrimeUncovered from ever
// firing.
func TestTelemetryEmitsTheMetricNamesTheAlertsQuery(t *testing.T) {
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))

	tel, err := NewTelemetryWithProvider(provider)
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider() = %v", err)
	}

	ctx := context.Background()
	tel.RecordSnapshotsWritten(ctx, 2)
	tel.RecordAllocationsWritten(ctx, 11)
	tel.RecordPositionsWritten(ctx, 59)
	tel.RecordPrimeUncovered(ctx, "grove")
	tel.RecordBalanceSheetDaysInserted(ctx, 3)
	tel.RecordBalanceSheetPrimeUncovered(ctx, "grove")

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect() = %v", err)
	}

	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names[m.Name] = true
		}
	}

	for _, want := range []string{
		"reference_capital.sync.snapshots.written.total",
		"reference_capital.sync.allocations.written.total",
		"reference_capital.sync.positions.written.total",
		"reference_capital.sync.primes.uncovered.total",
		"reference_capital.sync.balance_sheet.days.inserted.total",
		"reference_capital.sync.balance_sheet.primes.uncovered.total",
	} {
		if !names[want] {
			t.Errorf("metric %q not emitted; got %v", want, names)
		}
	}
}

// The service constructs Telemetry optionally, so every recorder must tolerate
// a nil receiver rather than panicking a cycle that would otherwise succeed.
func TestTelemetryRecordersAreNilSafe(t *testing.T) {
	var tel *Telemetry
	tel.RecordSnapshotsWritten(context.Background(), 1)
	tel.RecordAllocationsWritten(context.Background(), 1)
	tel.RecordPositionsWritten(context.Background(), 1)
	tel.RecordPrimeUncovered(context.Background(), "spark")
	tel.RecordBalanceSheetDaysInserted(context.Background(), 1)
	tel.RecordBalanceSheetPrimeUncovered(context.Background(), "spark")
}
