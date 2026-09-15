package dextelemetry

import (
	"context"
	"testing"

	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestNFTTransferRecorder(t *testing.T, prefix string, chainID int64) (*NFTTransferRecorder, *metricsdk.ManualReader) {
	t.Helper()
	reader := installTestMeterProvider(t)

	recorder, err := NewNFTTransferRecorder(prefix, chainID)
	if err != nil {
		t.Fatalf("NewNFTTransferRecorder: %v", err)
	}
	return recorder, reader
}

func collectSumTotals(t *testing.T, reader *metricsdk.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	totals := make(map[string]int64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				totals[m.Name] += dp.Value
			}
		}
	}
	return totals
}

// A replay's rows have to land on the live indexer's series, so both names and
// totals are read off what each side emits rather than off a literal.
func TestNewNFTTransferRecorder_EmitsTheSeriesNewTelemetryDoes(t *testing.T) {
	ctx := context.Background()

	recorder, recorderReader := newTestNFTTransferRecorder(t, "uniswap_v4", 1)
	recorder.RecordNFTTransferRows(ctx, 3, 2)
	recorded := collectSumTotals(t, recorderReader)

	tel, telReader := newTestTelemetry(t, "uniswap_v4", 1)
	tel.RecordNFTTransferRows(ctx, 3, 2)
	reference := collectSumTotals(t, telReader)

	if len(recorded) != 2 {
		t.Fatalf("recorder emitted %v, want exactly the two posm transfer counters", recorded)
	}
	for name, got := range recorded {
		want, ok := reference[name]
		if !ok {
			t.Errorf("recorder emits %q, which NewTelemetry does not (%v)", name, reference)
			continue
		}
		if got != want {
			t.Errorf("%s = %d, want %d: the same rows must move the same series", name, got, want)
		}
	}
}

// No alert reads these counters at ==0 for a hand-started replay, so seeding them
// would leave a permanent zero series on a worker that processes no blocks.
func TestNewNFTTransferRecorder_SeedsNeitherCounter(t *testing.T) {
	_, reader := newTestNFTTransferRecorder(t, "uniswap_v4", 1)

	if seeded := collectSumTotals(t, reader); len(seeded) != 0 {
		t.Errorf("recorder exported %v before any row was recorded, want no series", seeded)
	}
}

func TestNFTTransferRecorder_NonPositiveCountRecordsNothing(t *testing.T) {
	recorder, reader := newTestNFTTransferRecorder(t, "uniswap_v4", 1)
	ctx := context.Background()

	recorder.RecordNFTTransferRows(ctx, 0, 0)
	recorder.RecordNFTTransferRows(ctx, -1, -1)

	if exported := collectSumTotals(t, reader); len(exported) != 0 {
		t.Errorf("recorder exported %v, want no series", exported)
	}
}

func TestNFTTransferRecorder_NilSafe(t *testing.T) {
	var recorder *NFTTransferRecorder
	recorder.RecordNFTTransferRows(context.Background(), 5, 5)
}
