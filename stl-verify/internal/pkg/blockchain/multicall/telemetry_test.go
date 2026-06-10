package multicall

import (
	"context"
	"slices"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// histDataPoint returns the single histogram data point for the named metric.
func histDataPoint(t *testing.T, reader sdkmetric.Reader, name string) metricdata.HistogramDataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Histogram[int64]", name, m.Data)
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("metric %q has %d data points, want 1", name, len(hist.DataPoints))
			}
			return hist.DataPoints[0]
		}
	}
	t.Fatalf("metric %q not found", name)
	panic("unreachable")
}

func TestRecordBatch_CountSumAndChain(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProvider(mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProvider: %v", err)
	}
	tel.RecordBatch(context.Background(), 3)
	tel.RecordBatch(context.Background(), 7)

	dp := histDataPoint(t, reader, "multicall.batch.size")
	if dp.Count != 2 {
		t.Errorf("count = %d, want 2 (two Execute calls)", dp.Count)
	}
	if dp.Sum != 10 {
		t.Errorf("sum = %d, want 10 (3+7 individual calls)", dp.Sum)
	}
	val, ok := dp.Attributes.Value("chain")
	if !ok || val.AsString() != "mainnet" {
		t.Errorf("chain attribute = %v (ok=%v), want mainnet", val.AsString(), ok)
	}
	if !slices.Equal(dp.Bounds, batchSizeBuckets) {
		t.Errorf("bounds = %v, want %v", dp.Bounds, batchSizeBuckets)
	}
}
