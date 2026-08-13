package telemetry

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectHistogramBounds collects the named float64 histogram from reader and
// returns its bucket upper bounds.
func collectHistogramBounds(t *testing.T, reader sdkmetric.Reader, name string) []float64 {
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
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Histogram[float64]", name, m.Data)
			}
			if len(hist.DataPoints) != 1 {
				t.Fatalf("metric %q has %d data points, want 1", name, len(hist.DataPoints))
			}
			return hist.DataPoints[0].Bounds
		}
	}
	t.Fatalf("metric %q not found", name)
	return nil
}
