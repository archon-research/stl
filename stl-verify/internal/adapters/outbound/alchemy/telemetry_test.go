package alchemy

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
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

// TestRequestDurationHistogram_UsesSecondsBuckets guards against the
// bucket-boundary bug behind the VectorWatcherAlchemyLatencyHigh alert. Without
// explicit boundaries the SDK applies its default millisecond-scale buckets
// ([0,5,10,...]), so this seconds-valued metric collapses into the (0,5] bucket
// and histogram_quantile(0.99,...) pins at 0.99*5 = 4.95s, leaving the >5s alert
// blind to real regressions. The instrument must use telemetry.SecondsDurationBuckets.
func TestRequestDurationHistogram_UsesSecondsBuckets(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProviders(tracenoop.NewTracerProvider(), mp)
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders() error: %v", err)
	}
	tel.RecordRequest(context.Background(), "eth_call", 30*time.Millisecond, nil)

	bounds := collectHistogramBounds(t, reader, "alchemy.client.request.duration")
	if !slices.Equal(bounds, telemetry.SecondsDurationBuckets) {
		t.Errorf("alchemy.client.request.duration bounds = %v, want %v", bounds, telemetry.SecondsDurationBuckets)
	}
}
