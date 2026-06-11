package telemetry

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// TestNewMetrics_ProcessingDurationUsesSecondsBuckets guards against the
// bucket-boundary bug behind the VectorBackupWorkerLatencyHigh alert. The
// provider here has no view, so this exercises the instrument's own explicit
// boundaries rather than the defense-in-depth view.
func TestNewMetrics_ProcessingDurationUsesSecondsBuckets(t *testing.T) {
	ctx := context.Background()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })

	// NewMetrics reads the global meter provider, so set it for the test.
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() { otel.SetMeterProvider(prev) })

	m, err := NewMetrics("test", "mainnet")
	if err != nil {
		t.Fatalf("NewMetrics() error: %v", err)
	}
	m.RecordProcessingLatency(ctx, 30*time.Millisecond, "success")
	m.RecordBlockProcessed(ctx, "success")

	bounds := collectHistogramBounds(t, reader, "processing_duration_seconds")
	if !slices.Equal(bounds, SecondsDurationBuckets) {
		t.Errorf("processing_duration_seconds bounds = %v, want %v", bounds, SecondsDurationBuckets)
	}
}
