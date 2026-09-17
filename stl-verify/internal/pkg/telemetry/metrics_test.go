package telemetry

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
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

// prime-allocation-indexer and raw-data-backup are alerted on this counter with
// a bare rate()==0 and no zero-fill, so a worker that wedges before its first
// block must still produce the series or neither page can ever fire.
func TestNewMetrics_SeedsBlocksProcessedAtZero(t *testing.T) {
	// Installed first: otel's global delegation is one-shot, so an instrument
	// built before any provider exists only reaches this reader in a pristine
	// process. OnMeterProviderReady's deferred path is covered in meter_test.go;
	// this asserts the seed itself.
	resetStartupSeeds(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	SetMeterProvider(mp)

	if _, err := NewMetrics("stl-verify/test", "arbitrum"); err != nil {
		t.Fatalf("NewMetrics() error: %v", err)
	}

	dps := collectBlocksProcessedDataPoints(t, reader)
	if len(dps) != 2 {
		t.Fatalf("blocks_processed_total has %d series before any block, want 2 (success and error)", len(dps))
	}
	for _, dp := range dps {
		status, _ := dp.Attributes.Value("status")
		chain, _ := dp.Attributes.Value("chain")
		if dp.Value != 0 {
			t.Errorf("blocks_processed_total{status=%q} = %d, want 0", status.AsString(), dp.Value)
		}
		if chain.AsString() != "arbitrum" {
			t.Errorf("blocks_processed_total{status=%q} chain = %q, want %q", status.AsString(), chain.AsString(), "arbitrum")
		}
	}
}

// The seeded series and the series RecordBlockProcessed writes must be one
// series: a mismatch leaves the seeded one flat at 0 forever while the real
// counts land elsewhere, which reads as fixed and is not.
func TestRecordBlockProcessed_LandsOnTheSeededSeries(t *testing.T) {
	resetStartupSeeds(t)
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	SetMeterProvider(mp)

	m, err := NewMetrics("stl-verify/test", "arbitrum")
	if err != nil {
		t.Fatalf("NewMetrics() error: %v", err)
	}

	m.RecordBlockProcessed(context.Background(), SuccessStatusAttr().Value.AsString())

	dps := collectBlocksProcessedDataPoints(t, reader)
	if len(dps) != 2 {
		t.Fatalf("blocks_processed_total has %d series after one success, want 2 — the record orphaned the seeded series", len(dps))
	}
	for _, dp := range dps {
		status, _ := dp.Attributes.Value("status")
		want := int64(0)
		if status.AsString() == SuccessStatusAttr().Value.AsString() {
			want = 1
		}
		if dp.Value != want {
			t.Errorf("blocks_processed_total{status=%q} = %d, want %d", status.AsString(), dp.Value, want)
		}
	}
}

func collectBlocksProcessedDataPoints(t *testing.T, reader sdkmetric.Reader) []metricdata.DataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, metricValue := range scope.Metrics {
			if metricValue.Name != "blocks_processed_total" {
				continue
			}
			sum, ok := metricValue.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("blocks_processed_total is %T, want metricdata.Sum[int64]", metricValue.Data)
			}
			return sum.DataPoints
		}
	}
	t.Fatalf("blocks_processed_total not found; a nil return here would make every assertion vacuous")
	return nil
}
