package morpho_indexer

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// newRecordingTelemetry returns a Telemetry wired to an in-memory metric reader
// so tests can record calls and inspect the resulting metrics.
func newRecordingTelemetry(t *testing.T) (*Telemetry, sdkmetric.Reader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tel, err := NewTelemetryWithProviders(tracenoop.NewTracerProvider(), mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders() error: %v", err)
	}
	return tel, reader
}

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

// TestSecondsHistograms_UseSecondsBuckets guards against the bucket-boundary bug
// behind the VectorMorphoIndexerRPCLatencyHigh alert. Without explicit
// boundaries the SDK applies its default millisecond-scale buckets
// ([0,5,10,...]), so a seconds-valued metric collapses into the (0,5] bucket and
// histogram_quantile(0.99,...) interpolates to 0.99*5 = 4.95s, tripping the >3s
// alert permanently. Every seconds histogram must instead use
// telemetry.SecondsDurationBuckets.
func TestSecondsHistograms_UseSecondsBuckets(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, 30*time.Millisecond, nil)
	tel.RecordRPCCall(ctx, "getMarketState", 30*time.Millisecond, nil)

	for _, name := range []string{
		"morpho.block.duration_seconds",
		"morpho.rpc.duration_seconds",
	} {
		if bounds := collectHistogramBounds(t, reader, name); !slices.Equal(bounds, telemetry.SecondsDurationBuckets) {
			t.Errorf("%s bounds = %v, want %v", name, bounds, telemetry.SecondsDurationBuckets)
		}
	}
}

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry("mainnet")
	if err != nil {
		t.Fatalf("NewTelemetry() returned error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetry() returned nil")
	}

	exerciseAllMethods(t, tel)
}

func TestNewTelemetryWithProviders(t *testing.T) {
	tel, err := NewTelemetryWithProviders(tracenoop.NewTracerProvider(), metricnoop.NewMeterProvider(), "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders() returned error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetryWithProviders() returned nil")
	}

	exerciseAllMethods(t, tel)
}

// exerciseAllMethods calls every public method, covering both the success and
// error-status branches of the recorders.
func exerciseAllMethods(t *testing.T, tel *Telemetry) {
	t.Helper()
	ctx := context.Background()
	someErr := errors.New("e")
	tel.RecordBlockProcessed(ctx, time.Second, nil)
	tel.RecordBlockProcessed(ctx, time.Second, someErr)
	tel.RecordEventProcessed(ctx, "Supply")
	tel.RecordRPCCall(ctx, "getMarketState", time.Millisecond, nil)
	tel.RecordRPCCall(ctx, "getMarketState", time.Millisecond, someErr)
	tel.RecordError(ctx, "op", someErr)

	_, span := tel.StartBlockSpan(ctx, 1)
	span.End()
	_, span = tel.StartSpan(ctx, "test.span", attribute.String("key", "value"))
	span.End()
}

// Guards the startup seed: VectorMorphoIndexerStalled reads
// morpho_blocks_processed_total with rate()==0 and must be computable from
// process start (a worker dead before its first block emits no series at
// all otherwise). See telemetry.SeedCounter.
func TestNewTelemetry_SeedsBlockStatusSeriesAtZero(t *testing.T) {
	_, reader := newRecordingTelemetry(t)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	got := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "morpho.blocks.processed" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("morpho.blocks.processed is %T, want metricdata.Sum[int64]", m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, _ := dp.Attributes.Value("status")
				got[status.AsString()] = dp.Value
			}
		}
	}
	for _, status := range []string{"success", "error"} {
		v, ok := got[status]
		if !ok {
			t.Errorf("morpho.blocks.processed missing status=%q series before any block", status)
			continue
		}
		if v != 0 {
			t.Errorf("morpho.blocks.processed{status=%q} = %d, want 0", status, v)
		}
	}
}

func TestTelemetry_NilSafe(t *testing.T) {
	var tel *Telemetry // nil pointer
	ctx := context.Background()
	someErr := errors.New("test error")

	// All methods must be no-ops on a nil receiver: no panics.
	t.Run("RecordBlockProcessed", func(t *testing.T) {
		tel.RecordBlockProcessed(ctx, time.Second, nil)
		tel.RecordBlockProcessed(ctx, time.Second, someErr)
	})

	t.Run("RecordEventProcessed", func(t *testing.T) {
		tel.RecordEventProcessed(ctx, "Supply")
	})

	t.Run("RecordRPCCall", func(t *testing.T) {
		tel.RecordRPCCall(ctx, "getMarketState", 100*time.Millisecond, nil)
		tel.RecordRPCCall(ctx, "getMarketState", 100*time.Millisecond, someErr)
	})

	t.Run("RecordError", func(t *testing.T) {
		tel.RecordError(ctx, "processBlock", someErr)
		tel.RecordError(ctx, "processBlock", nil)
	})

	t.Run("StartBlockSpan", func(t *testing.T) {
		retCtx, span := tel.StartBlockSpan(ctx, 12345)
		if retCtx == nil {
			t.Error("StartBlockSpan returned nil context")
		}
		if span == nil {
			t.Error("StartBlockSpan returned nil span")
		}
		span.End()
	})

	t.Run("StartSpan", func(t *testing.T) {
		retCtx, span := tel.StartSpan(ctx, "test.span", attribute.String("key", "value"))
		if retCtx == nil {
			t.Error("StartSpan returned nil context")
		}
		if span == nil {
			t.Error("StartSpan returned nil span")
		}
		span.End()
	})

	t.Run("SetSpanError", func(t *testing.T) {
		// SetSpanError is a package-level function, not a method.
		span := telemetry.NoopSpan()
		telemetry.SetSpanError(span, nil, "should be no-op")
		telemetry.SetSpanError(span, someErr, "test error description")
	})
}
