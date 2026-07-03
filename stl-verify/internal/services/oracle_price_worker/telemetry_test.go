package oracle_price_worker

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

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
	tp := tracenoop.NewTracerProvider()
	mp := metricnoop.NewMeterProvider()

	tel, err := NewTelemetryWithProviders(tp, mp, "mainnet")
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
	tel.RecordPricesChanged(ctx, "test", 1)
	tel.RecordRPCCall(ctx, "getAssetsPrices", time.Millisecond, nil)
	tel.RecordRPCCall(ctx, "getAssetsPrices", time.Millisecond, someErr)
	tel.RecordError(ctx, "op", someErr)

	_, span := tel.StartBlockSpan(ctx, 1)
	span.End()
	_, span = tel.StartSpan(ctx, "test.span")
	span.End()
}

func TestNewTelemetryWithProviders_CreatesSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	mp := metricnoop.NewMeterProvider()

	tel, err := NewTelemetryWithProviders(tp, mp, "mainnet")
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders() error: %v", err)
	}

	ctx := context.Background()
	ctx, blockSpan := tel.StartBlockSpan(ctx, 42)
	_, childSpan := tel.StartSpan(ctx, "oracle.fetchPrices", attribute.String("rpc.method", "getAssetsPrices"))
	childSpan.End()
	blockSpan.End()

	_ = tp.ForceFlush(context.Background())

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("expected 2 spans, got %d", len(spans))
	}

	// Child span is exported first (ended first).
	if spans[0].Name != "oracle.fetchPrices" {
		t.Errorf("span[0].Name = %q, want %q", spans[0].Name, "oracle.fetchPrices")
	}
	if spans[1].Name != "oracle.processBlock" {
		t.Errorf("span[1].Name = %q, want %q", spans[1].Name, "oracle.processBlock")
	}

	// Verify parent-child relationship.
	if spans[0].Parent.SpanID() != spans[1].SpanContext.SpanID() {
		t.Error("fetchPrices span should be a child of processBlock span")
	}
}

// newRecordingTelemetry returns a Telemetry wired to an in-memory metric reader
// so tests can record RPC calls and inspect the resulting histogram.
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
// behind the VectorOracleIndexerRPCLatencyHigh alert. Without explicit
// boundaries the SDK applies its default millisecond-scale buckets
// ([0,5,10,...]), so a seconds-valued metric collapses into the (0,5] bucket and
// histogram_quantile(0.99,...) interpolates to 0.99*5 = 4.95s, tripping the >3s
// alert permanently. Every seconds histogram must instead use
// telemetry.SecondsDurationBuckets.
func TestSecondsHistograms_UseSecondsBuckets(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, 30*time.Millisecond, nil)
	tel.RecordRPCCall(ctx, "getAssetsPrices", 30*time.Millisecond, nil)

	for _, name := range []string{
		"oracle.block.duration_seconds",
		"oracle.rpc.duration_seconds",
	} {
		if bounds := collectHistogramBounds(t, reader, name); !slices.Equal(bounds, telemetry.SecondsDurationBuckets) {
			t.Errorf("%s bounds = %v, want %v", name, bounds, telemetry.SecondsDurationBuckets)
		}
	}
}

// Guards the startup seed: VectorOracleIndexerStalled reads
// oracle_blocks_processed_total with rate()==0 and must be computable from
// process start (a worker dead before its first block emits no series at
// all otherwise). See telemetry.SeedCounter.
func TestNewTelemetry_SeedsBlockStatusSeriesAtZero(t *testing.T) {
	_, reader := newRecordingTelemetry(t)

	dps := testutil.CollectSumDataPoints(t, reader, "oracle.blocks.processed")
	got := map[string]int64{}
	for _, dp := range dps {
		if chain := testutil.AttrValue(dp, "chain"); chain != "mainnet" {
			t.Errorf("oracle.blocks.processed chain attr = %q, want %q", chain, "mainnet")
		}
		got[testutil.AttrValue(dp, "status")] = dp.Value
	}
	for _, status := range []string{"success", "error"} {
		v, ok := got[status]
		if !ok {
			t.Errorf("oracle.blocks.processed missing status=%q series before any block", status)
			continue
		}
		if v != 0 {
			t.Errorf("oracle.blocks.processed{status=%q} = %d, want 0", status, v)
		}
	}
}

func TestTelemetry_NilSafe(t *testing.T) {
	var tel *Telemetry // nil pointer
	ctx := context.Background()
	someErr := errors.New("test error")

	// All methods must be no-ops on nil receiver: no panics.
	t.Run("RecordBlockProcessed", func(t *testing.T) {
		tel.RecordBlockProcessed(ctx, time.Second, nil)
		tel.RecordBlockProcessed(ctx, time.Second, someErr)
	})

	t.Run("RecordPricesChanged", func(t *testing.T) {
		tel.RecordPricesChanged(ctx, "test-oracle", 5)
	})

	t.Run("RecordRPCCall", func(t *testing.T) {
		tel.RecordRPCCall(ctx, "eth_call", time.Millisecond*100, nil)
		tel.RecordRPCCall(ctx, "eth_call", time.Millisecond*100, someErr)
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
		// It should handle nil errors gracefully.
		span := telemetry.NoopSpan()
		telemetry.SetSpanError(span, nil, "should be no-op")
		telemetry.SetSpanError(span, someErr, "test error description")
	})
}
