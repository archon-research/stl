package oracle_price_worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry()
	if err != nil {
		t.Fatalf("NewTelemetry() returned error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetry() returned nil")
	}

	// Verify the instance works by calling all public methods without panic.
	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, time.Second, nil)
	tel.RecordPricesChanged(ctx, "test", 1)
	tel.RecordRPCCall(ctx, "eth_call", time.Millisecond, nil)
	tel.RecordError(ctx, "op", errors.New("e"))

	_, span := tel.StartBlockSpan(ctx, 1)
	span.End()
	_, span = tel.StartSpan(ctx, "test.span")
	span.End()
}

func TestNewTelemetryWithProviders(t *testing.T) {
	tp := tracenoop.NewTracerProvider()
	mp := metricnoop.NewMeterProvider()

	tel, err := NewTelemetryWithProviders(tp, mp)
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders() returned error: %v", err)
	}
	if tel == nil {
		t.Fatal("NewTelemetryWithProviders() returned nil")
	}

	// Verify the instance works by calling all public methods without panic.
	ctx := context.Background()
	tel.RecordBlockProcessed(ctx, time.Second, nil)
	tel.RecordPricesChanged(ctx, "test", 1)
	tel.RecordRPCCall(ctx, "eth_call", time.Millisecond, nil)
	tel.RecordError(ctx, "op", errors.New("e"))

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

	tel, err := NewTelemetryWithProviders(tp, mp)
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

func TestTelemetry_NilSafe(t *testing.T) {
	var tel *Telemetry // nil pointer
	ctx := context.Background()
	someErr := errors.New("test error")

	// All methods must be no-ops on nil receiver — no panics.
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
		span := noopSpan()
		SetSpanError(span, nil, "should be no-op")
		SetSpanError(span, someErr, "test error description")
	})
}
