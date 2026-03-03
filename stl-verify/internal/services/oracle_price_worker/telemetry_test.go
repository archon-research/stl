package oracle_price_worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
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
	if tel.tracer == nil {
		t.Error("tracer is nil")
	}
	if tel.meter == nil {
		t.Error("meter is nil")
	}
	if tel.blocksProcessed == nil {
		t.Error("blocksProcessed counter is nil")
	}
	if tel.pricesChanged == nil {
		t.Error("pricesChanged counter is nil")
	}
	if tel.rpcCallsTotal == nil {
		t.Error("rpcCallsTotal counter is nil")
	}
	if tel.errorsTotal == nil {
		t.Error("errorsTotal counter is nil")
	}
	if tel.blockDuration == nil {
		t.Error("blockDuration histogram is nil")
	}
	if tel.rpcDuration == nil {
		t.Error("rpcDuration histogram is nil")
	}
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
	if tel.tracer == nil {
		t.Error("tracer is nil")
	}
	if tel.meter == nil {
		t.Error("meter is nil")
	}
	if tel.blocksProcessed == nil {
		t.Error("blocksProcessed counter is nil")
	}
	if tel.pricesChanged == nil {
		t.Error("pricesChanged counter is nil")
	}
	if tel.rpcCallsTotal == nil {
		t.Error("rpcCallsTotal counter is nil")
	}
	if tel.errorsTotal == nil {
		t.Error("errorsTotal counter is nil")
	}
	if tel.blockDuration == nil {
		t.Error("blockDuration histogram is nil")
	}
	if tel.rpcDuration == nil {
		t.Error("rpcDuration histogram is nil")
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
