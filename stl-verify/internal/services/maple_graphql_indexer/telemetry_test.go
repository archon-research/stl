package maple_graphql_indexer

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func newTestTelemetry(t *testing.T) *Telemetry {
	t.Helper()
	tp := sdktrace.NewTracerProvider()
	mp := sdkmetric.NewMeterProvider()
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	})

	tel, err := NewTelemetryWithProviders(tp, mp)
	if err != nil {
		t.Fatalf("NewTelemetryWithProviders: %v", err)
	}
	return tel
}

func TestNewTelemetry(t *testing.T) {
	tel, err := NewTelemetry()
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	if tel == nil {
		t.Fatal("telemetry is nil")
	}
}

func TestTelemetry_RecordsDoNotPanic(t *testing.T) {
	tel := newTestTelemetry(t)
	ctx := context.Background()

	ctx, cycleSpan := tel.StartCycleSpan(ctx, time.Now())
	phaseCtx, phaseSpan := tel.StartPhaseSpan(ctx, "pools")

	tel.RecordPhase(phaseCtx, "pools", 250*time.Millisecond, nil)
	tel.RecordPhase(phaseCtx, "loans", time.Second, errors.New("boom"))
	tel.RecordRowsWritten(phaseCtx, "maple_pool_state", 21)
	tel.RecordCycle(ctx, nil)
	tel.RecordCycle(ctx, errors.New("boom"))

	SetSpanError(phaseSpan, errors.New("boom"), "phase failed")
	SetSpanError(cycleSpan, nil, "ignored for nil error")
	phaseSpan.End()
	cycleSpan.End()
}

func TestTelemetry_NilReceiverSafe(t *testing.T) {
	var tel *Telemetry
	ctx := context.Background()

	cycleCtx, cycleSpan := tel.StartCycleSpan(ctx, time.Now())
	if cycleCtx == nil || cycleSpan == nil {
		t.Fatal("nil telemetry must return usable ctx/span")
	}
	phaseCtx, phaseSpan := tel.StartPhaseSpan(ctx, "pools")
	if phaseCtx == nil || phaseSpan == nil {
		t.Fatal("nil telemetry must return usable ctx/span")
	}

	// None of these may panic on a nil receiver.
	tel.RecordCycle(ctx, nil)
	tel.RecordPhase(ctx, "pools", time.Second, nil)
	tel.RecordRowsWritten(ctx, "maple_pool_state", 1)
	cycleSpan.End()
	phaseSpan.End()
}

// failingMeter errors on the instrument whose name matches failOn.
type failingMeter struct {
	noop.Meter
	failOn string
}

func (m failingMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == m.failOn {
		return nil, errors.New("instrument creation failed")
	}
	return m.Meter.Int64Counter(name, options...)
}

func (m failingMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == m.failOn {
		return nil, errors.New("instrument creation failed")
	}
	return m.Meter.Float64Histogram(name, options...)
}

type failingMeterProvider struct {
	noop.MeterProvider
	failOn string
}

func (p failingMeterProvider) Meter(name string, options ...metric.MeterOption) metric.Meter {
	return failingMeter{failOn: p.failOn}
}

func TestNewTelemetryWithProviders_InstrumentErrors(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	instruments := []string{
		"maple.sync.cycles.total",
		"maple.sync.phases.total",
		"maple.sync.rows.written",
		"maple.sync.phase.duration_seconds",
	}
	for _, name := range instruments {
		t.Run(name, func(t *testing.T) {
			_, err := NewTelemetryWithProviders(tp, failingMeterProvider{failOn: name})
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), "instrument creation failed") {
				t.Errorf("error = %q", err.Error())
			}
		})
	}
}

func TestStatusAttr(t *testing.T) {
	if got := statusAttr(nil).Value.AsString(); got != "success" {
		t.Errorf("statusAttr(nil) = %q, want success", got)
	}
	if got := statusAttr(errors.New("x")).Value.AsString(); got != "error" {
		t.Errorf("statusAttr(err) = %q, want error", got)
	}
}
