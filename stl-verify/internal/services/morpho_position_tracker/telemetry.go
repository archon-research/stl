package morpho_position_tracker

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/morpho_position_tracker"

// Telemetry provides OpenTelemetry metrics and tracing for the Morpho position tracker.
type Telemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Counters
	blocksProcessed metric.Int64Counter
	eventsProcessed metric.Int64Counter
	rpcCallsTotal   metric.Int64Counter
	errorsTotal     metric.Int64Counter

	// Histograms
	blockDuration   metric.Float64Histogram
	receiptDuration metric.Float64Histogram
	rpcDuration     metric.Float64Histogram
}

// NewTelemetry creates a new Telemetry instance using the global providers.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProviders(
		otel.GetTracerProvider(),
		otel.GetMeterProvider(),
	)
}

// NewTelemetryWithProviders creates a new Telemetry instance with custom providers.
func NewTelemetryWithProviders(tp trace.TracerProvider, mp metric.MeterProvider) (*Telemetry, error) {
	tracer := tp.Tracer(instrumentationName)
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{
		tracer: tracer,
		meter:  meter,
	}

	var err error

	t.blocksProcessed, err = meter.Int64Counter(
		"morpho.blocks.processed",
		metric.WithDescription("Total number of blocks processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating blocksProcessed counter: %w", err)
	}

	t.eventsProcessed, err = meter.Int64Counter(
		"morpho.events.processed",
		metric.WithDescription("Total number of events processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating eventsProcessed counter: %w", err)
	}

	t.rpcCallsTotal, err = meter.Int64Counter(
		"morpho.rpc.calls.total",
		metric.WithDescription("Total number of RPC calls"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rpcCallsTotal counter: %w", err)
	}

	t.errorsTotal, err = meter.Int64Counter(
		"morpho.errors.total",
		metric.WithDescription("Total number of errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating errorsTotal counter: %w", err)
	}

	t.blockDuration, err = meter.Float64Histogram(
		"morpho.block.duration_seconds",
		metric.WithDescription("Duration of block processing in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating blockDuration histogram: %w", err)
	}

	t.receiptDuration, err = meter.Float64Histogram(
		"morpho.receipt.duration_seconds",
		metric.WithDescription("Duration of receipt processing in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating receiptDuration histogram: %w", err)
	}

	t.rpcDuration, err = meter.Float64Histogram(
		"morpho.rpc.duration_seconds",
		metric.WithDescription("Duration of RPC calls in seconds"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rpcDuration histogram: %w", err)
	}

	return t, nil
}

// RecordBlockProcessed records metrics for a processed block.
func (t *Telemetry) RecordBlockProcessed(ctx context.Context, blockNumber int64, duration time.Duration, err error) {
	if t == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.Int64("block.number", blockNumber),
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	attrs = append(attrs, attribute.String("status", status))

	t.blocksProcessed.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.blockDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordEventProcessed records that an event was processed.
func (t *Telemetry) RecordEventProcessed(ctx context.Context, eventType string) {
	if t == nil {
		return
	}
	t.eventsProcessed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event.type", eventType),
	))
}

// RecordRPCCall records metrics for an RPC call.
func (t *Telemetry) RecordRPCCall(ctx context.Context, method string, duration time.Duration, err error) {
	if t == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("rpc.method", method),
	}

	status := "success"
	if err != nil {
		status = "error"
	}
	attrs = append(attrs, attribute.String("status", status))

	t.rpcCallsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.rpcDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordError records an error with the operation context.
func (t *Telemetry) RecordError(ctx context.Context, operation string, err error) {
	if t == nil || err == nil {
		return
	}
	t.errorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation),
	))
}

// StartBlockSpan starts a top-level span for block processing.
func (t *Telemetry) StartBlockSpan(ctx context.Context, blockNumber int64) (context.Context, trace.Span) {
	if t == nil {
		return ctx, noopSpan()
	}
	return t.tracer.Start(ctx, "morpho.processBlock",
		trace.WithAttributes(
			attribute.Int64("block.number", blockNumber),
		),
	)
}

// StartSpan starts a named child span with optional attributes.
func (t *Telemetry) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if t == nil {
		return ctx, noopSpan()
	}
	return t.tracer.Start(ctx, name,
		trace.WithAttributes(attrs...),
	)
}

// SetSpanError records an error on a span and sets its status.
func SetSpanError(span trace.Span, err error, description string) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, description)
}

func noopSpan() trace.Span {
	return trace.SpanFromContext(context.Background())
}
