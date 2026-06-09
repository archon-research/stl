package maple_indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/maple_indexer"

// Telemetry provides OpenTelemetry metrics and tracing for the Maple indexer.
type Telemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Counters
	blocksProcessed  metric.Int64Counter // blocks (= SQS messages) processed, labelled success/error
	vaultStateWrites metric.Int64Counter // maple_vault_state rows written
	positionWrites   metric.Int64Counter // maple_vault_position rows written
	rpcCallsTotal    metric.Int64Counter // RPC calls made, labelled by method + success/error
	errorsTotal      metric.Int64Counter // errors, labelled by operation

	// Histograms
	// blockDuration is the end-to-end latency of processing one block event:
	// one SQS message = one block, so this is the per-event processing time
	// (cache fetch → vault state/position writes). Use it for "how long to
	// process 1 event".
	blockDuration metric.Float64Histogram
	rpcDuration   metric.Float64Histogram // per-RPC-call latency, labelled by method
}

// NewTelemetry creates a new Telemetry instance using the global providers.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProviders(
		otel.GetTracerProvider(),
		otel.GetMeterProvider(),
	)
}

// NewTelemetryWithProviders creates a Telemetry instance with custom providers.
func NewTelemetryWithProviders(tp trace.TracerProvider, mp metric.MeterProvider) (*Telemetry, error) {
	tracer := tp.Tracer(instrumentationName)
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{tracer: tracer, meter: meter}

	var err error
	t.blocksProcessed, err = meter.Int64Counter(
		"maple.blocks.processed",
		metric.WithDescription("Total number of blocks processed by the maple indexer"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating blocksProcessed counter: %w", err)
	}

	t.vaultStateWrites, err = meter.Int64Counter(
		"maple.vault_state.writes",
		metric.WithDescription("Total number of maple_vault_state rows written"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating vaultStateWrites counter: %w", err)
	}

	t.positionWrites, err = meter.Int64Counter(
		"maple.vault_position.writes",
		metric.WithDescription("Total number of maple_vault_position rows written"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating positionWrites counter: %w", err)
	}

	t.rpcCallsTotal, err = meter.Int64Counter(
		"maple.rpc.calls.total",
		metric.WithDescription("Total number of RPC calls"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rpcCallsTotal counter: %w", err)
	}

	t.errorsTotal, err = meter.Int64Counter(
		"maple.errors.total",
		metric.WithDescription("Total number of errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating errorsTotal counter: %w", err)
	}

	t.blockDuration, err = meter.Float64Histogram(
		"maple.block.duration_seconds",
		metric.WithDescription("Duration of block processing in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating blockDuration histogram: %w", err)
	}

	t.rpcDuration, err = meter.Float64Histogram(
		"maple.rpc.duration_seconds",
		metric.WithDescription("Duration of RPC calls in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rpcDuration histogram: %w", err)
	}

	return t, nil
}

// RecordBlockProcessed records metrics for a processed block.
func (t *Telemetry) RecordBlockProcessed(ctx context.Context, duration time.Duration, err error) {
	if t == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	attrs := metric.WithAttributes(attribute.String("status", status))
	t.blocksProcessed.Add(ctx, 1, attrs)
	t.blockDuration.Record(ctx, duration.Seconds(), attrs)
}

// RecordVaultStateWrite records that a maple_vault_state row was written.
func (t *Telemetry) RecordVaultStateWrite(ctx context.Context) {
	if t == nil {
		return
	}
	t.vaultStateWrites.Add(ctx, 1)
}

// RecordPositionWrites records that N maple_vault_position rows were written.
func (t *Telemetry) RecordPositionWrites(ctx context.Context, n int64) {
	if t == nil || n == 0 {
		return
	}
	t.positionWrites.Add(ctx, n)
}

// RecordRPCCall records metrics for an RPC call.
func (t *Telemetry) RecordRPCCall(ctx context.Context, method string, duration time.Duration, err error) {
	if t == nil {
		return
	}
	attrs := []attribute.KeyValue{attribute.String("rpc.method", method)}
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
	return t.tracer.Start(ctx, "maple.processBlock",
		trace.WithAttributes(attribute.Int64("block.number", blockNumber)),
	)
}

// StartSpan starts a named child span with optional attributes.
func (t *Telemetry) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if t == nil {
		return ctx, noopSpan()
	}
	return t.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
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
