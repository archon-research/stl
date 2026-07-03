package oracle_price_worker

import (
	"context"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/oracle_price_worker"

// Telemetry provides OpenTelemetry metrics and tracing for the oracle price worker.
type Telemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Counters
	blocksProcessed metric.Int64Counter
	pricesChanged   metric.Int64Counter
	rpcCallsTotal   metric.Int64Counter
	errorsTotal     metric.Int64Counter

	// Histograms
	blockDuration metric.Float64Histogram
	rpcDuration   metric.Float64Histogram

	// chainAttr is the constant per-chain attribute attached to every metric.
	// One worker process serves one chain, so the value is fixed at
	// construction. It surfaces as the `chain` Prometheus label that the Vector
	// indexer alerts group by; without it those alerts render an empty chain.
	chainAttr attribute.KeyValue
}

// NewTelemetry creates a new Telemetry instance using the global providers.
// chain is the chain name (e.g. "optimism") attached as the `chain` label.
func NewTelemetry(chain string) (*Telemetry, error) {
	return NewTelemetryWithProviders(
		otel.GetTracerProvider(),
		otel.GetMeterProvider(),
		chain,
	)
}

// NewTelemetryWithProviders creates a new Telemetry instance with custom providers.
func NewTelemetryWithProviders(tp trace.TracerProvider, mp metric.MeterProvider, chain string) (*Telemetry, error) {
	tracer := tp.Tracer(instrumentationName)
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{
		tracer:    tracer,
		meter:     meter,
		chainAttr: attribute.String("chain", chain),
	}

	var err error

	t.blocksProcessed, err = meter.Int64Counter(
		"oracle.blocks.processed",
		metric.WithDescription("Total number of blocks processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating blocksProcessed counter: %w", err)
	}

	t.pricesChanged, err = meter.Int64Counter(
		"oracle.prices.changed",
		metric.WithDescription("Total number of price changes detected"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating pricesChanged counter: %w", err)
	}

	t.rpcCallsTotal, err = meter.Int64Counter(
		"oracle.rpc.calls.total",
		metric.WithDescription("Total number of RPC calls"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rpcCallsTotal counter: %w", err)
	}

	t.errorsTotal, err = meter.Int64Counter(
		"oracle.errors.total",
		metric.WithDescription("Total number of errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating errorsTotal counter: %w", err)
	}

	t.blockDuration, err = meter.Float64Histogram(
		"oracle.block.duration_seconds",
		metric.WithDescription("Duration of block processing in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating blockDuration histogram: %w", err)
	}

	t.rpcDuration, err = meter.Float64Histogram(
		"oracle.rpc.duration_seconds",
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

	attrs := metric.WithAttributes(t.chainAttr, telemetry.StatusAttr(err))

	t.blocksProcessed.Add(ctx, 1, attrs)
	t.blockDuration.Record(ctx, duration.Seconds(), attrs)
}

// RecordPricesChanged records the number of price changes detected for an oracle.
func (t *Telemetry) RecordPricesChanged(ctx context.Context, oracleName string, count int) {
	if t == nil {
		return
	}
	t.pricesChanged.Add(ctx, int64(count), metric.WithAttributes(
		t.chainAttr,
		attribute.String("oracle.name", oracleName),
	))
}

// RecordRPCCall records metrics for an RPC call.
func (t *Telemetry) RecordRPCCall(ctx context.Context, method string, duration time.Duration, err error) {
	if t == nil {
		return
	}

	attrs := []attribute.KeyValue{
		t.chainAttr,
		attribute.String("rpc.method", method),
	}

	attrs = append(attrs, telemetry.StatusAttr(err))

	t.rpcCallsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.rpcDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordError records an error with the operation context.
func (t *Telemetry) RecordError(ctx context.Context, operation string, err error) {
	if t == nil || err == nil {
		return
	}
	t.errorsTotal.Add(ctx, 1, metric.WithAttributes(
		t.chainAttr,
		attribute.String("operation", operation),
	))
}

// StartBlockSpan starts a top-level span for block processing.
func (t *Telemetry) StartBlockSpan(ctx context.Context, blockNumber int64) (context.Context, trace.Span) {
	if t == nil {
		return ctx, telemetry.NoopSpan()
	}
	return t.tracer.Start(ctx, "oracle.processBlock",
		trace.WithAttributes(
			attribute.Int64("block.number", blockNumber),
		),
	)
}

// StartSpan starts a named child span with optional attributes.
func (t *Telemetry) StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if t == nil {
		return ctx, telemetry.NoopSpan()
	}
	return t.tracer.Start(ctx, name,
		trace.WithAttributes(attrs...),
	)
}
