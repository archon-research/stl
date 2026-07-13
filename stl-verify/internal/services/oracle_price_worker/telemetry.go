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
	blocksProcessed   metric.Int64Counter
	pricesChanged     metric.Int64Counter
	rpcCallsTotal     metric.Int64Counter
	errorsTotal       metric.Int64Counter
	unitPricesFetched metric.Int64Counter

	// Histograms
	blockDuration metric.Float64Histogram
	rpcDuration   metric.Float64Histogram

	// Gauges
	unitLastSuccess metric.Float64Gauge

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

	// Per-unit freshness is tracked per ORACLE, not per token: the label set is
	// bounded by the oracle registry (a handful of series per chain), whereas a
	// per-token label would mint a series per feed and grow with every asset
	// onboarding. The failure modes this signal exists for (a unit erroring on
	// every block, including a misconfigured unit that has never succeeded,
	// thanks to the load-time baseline) hit the whole unit anyway; a single
	// dark feed inside a healthy unit shows up as oracle.unit.prices_fetched
	// falling below the unit's feed count, not here. Known limit: a unit
	// dropped from the registry entirely stops exporting the series, and an
	// absent series cannot age, so that case is invisible to the staleness
	// alert (it needs an expected-units signal, tracked as follow-up).
	t.unitLastSuccess, err = meter.Float64Gauge(
		"oracle.unit.last_success_timestamp_seconds",
		metric.WithDescription("Unix timestamp of the last successful per-oracle processing pass, baselined to load time at startup"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating unitLastSuccess gauge: %w", err)
	}

	t.unitPricesFetched, err = meter.Int64Counter(
		"oracle.unit.prices_fetched",
		metric.WithDescription("Usable prices fetched per oracle unit (successful, non-zero reads), before change detection"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating unitPricesFetched counter: %w", err)
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

// RecordUnitSuccess marks a successful per-oracle processing pass by setting
// the unit's last-success timestamp to the current wall-clock time. It must be
// recorded on every successful pass regardless of whether rows were written:
// change-only persistence means a healthy unit can go hours without a write,
// so staleness has to mean "the worker stopped successfully processing this
// unit", never "prices stopped changing".
func (t *Telemetry) RecordUnitSuccess(ctx context.Context, oracleName string) {
	if t == nil {
		return
	}
	t.recordUnitFreshness(ctx, oracleName)
}

// RecordUnitLoaded baselines the unit's freshness gauge at load time. Without
// it, a unit that never completes a successful pass (a misconfigured feed
// hard-erroring from the first block after a deploy) never creates the
// series, an absent series cannot age, and a pod restart would silently
// resolve a firing staleness alert forever. With the baseline, such a unit
// goes stale one threshold after startup and a restart merely re-arms the
// alert.
func (t *Telemetry) RecordUnitLoaded(ctx context.Context, oracleName string) {
	if t == nil {
		return
	}
	t.recordUnitFreshness(ctx, oracleName)
}

// recordUnitFreshness records fractional seconds (not whole) so consecutive
// recordings within one second stay distinguishable, e.g. the startup
// baseline versus the first pass.
func (t *Telemetry) recordUnitFreshness(ctx context.Context, oracleName string) {
	t.unitLastSuccess.Record(ctx, float64(time.Now().UnixNano())/1e9, metric.WithAttributes(
		t.chainAttr,
		attribute.String("oracle.name", oracleName),
	))
}

// RecordPricesFetched counts the usable prices a per-oracle pass fetched. A
// zero count is recorded on purpose: it creates/keeps the series so "the unit
// passes but every feed reverts" is queryable as a zero rate instead of being
// indistinguishable from an absent series (the uniswap-v3 NotWritingState
// lesson).
func (t *Telemetry) RecordPricesFetched(ctx context.Context, oracleName string, count int) {
	if t == nil {
		return
	}
	t.unitPricesFetched.Add(ctx, int64(count), metric.WithAttributes(
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
