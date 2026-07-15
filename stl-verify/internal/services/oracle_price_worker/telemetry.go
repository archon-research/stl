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
	unitReadsFailed   metric.Int64Counter

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
	t := &Telemetry{
		tracer:    tp.Tracer(instrumentationName),
		meter:     mp.Meter(instrumentationName),
		chainAttr: attribute.String("chain", chain),
	}
	if err := t.initWorkerCounters(); err != nil {
		return nil, err
	}
	if err := t.initDurationHistograms(); err != nil {
		return nil, err
	}
	if err := t.initUnitInstruments(); err != nil {
		return nil, err
	}
	return t, nil
}

// initWorkerCounters creates the worker's throughput and error counters
// (blocks, price changes, RPC calls, errors).
func (t *Telemetry) initWorkerCounters() error {
	var err error

	t.blocksProcessed, err = t.meter.Int64Counter(
		"oracle.blocks.processed",
		metric.WithDescription("Total number of blocks processed"),
	)
	if err != nil {
		return fmt.Errorf("creating blocksProcessed counter: %w", err)
	}

	t.pricesChanged, err = t.meter.Int64Counter(
		"oracle.prices.changed",
		metric.WithDescription("Total number of price changes detected"),
	)
	if err != nil {
		return fmt.Errorf("creating pricesChanged counter: %w", err)
	}

	t.rpcCallsTotal, err = t.meter.Int64Counter(
		"oracle.rpc.calls.total",
		metric.WithDescription("Total number of RPC calls"),
	)
	if err != nil {
		return fmt.Errorf("creating rpcCallsTotal counter: %w", err)
	}

	t.errorsTotal, err = t.meter.Int64Counter(
		"oracle.errors.total",
		metric.WithDescription("Total number of errors"),
	)
	if err != nil {
		return fmt.Errorf("creating errorsTotal counter: %w", err)
	}

	return nil
}

// initDurationHistograms creates the seconds-valued histograms. Every one of
// them must use telemetry.SecondsDurationBuckets: the SDK's default buckets
// are millisecond-scaled and collapse seconds metrics into the (0,5] bucket,
// pinning the p99 histogram_quantile at 0.99*5 = 4.95s.
func (t *Telemetry) initDurationHistograms() error {
	var err error

	t.blockDuration, err = t.meter.Float64Histogram(
		"oracle.block.duration_seconds",
		metric.WithDescription("Duration of block processing in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return fmt.Errorf("creating blockDuration histogram: %w", err)
	}

	t.rpcDuration, err = t.meter.Float64Histogram(
		"oracle.rpc.duration_seconds",
		metric.WithDescription("Duration of RPC calls in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return fmt.Errorf("creating rpcDuration histogram: %w", err)
	}

	return nil
}

// initUnitInstruments creates the per-oracle-unit instruments.
//
// They are tracked per ORACLE, not per token: the label set is bounded by the
// oracle registry (a handful of series per chain), whereas a per-token label
// would mint a series per feed and grow with every asset onboarding. The
// failure modes the freshness gauge exists for (a unit erroring on every
// block, including a misconfigured unit that has never succeeded, thanks to
// the load-time baseline) hit the whole unit anyway; a single dark feed
// inside a healthy unit shows up as a nonzero oracle.unit.reads_failed rate
// (the VectorOracleUnitReadsFailing alert), not here. Known limit: a unit
// dropped from the registry entirely stops exporting its series, and an
// absent series cannot age, so that case is invisible to the staleness alert
// (it needs an expected-units signal, tracked as follow-up).
func (t *Telemetry) initUnitInstruments() error {
	var err error

	t.unitLastSuccess, err = t.meter.Float64Gauge(
		"oracle.unit.last_success_timestamp_seconds",
		metric.WithDescription("Unix timestamp of the last successful per-oracle processing pass, baselined to load time at startup"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return fmt.Errorf("creating unitLastSuccess gauge: %w", err)
	}

	t.unitPricesFetched, err = t.meter.Int64Counter(
		"oracle.unit.prices_fetched",
		metric.WithDescription("Usable prices fetched per oracle unit (successful, non-zero reads), before change detection"),
	)
	if err != nil {
		return fmt.Errorf("creating unitPricesFetched counter: %w", err)
	}

	t.unitReadsFailed, err = t.meter.Int64Counter(
		"oracle.unit.reads_failed",
		metric.WithDescription("Per-oracle-unit reads that produced no usable price (reverted feed or zero quote): expected reads minus fetched, per pass"),
	)
	if err != nil {
		return fmt.Errorf("creating unitReadsFailed counter: %w", err)
	}

	return nil
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

// RecordUnitReads counts one per-oracle pass's read outcomes: fetched is the
// number of usable prices (successful, non-zero reads) and expected is the
// number of read outcomes the pass produced (the fetchers guarantee one
// outcome per read performed; the curve path folds its whole batch into a
// single LP-price outcome), so the difference lands on the failed-reads
// counter. Recording both from one call keeps the two series in lockstep,
// and zeros are recorded on purpose: they create/keep the series so "the
// unit passes but a feed reverts every block" is queryable as a nonzero
// failed rate (and total feed loss as a zero fetched rate) instead of being
// indistinguishable from an absent series (the uniswap-v3 NotWritingState
// lesson).
func (t *Telemetry) RecordUnitReads(ctx context.Context, oracleName string, fetched, expected int) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(
		t.chainAttr,
		attribute.String("oracle.name", oracleName),
	)
	t.unitPricesFetched.Add(ctx, int64(fetched), attrs)
	failed := expected - fetched
	if failed < 0 {
		// fetched > expected is by definition a caller bug, not a feed
		// outcome (every fetcher yields exactly one outcome per read). Clamp
		// rather than forward: the OTel sum aggregator adds negative values
		// unconditionally, which would corrupt the cumulative counter and
		// make rate() misread the decrease as a counter reset. Loud, not
		// silent: the bug lands on the errors counter, where a persistent
		// occurrence trips VectorOracleIndexerErrorsHigh.
		t.errorsTotal.Add(ctx, 1, metric.WithAttributes(
			t.chainAttr,
			attribute.String("operation", "unit_reads_accounting"),
		))
		failed = 0
	}
	t.unitReadsFailed.Add(ctx, int64(failed), attrs)
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
