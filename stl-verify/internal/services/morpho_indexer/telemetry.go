package morpho_indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"

// v2SnapshotType names the structured VaultV2 table a snapshot landed in.
type v2SnapshotType string

const (
	v2SnapshotAdapterState v2SnapshotType = "adapter_state"
	v2SnapshotVaultCap     v2SnapshotType = "vault_cap"
	v2SnapshotVaultFee     v2SnapshotType = "vault_fee"
)

// adapterTypeLabel renders an adapter classification as a metric label. A value
// outside the modelled set renders as its numeric form rather than collapsing
// into "unknown": Unknown (99) is a real classification the alerts count, so
// masking a newly added enum value as Unknown would corrupt that signal.
//
// A nil classification renders as "unprobed", which is a THIRD state and not a
// synonym for "unknown": 99 means an on-chain probe ran and could not classify
// the adapter, while unprobed means the observation carried no probe at all —
// the shape a de-registration takes, since an adapter first seen by its own
// removal has no known type. VectorMorphoV2UnknownAdapters counts only the
// former, so collapsing the two would make removals look like probe failures.
func adapterTypeLabel(t *entity.MorphoAdapterType) string {
	if t == nil {
		return "unprobed"
	}
	switch *t {
	case entity.MorphoAdapterTypeMarketV1:
		return "market_v1"
	case entity.MorphoAdapterTypeVaultV1:
		return "vault_v1"
	case entity.MorphoAdapterTypeUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("type_%d", int16(*t))
	}
}

// Telemetry provides OpenTelemetry metrics and tracing for the Morpho indexer.
type Telemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Counters
	blocksProcessed      metric.Int64Counter
	eventsProcessed      metric.Int64Counter
	rpcCallsTotal        metric.Int64Counter
	errorsTotal          metric.Int64Counter
	adapterRegistrations metric.Int64Counter
	v2SnapshotsWritten   metric.Int64Counter

	// Histograms
	blockDuration   metric.Float64Histogram
	receiptDuration metric.Float64Histogram
	rpcDuration     metric.Float64Histogram

	// Gauges
	symbolsMissing metric.Int64Gauge

	// chainAttr is the constant per-chain attribute attached to every metric.
	// One indexer process serves one chain, so the value is fixed at
	// construction. It surfaces as the `chain` Prometheus label that the Vector
	// indexer alerts group by; without it those alerts render an empty chain.
	chainAttr attribute.KeyValue
}

// NewTelemetry creates a new Telemetry instance using the global providers.
// chain is the chain name (e.g. "arbitrum") attached as the `chain` label.
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

	t.adapterRegistrations, err = meter.Int64Counter(
		"morpho.v2.adapter.registrations",
		metric.WithDescription("VaultV2 adapter-membership observations appended, by on-chain classification and how the membership was observed"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating adapterRegistrations counter: %w", err)
	}

	t.v2SnapshotsWritten, err = meter.Int64Counter(
		"morpho.v2.snapshots.written",
		metric.WithDescription("VaultV2 structured snapshots committed by the event-driven handlers (adapter realAssets, allocation caps, fee config)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating v2SnapshotsWritten counter: %w", err)
	}

	t.symbolsMissing, err = meter.Int64Gauge(
		"morpho.token.symbol.missing",
		metric.WithDescription("Tokens still missing a symbol as seen by the latest reconciliation sweep (capped at the sweep batch size)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating symbolsMissing gauge: %w", err)
	}

	t.blockDuration, err = meter.Float64Histogram(
		"morpho.block.duration_seconds",
		metric.WithDescription("Duration of block processing in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating blockDuration histogram: %w", err)
	}

	t.receiptDuration, err = meter.Float64Histogram(
		"morpho.receipt.duration_seconds",
		metric.WithDescription("Duration of receipt processing in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating receiptDuration histogram: %w", err)
	}

	t.rpcDuration, err = meter.Float64Histogram(
		"morpho.rpc.duration_seconds",
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

// RecordEventProcessed records that an event was processed.
func (t *Telemetry) RecordEventProcessed(ctx context.Context, eventType string) {
	if t == nil {
		return
	}
	t.eventsProcessed.Add(ctx, 1, metric.WithAttributes(
		t.chainAttr,
		attribute.String("event.type", eventType),
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

// RecordAdapterMembershipObservation records that one VaultV2 adapter-membership
// observation was APPENDED to the log. Callers must not record when the
// repository appended nothing, so the counter means "observations recorded"
// rather than "write attempts" — otherwise every Allocate would increment it,
// thousands per day, and VectorMorphoV2LazyAdapterRegistrations would fire
// permanently.
//
// observed_via carries the same five values as
// morpho_adapter_membership.observed_via, so a PromQL question and a SQL question
// about provenance have the same vocabulary. adapter.type is the classification
// the observation carried, with "unprobed" for the removals that carry none.
//
// Callers accumulate their appends and flush them here only after the transaction
// that made them commits (recordMembershipObservations), so a rolled-back append —
// which an SQS redelivery repeats every visibility timeout for as long as a block
// stays stuck — cannot inflate the counter against a table that gained no rows.
func (t *Telemetry) RecordAdapterMembershipObservation(ctx context.Context, adapterType *entity.MorphoAdapterType, observedVia entity.MembershipSource) {
	if t == nil {
		return
	}
	t.adapterRegistrations.Add(ctx, 1, metric.WithAttributes(
		t.chainAttr,
		attribute.String("adapter.type", adapterTypeLabel(adapterType)),
		attribute.String("observed_via", string(observedVia)),
	))
}

// RecordV2Snapshot records one committed VaultV2 structured snapshot. Callers
// record after their write transaction returns and only when the writer reports a
// row appended, so the counter never claims a row that was rolled back — or that
// deduped to no row, which same-block siblings and a redelivery re-running an
// already-committed handler both do.
//
// This counter is the liveness signal for the EVENT-DRIVEN write path, so exactly
// two writers of these tables stay uncounted, both deliberately: discovery's
// adapter_state seeds (seedDiscoveredAdapters) and its vault_fee seed
// (seedDiscoveredFees). Both fire on vault registration, not on a V2 log, so
// counting them would let a run of new vaults mask a dead event path. Every other
// writer counts — including the adapter_state seed an AddAdapter commits
// (saveAdapterSeedState), which an AddAdapter log drives.
func (t *Telemetry) RecordV2Snapshot(ctx context.Context, snapshotType v2SnapshotType) {
	if t == nil {
		return
	}
	t.v2SnapshotsWritten.Add(ctx, 1, metric.WithAttributes(
		t.chainAttr,
		attribute.String("snapshot.type", string(snapshotType)),
	))
}

// RecordSymbolsMissing records how many tokens the latest reconciliation sweep
// found still missing a symbol (capped at the sweep batch size). Sustained growth
// means unresolvable tokens are accumulating; at the batch cap the oldest-first
// sweep starves newer tokens.
func (t *Telemetry) RecordSymbolsMissing(ctx context.Context, count int64) {
	if t == nil {
		return
	}
	t.symbolsMissing.Record(ctx, count, metric.WithAttributes(t.chainAttr))
}

// StartBlockSpan starts a top-level span for block processing.
func (t *Telemetry) StartBlockSpan(ctx context.Context, blockNumber int64) (context.Context, trace.Span) {
	if t == nil {
		return ctx, telemetry.NoopSpan()
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
		return ctx, telemetry.NoopSpan()
	}
	return t.tracer.Start(ctx, name,
		trace.WithAttributes(attrs...),
	)
}
