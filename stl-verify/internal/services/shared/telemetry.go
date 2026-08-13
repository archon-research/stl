// Package shared provides shared utilities and instrumentation for services.
package shared

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Compile-time assertion that ServiceTelemetry implements ReorgRecorder and
// BackfillRecorder.
var (
	_ outbound.ReorgRecorder    = (*ServiceTelemetry)(nil)
	_ outbound.BackfillRecorder = (*ServiceTelemetry)(nil)
)

const (
	// instrumentationName is the name used for OpenTelemetry instrumentation.
	instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services"
)

// ServiceTelemetry provides OpenTelemetry metrics for service-level domain events.
// This is separate from adapter-level telemetry (e.g., alchemy.Telemetry) which
// tracks infrastructure concerns like HTTP requests and WebSocket connections.
type ServiceTelemetry struct {
	meter metric.Meter

	// Chain metrics
	reorgsTotal        metric.Int64Counter
	reorgsDroppedTotal metric.Int64Counter
	outOfOrderBlocks   metric.Int64Counter

	// Backfill invariant metrics
	backfillGapNoCanonicalTotal metric.Int64Counter
	backfillWatermarkLag        metric.Int64Gauge
}

// NewServiceTelemetry creates a new ServiceTelemetry instance with OpenTelemetry instrumentation.
// Uses the global meter provider by default.
func NewServiceTelemetry() (*ServiceTelemetry, error) {
	return NewServiceTelemetryWithProvider(otel.GetMeterProvider())
}

// NewServiceTelemetryWithProvider creates a new ServiceTelemetry instance with a custom meter provider.
func NewServiceTelemetryWithProvider(mp metric.MeterProvider) (*ServiceTelemetry, error) {
	meter := mp.Meter(instrumentationName)

	t := &ServiceTelemetry{
		meter: meter,
	}

	var err error

	t.reorgsTotal, err = meter.Int64Counter(
		"chain.reorgs.total",
		metric.WithDescription("Total number of chain reorganizations detected"),
	)
	if err != nil {
		return nil, err
	}

	t.reorgsDroppedTotal, err = meter.Int64Counter(
		"chain.reorgs.dropped.total",
		metric.WithDescription("Total number of reorg signals dropped before state mutation, labelled by reason (stale_fork, verify_error, state_shifted)"),
	)
	if err != nil {
		return nil, err
	}

	// backfill.gap_fill.no_canonical.total fires once per per-block gap-fill
	// cycle that completed without producing a non-orphaned canonical row.
	// Steady-state expectation is zero; any non-zero rate signals the silent
	// failure mode behind the 2026-06-02 arbitrum backfill incident.
	t.backfillGapNoCanonicalTotal, err = meter.Int64Counter(
		"backfill.gap_fill.no_canonical.total",
		metric.WithDescription("Total per-block gap-fill cycles that completed without a non-orphaned canonical row in block_states (silent-failure invariant)"),
	)
	if err != nil {
		return nil, err
	}

	// live.block.out_of_order.total counts blocks delivered with a number at or
	// below the current head (out-of-order / late upstream delivery), labelled
	// by outcome (late_arrival vs reorg). This is the direct trigger signal for
	// the 2026-06-02 arbitrum incident; steady state is ~zero.
	t.outOfOrderBlocks, err = meter.Int64Counter(
		"live.block.out_of_order.total",
		metric.WithDescription("Blocks received with number <= current head (out-of-order/late delivery), labelled by outcome"),
	)
	if err != nil {
		return nil, err
	}

	// backfill.watermark_lag is the highest known block minus the backfill
	// watermark. Sustained/growing lag is the symptom that went unseen for 26
	// days in the VEC-277 incident.
	t.backfillWatermarkLag, err = meter.Int64Gauge(
		"backfill.watermark_lag",
		metric.WithDescription("Highest known block number minus the backfill watermark (blocks behind)"),
	)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// RecordReorg records a chain reorganization event.
// depth is how many blocks were reorganized, fromBlock is the common ancestor,
// and toBlock is the new chain head after the reorg.
//
// Only reorg.depth is attached as a metric attribute: it is bounded by the
// finality window, so its cardinality is small and fixed. fromBlock/toBlock
// are intentionally NOT attached — raw block numbers are unbounded and would
// mint a new Prometheus series per reorg, ballooning storage. This is the same
// reason RecordReorgDropped omits the block number. The parameters remain on
// the signature for callers/logs and possible future use as span exemplars.
func (t *ServiceTelemetry) RecordReorg(ctx context.Context, depth int, fromBlock, toBlock int64) {
	_ = fromBlock
	_ = toBlock
	t.reorgsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("reorg.depth", depth),
	))
}

// RecordReorgDropped records a reorg signal that was rejected before any state
// mutation. reason is a stable label value (see outbound.ReorgDropReason*
// constants) so Grafana dashboards and alerts can filter by it. The block
// number is intentionally NOT included as a metric attribute — it would
// produce one Prometheus series per block and balloon storage.
func (t *ServiceTelemetry) RecordReorgDropped(ctx context.Context, reason string) {
	t.reorgsDroppedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("reorg.dropped_reason", reason),
	))
}

// RecordOutOfOrderBlock counts a block delivered with a number at or below the
// current head, labelled by outcome (outbound.OutOfOrderOutcome*). Low
// cardinality (one bounded label); per-chain attribution comes from
// service.name. A sustained nonzero rate is the direct fingerprint of
// out-of-order upstream delivery (the VEC-277 trigger).
func (t *ServiceTelemetry) RecordOutOfOrderBlock(ctx context.Context, outcome string) {
	t.outOfOrderBlocks.Add(ctx, 1, metric.WithAttributes(
		attribute.String("outcome", outcome),
	))
}

// RecordBackfillGapNoCanonical increments the silent-failure counter that
// catches gap-fill cycles which returned success but did not yield a
// non-orphaned canonical row. Per-chain attribution comes from the OTel
// resource attribute `service.name` (e.g. "arbitrum-watcher"), which is set
// per pod via the k8s downward API and is what the Vector alerts group by
// — see alerts/vector-watcher.yaml. The chain ID is intentionally NOT
// attached as a metric attribute: it would inflate cardinality without
// adding a label that any rule queries (PR #373 review Finding 2). The
// chainID parameter is kept on the port for future use (per-chain
// dashboards, exemplars, etc.) and to avoid a ripple-changing port edit.
func (t *ServiceTelemetry) RecordBackfillGapNoCanonical(ctx context.Context, chainID int64) {
	_ = chainID
	t.backfillGapNoCanonicalTotal.Add(ctx, 1)
}

// RecordWatermarkLag records the current backfill lag (highest known block
// minus the watermark) as a gauge. Per-chain attribution comes from
// service.name; no per-block attribute, so cardinality stays flat.
func (t *ServiceTelemetry) RecordWatermarkLag(ctx context.Context, lag int64) {
	t.backfillWatermarkLag.Record(ctx, lag)
}
