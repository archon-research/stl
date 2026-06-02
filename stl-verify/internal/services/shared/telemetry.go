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

	// Backfill invariant metrics
	backfillGapNoCanonicalTotal metric.Int64Counter
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

	return t, nil
}

// RecordReorg records a chain reorganization event.
// depth is how many blocks were reorganized, fromBlock is the common ancestor,
// and toBlock is the new chain head after the reorg.
func (t *ServiceTelemetry) RecordReorg(ctx context.Context, depth int, fromBlock, toBlock int64) {
	t.reorgsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("reorg.depth", depth),
		attribute.Int64("reorg.from_block", fromBlock),
		attribute.Int64("reorg.to_block", toBlock),
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
