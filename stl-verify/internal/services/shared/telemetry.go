// Package shared provides shared utilities and instrumentation for services.
package shared

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Compile-time assertion that ServiceTelemetry implements ReorgRecorder.
var _ outbound.ReorgRecorder = (*ServiceTelemetry)(nil)

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
