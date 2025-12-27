// Package shared provides shared utilities and instrumentation for application services.
package shared

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Compile-time assertion that AppTelemetry implements MetricsRecorder.
var _ outbound.MetricsRecorder = (*AppTelemetry)(nil)

const (
	// instrumentationName is the name used for OpenTelemetry instrumentation.
	instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services"
)

// AppTelemetry provides OpenTelemetry metrics for application-level domain events.
// This is separate from adapter-level telemetry (e.g., alchemy.Telemetry) which
// tracks infrastructure concerns like HTTP requests and WebSocket connections.
type AppTelemetry struct {
	meter metric.Meter

	// Chain metrics
	reorgsTotal metric.Int64Counter
}

// NewAppTelemetry creates a new AppTelemetry instance with OpenTelemetry instrumentation.
// Uses the global meter provider by default.
func NewAppTelemetry() (*AppTelemetry, error) {
	return NewAppTelemetryWithProvider(otel.GetMeterProvider())
}

// NewAppTelemetryWithProvider creates a new AppTelemetry instance with a custom meter provider.
func NewAppTelemetryWithProvider(mp metric.MeterProvider) (*AppTelemetry, error) {
	meter := mp.Meter(instrumentationName)

	t := &AppTelemetry{
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

	return t, nil
}

// RecordReorg records a chain reorganization event.
// depth is how many blocks were reorganized, fromBlock is the common ancestor,
// and toBlock is the new chain head after the reorg.
func (t *AppTelemetry) RecordReorg(ctx context.Context, depth int, fromBlock, toBlock int64) {
	t.reorgsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("reorg.depth", depth),
		attribute.Int64("reorg.from_block", fromBlock),
		attribute.Int64("reorg.to_block", toBlock),
	))
}
