package position_materializer

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/position_materializer"

// Telemetry provides OpenTelemetry metrics for the position materializer. A nil
// *Telemetry is valid: every method no-ops, so tests and callers without a meter
// provider need no stub.
type Telemetry struct {
	projectionRuns metric.Int64Counter
	rowsChanged    metric.Int64Counter
}

// NewTelemetry creates a Telemetry using the global meter provider.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider())
}

// NewTelemetryWithProvider creates a Telemetry with a custom meter provider.
func NewTelemetryWithProvider(mp metric.MeterProvider) (*Telemetry, error) {
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{}
	var err error
	if t.projectionRuns, err = meter.Int64Counter(
		"position_materializer.projection_runs.total",
		metric.WithDescription("Projection materialization runs, by view and status"),
	); err != nil {
		return nil, fmt.Errorf("creating projectionRuns counter: %w", err)
	}
	if t.rowsChanged, err = meter.Int64Counter(
		"position_materializer.rows_changed.total",
		metric.WithDescription("position_state rows inserted or changed per projection run (guarded upsert; a no-op rerun records 0)"),
	); err != nil {
		return nil, fmt.Errorf("creating rowsChanged counter: %w", err)
	}
	return t, nil
}

// RecordRun records one projection run and its changed-row count.
func (t *Telemetry) RecordRun(ctx context.Context, view, status string, changed int64) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("view", view),
		attribute.String("status", status),
	)
	t.projectionRuns.Add(ctx, 1, attrs)
	if changed > 0 {
		t.rowsChanged.Add(ctx, changed, metric.WithAttributes(attribute.String("view", view)))
	}
}
