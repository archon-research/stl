package position_daily_crystallizer

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Telemetry holds the instruments this service records. A nil *Telemetry is usable:
// every method is a no-op on it, so a caller that could not build one still runs.
type Telemetry struct {
	rowsWritten metric.Int64Counter
	runs        metric.Int64Counter
	failures    metric.Int64Counter
	duration    metric.Float64Histogram
}

const instrumentationName = "position_daily_crystallizer"

// NewTelemetry creates a Telemetry using the global meter provider.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider())
}

// NewTelemetryWithProvider creates a Telemetry with a custom meter provider.
func NewTelemetryWithProvider(mp metric.MeterProvider) (*Telemetry, error) {
	meter := mp.Meter(instrumentationName)

	rowsWritten, err := meter.Int64Counter("position_daily.rows_written",
		metric.WithDescription("Rows written into position_daily_observation. Zero per run is the steady state."))
	if err != nil {
		return nil, fmt.Errorf("creating rows_written counter: %w", err)
	}
	runs, err := meter.Int64Counter("position_daily.runs",
		metric.WithDescription("Completed crystallization passes."))
	if err != nil {
		return nil, fmt.Errorf("creating runs counter: %w", err)
	}
	failures, err := meter.Int64Counter("position_daily.failures",
		metric.WithDescription("Crystallization passes that returned an error."))
	if err != nil {
		return nil, fmt.Errorf("creating failures counter: %w", err)
	}
	duration, err := meter.Float64Histogram("position_daily.run_duration_seconds",
		metric.WithDescription("Wall time of one crystallization pass."))
	if err != nil {
		return nil, fmt.Errorf("creating run_duration histogram: %w", err)
	}

	return &Telemetry{rowsWritten: rowsWritten, runs: runs, failures: failures, duration: duration}, nil
}

// RecordRun records one successful pass.
func (t *Telemetry) RecordRun(ctx context.Context, rows int64, elapsed time.Duration) {
	if t == nil {
		return
	}
	t.runs.Add(ctx, 1)
	t.rowsWritten.Add(ctx, rows)
	t.duration.Record(ctx, elapsed.Seconds())
}

// RecordFailure records one failed pass.
func (t *Telemetry) RecordFailure(ctx context.Context) {
	if t == nil {
		return
	}
	t.failures.Add(ctx, 1)
}
