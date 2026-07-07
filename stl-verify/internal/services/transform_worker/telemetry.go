package transform_worker

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/transform_worker"

// Telemetry provides OpenTelemetry metrics for the transform worker.
type Telemetry struct {
	tableRuns    metric.Int64Counter
	rowsUpserted metric.Int64Counter
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
	if t.tableRuns, err = meter.Int64Counter(
		"transform.table_runs.total",
		metric.WithDescription("Transform run-function invocations, by table and status"),
	); err != nil {
		return nil, fmt.Errorf("creating tableRuns counter: %w", err)
	}
	if t.rowsUpserted, err = meter.Int64Counter(
		"transform.rows_upserted.total",
		metric.WithDescription("Rows upserted into transformed tables, by table"),
	); err != nil {
		return nil, fmt.Errorf("creating rowsUpserted counter: %w", err)
	}
	return t, nil
}

// RecordTableSuccess counts one successful run and the rows it upserted.
// Nil-safe so unit tests may pass a nil Telemetry.
func (t *Telemetry) RecordTableSuccess(ctx context.Context, source string, rows int64) {
	if t == nil {
		return
	}
	table := attribute.String("table", source)
	t.tableRuns.Add(ctx, 1, metric.WithAttributes(table, attribute.String("status", "success")))
	t.rowsUpserted.Add(ctx, rows, metric.WithAttributes(table))
}

// RecordTableFailure counts one failed run. Nil-safe.
func (t *Telemetry) RecordTableFailure(ctx context.Context, source string) {
	if t == nil {
		return
	}
	t.tableRuns.Add(ctx, 1, metric.WithAttributes(
		attribute.String("table", source),
		attribute.String("status", "failure"),
	))
}
