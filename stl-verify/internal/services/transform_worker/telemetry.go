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
	tableRuns       metric.Int64Counter
	rowsConsumed    metric.Int64Counter
	queuePending    metric.Int64Gauge
	queueOldestAge  metric.Float64Gauge
	parityDrift     metric.Int64Gauge
	parityRaw       metric.Int64Gauge
	parityTransform metric.Int64Gauge
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
	if t.rowsConsumed, err = meter.Int64Counter(
		"transform.queue.rows_consumed.total",
		metric.WithDescription("Queue rows consumed (drained) per table; the guard/DISTINCT make actual upserts <= this"),
	); err != nil {
		return nil, fmt.Errorf("creating rowsConsumed counter: %w", err)
	}
	if t.queuePending, err = meter.Int64Gauge(
		"transform.queue.pending",
		metric.WithDescription("Un-drained rows in each source's change queue"),
	); err != nil {
		return nil, fmt.Errorf("creating queuePending gauge: %w", err)
	}
	if t.queueOldestAge, err = meter.Float64Gauge(
		"transform.queue.oldest_age_seconds",
		metric.WithDescription("Age of the oldest un-drained queue row, by source; the stalled-transform signal"),
	); err != nil {
		return nil, fmt.Errorf("creating queueOldestAge gauge: %w", err)
	}
	if t.parityDrift, err = meter.Int64Gauge(
		"transform.parity.drift",
		metric.WithDescription("raw - transformed - pending row count, by source; nonzero = a silent parity gap"),
	); err != nil {
		return nil, fmt.Errorf("creating parityDrift gauge: %w", err)
	}
	if t.parityRaw, err = meter.Int64Gauge(
		"transform.parity.raw_rows",
		metric.WithDescription("Raw source row count, by source (parity context)"),
	); err != nil {
		return nil, fmt.Errorf("creating parityRaw gauge: %w", err)
	}
	if t.parityTransform, err = meter.Int64Gauge(
		"transform.parity.transformed_rows",
		metric.WithDescription("Transformed table row count, by source (parity context)"),
	); err != nil {
		return nil, fmt.Errorf("creating parityTransform gauge: %w", err)
	}
	return t, nil
}

// RecordTableSuccess counts one successful run and the queue rows it consumed
// (drained). Nil-safe so unit tests may pass a nil Telemetry.
func (t *Telemetry) RecordTableSuccess(ctx context.Context, source string, rowsConsumed int64) {
	if t == nil {
		return
	}
	table := attribute.String("table", source)
	t.tableRuns.Add(ctx, 1, metric.WithAttributes(table, attribute.String("status", "success")))
	t.rowsConsumed.Add(ctx, rowsConsumed, metric.WithAttributes(table))
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

// RecordQueueDepth records one source's change-queue backlog. Nil-safe.
func (t *Telemetry) RecordQueueDepth(ctx context.Context, source string, pending int64, oldestAgeSecs float64) {
	if t == nil {
		return
	}
	table := attribute.String("table", source)
	t.queuePending.Record(ctx, pending, metric.WithAttributes(table))
	t.queueOldestAge.Record(ctx, oldestAgeSecs, metric.WithAttributes(table))
}

// RecordParity records one source's raw-vs-transformed parity. Nil-safe.
func (t *Telemetry) RecordParity(ctx context.Context, source string, rawRows, transformedRows, drift int64) {
	if t == nil {
		return
	}
	table := attribute.String("table", source)
	t.parityDrift.Record(ctx, drift, metric.WithAttributes(table))
	t.parityRaw.Record(ctx, rawRows, metric.WithAttributes(table))
	t.parityTransform.Record(ctx, transformedRows, metric.WithAttributes(table))
}
