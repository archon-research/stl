package position_materializer

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/position_materializer"

// Run statuses. canceled is a run cut short by its parent context (a rollout stopping the pod), not a
// broken view, so VectorPositionMaterializerViewFailing does not select it.
const (
	statusOK       = "ok"
	statusError    = "error"
	statusCanceled = "canceled"
)

// runStatuses is every status RecordRun is passed, and so every series the seed exports.
var runStatuses = []string{statusOK, statusError, statusCanceled}

// Telemetry provides OpenTelemetry metrics for the position materializer. A nil
// *Telemetry is valid: every method no-ops, so tests and callers without a meter
// provider need no stub.
type Telemetry struct {
	projectionRuns   metric.Int64Counter
	rowsChanged      metric.Int64Counter
	positionsRefused metric.Int64Gauge
	cacheRows        metric.Int64Gauge
}

// NewTelemetry creates a Telemetry using the global meter provider, seeding the counters of every
// configured materializer.
func NewTelemetry(materializers []string) (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider(), materializers)
}

// NewTelemetryWithProvider creates a Telemetry with a custom meter provider. The run and row counters
// are seeded at zero for each materializer: an alert reading increase() cannot see a series that
// first appears at its first value.
func NewTelemetryWithProvider(mp metric.MeterProvider, materializers []string) (*Telemetry, error) {
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{}
	var err error
	if t.projectionRuns, err = meter.Int64Counter(
		"position_materializer.projection_runs.total",
		metric.WithDescription("Projection materialization runs, by view and status"),
	); err != nil {
		return nil, fmt.Errorf("creating projectionRuns counter: %w", err)
	}
	if t.positionsRefused, err = meter.Int64Gauge(
		"position_materializer.positions_refused",
		metric.WithDescription("Positions the projection's latest run withheld or declined a correction for"),
	); err != nil {
		return nil, fmt.Errorf("building positionsRefused gauge: %w", err)
	}
	if t.cacheRows, err = meter.Int64Gauge(
		"position_materializer.cache_rows",
		metric.WithDescription("Estimated rows in each trigger-fed cache derived from position_state, by table"),
	); err != nil {
		return nil, fmt.Errorf("building cacheRows gauge: %w", err)
	}
	if t.rowsChanged, err = meter.Int64Counter(
		"position_materializer.rows_changed.total",
		metric.WithDescription("position_state rows appended per projection run (a rerun that finds nothing new records 0)"),
	); err != nil {
		return nil, fmt.Errorf("creating rowsChanged counter: %w", err)
	}
	ctx := context.Background()
	for _, m := range materializers {
		view := attribute.String("materializer", m)
		for _, status := range runStatuses {
			telemetry.SeedCounter(ctx, t.projectionRuns, view, attribute.String("status", status))
		}
		telemetry.SeedCounter(ctx, t.rowsChanged, view)
	}
	return t, nil
}

// RecordRun records one projection run and its changed-row count.
func (t *Telemetry) RecordRun(ctx context.Context, view, status string, changed int64) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(
		attribute.String("materializer", view),
		attribute.String("status", status),
	)
	t.projectionRuns.Add(ctx, 1, attrs)
	// Recorded even at zero: the series has to exist for a run that appended nothing,
	// which is the case VectorPositionMaterializerSilentlyEmpty exists to catch.
	t.rowsChanged.Add(ctx, changed, metric.WithAttributes(attribute.String("materializer", view)))
}

// RecordCacheRows publishes one cache's estimated row count. A gauge, not a counter: these caches
// are written by database triggers, so no process here can count the rows they persisted the way an
// indexer counts its own writes. The level is what the plain-table tripwire compares to its budget.
func (t *Telemetry) RecordCacheRows(ctx context.Context, table string, rows int64) {
	if t == nil {
		return
	}
	t.cacheRows.Record(ctx, rows, metric.WithAttributes(attribute.String("table", table)))
}

// RecordRefused publishes how many positions a projection's latest run withheld. A gauge, not a
// counter: the question is how many are withheld now, and a projection that keeps refusing the
// same position reports the same level every tick, which is what a sustained alert reads.
func (t *Telemetry) RecordRefused(ctx context.Context, projection string, refused int64) {
	if t == nil {
		return
	}
	t.positionsRefused.Record(ctx, refused,
		metric.WithAttributes(attribute.String("projection", projection)))
}
