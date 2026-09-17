package position_materializer

import (
	"context"
	"fmt"
	"maps"
	"sync"

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

// Reads recorded by read_failures.total, seeded so its first failure is an increase.
const (
	readRefused   = "refused"
	readCacheRows = "cache_rows"
)

var reads = []string{readRefused, readCacheRows}

// Telemetry provides OpenTelemetry metrics for the position materializer. A nil
// *Telemetry is valid: every method no-ops, so tests and callers without a meter
// provider need no stub.
type Telemetry struct {
	projectionRuns metric.Int64Counter
	rowsChanged    metric.Int64Counter
	readFailures   metric.Int64Counter

	// The gauges export only what the latest tick set: an observable gauge drops a series its
	// callback stops observing, so a projection absent from the latest read goes absent.
	mu        sync.Mutex
	refused   map[string]int64
	cacheRows map[string]int64
}

// NewTelemetry creates a Telemetry using the global meter provider, seeding the counters of every
// configured materializer.
func NewTelemetry(materializers []string) (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider(), materializers)
}

// NewTelemetryWithProvider creates a Telemetry with a custom meter provider. The counters are seeded
// at zero for each materializer and read: an alert reading increase() cannot see a series that first
// appears at its first value.
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
	if t.rowsChanged, err = meter.Int64Counter(
		"position_materializer.rows_changed.total",
		metric.WithDescription("position_state rows appended per projection run (a rerun that finds nothing new records 0)"),
	); err != nil {
		return nil, fmt.Errorf("creating rowsChanged counter: %w", err)
	}
	if t.readFailures, err = meter.Int64Counter(
		"position_materializer.read_failures.total",
		metric.WithDescription("Failed reads of the withheld level or the cache sizes; the gauge goes absent until one succeeds"),
	); err != nil {
		return nil, fmt.Errorf("creating readFailures counter: %w", err)
	}
	if _, err = meter.Int64ObservableGauge(
		"position_materializer.positions_refused",
		metric.WithDescription("Positions the projection's run in the latest tick withheld or declined a correction for"),
		metric.WithInt64Callback(t.observe(&t.refused, "projection")),
	); err != nil {
		return nil, fmt.Errorf("building positionsRefused gauge: %w", err)
	}
	if _, err = meter.Int64ObservableGauge(
		"position_materializer.cache_rows",
		metric.WithDescription("Estimated rows in each trigger-fed cache derived from position_state, by table"),
		metric.WithInt64Callback(t.observe(&t.cacheRows, "table")),
	); err != nil {
		return nil, fmt.Errorf("building cacheRows gauge: %w", err)
	}
	t.seed(materializers)
	return t, nil
}

func (t *Telemetry) seed(materializers []string) {
	ctx := context.Background()
	for _, m := range materializers {
		view := attribute.String("materializer", m)
		for _, status := range runStatuses {
			telemetry.SeedCounter(ctx, t.projectionRuns, view, attribute.String("status", status))
		}
		telemetry.SeedCounter(ctx, t.rowsChanged, view)
	}
	for _, r := range reads {
		telemetry.SeedCounter(ctx, t.readFailures, attribute.String("read", r))
	}
}

func (t *Telemetry) observe(levels *map[string]int64, key string) metric.Int64Callback {
	return func(_ context.Context, o metric.Int64Observer) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		for name, v := range *levels {
			o.Observe(v, metric.WithAttributes(attribute.String(key, name)))
		}
		return nil
	}
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

// SetCacheRows replaces the exported cache sizes, keyed by table. The caches are written by database
// triggers, so their size is read as a level rather than counted as it is written.
func (t *Telemetry) SetCacheRows(rows map[string]int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cacheRows = maps.Clone(rows)
}

// SetRefused replaces the exported withheld levels, keyed by projection. A projection that keeps
// refusing the same position exports the same level every tick, which is what a sustained alert reads.
func (t *Telemetry) SetRefused(refused map[string]int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refused = maps.Clone(refused)
}

// RecordReadFailure counts a failed read of one gauge's source (readRefused or readCacheRows).
func (t *Telemetry) RecordReadFailure(ctx context.Context, read string) {
	if t == nil {
		return
	}
	t.readFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("read", read)))
}
