package reference_capital_indexer

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/reference_capital_indexer"

// Telemetry records what a cycle wrote, which the shared cronjob outcome
// counter cannot express.
//
// A cycle that persists nothing, or that quietly stops covering one of the
// primes STL tracks, still reports success — and because the read path
// gap-fills with locf, the stalled series keeps serving its last value as
// current. These two counters are what make that visible.
//
// All methods are nil-receiver-safe so the service runs without telemetry
// wired (tests, local runs).
type Telemetry struct {
	meter metric.Meter

	snapshotsWritten metric.Int64Counter
	primesUncovered  metric.Int64Counter
}

// NewTelemetry creates a Telemetry instance using the global meter provider.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider())
}

// NewTelemetryWithProvider creates a Telemetry instance with a custom provider.
func NewTelemetryWithProvider(mp metric.MeterProvider) (*Telemetry, error) {
	t := &Telemetry{meter: mp.Meter(instrumentationName)}

	var err error
	t.snapshotsWritten, err = t.meter.Int64Counter(
		"reference_capital.sync.snapshots.written.total",
		metric.WithDescription("Reference capital snapshots persisted, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating snapshotsWritten counter: %w", err)
	}

	t.primesUncovered, err = t.meter.Int64Counter(
		"reference_capital.sync.primes.uncovered.total",
		metric.WithDescription("Tracked primes the upstream monitor did not cover, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating primesUncovered counter: %w", err)
	}

	return t, nil
}

// RecordSnapshotsWritten records how many snapshots a cycle persisted.
func (t *Telemetry) RecordSnapshotsWritten(ctx context.Context, count int) {
	if t == nil || t.snapshotsWritten == nil {
		return
	}
	t.snapshotsWritten.Add(ctx, int64(count))
}

// RecordPrimeUncovered records one tracked prime the monitor did not cover.
func (t *Telemetry) RecordPrimeUncovered(ctx context.Context, star string) {
	if t == nil || t.primesUncovered == nil {
		return
	}
	t.primesUncovered.Add(ctx, 1, metric.WithAttributes(attribute.String("star", star)))
}
