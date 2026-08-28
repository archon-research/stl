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

	snapshotsWritten            metric.Int64Counter
	allocationsWritten          metric.Int64Counter
	positionsWritten            metric.Int64Counter
	primesUncovered             metric.Int64Counter
	balanceSheetDaysInserted    metric.Int64Counter
	balanceSheetPrimesUncovered metric.Int64Counter
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

	t.allocationsWritten, err = t.meter.Int64Counter(
		"reference_capital.sync.allocations.written.total",
		metric.WithDescription("Per-allocation breakdown rows persisted, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating allocationsWritten counter: %w", err)
	}

	t.positionsWritten, err = t.meter.Int64Counter(
		"reference_capital.sync.positions.written.total",
		metric.WithDescription("Balance-sheet position rows persisted, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating positionsWritten counter: %w", err)
	}

	t.primesUncovered, err = t.meter.Int64Counter(
		"reference_capital.sync.primes.uncovered.total",
		metric.WithDescription("Tracked primes the upstream monitor did not cover, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating primesUncovered counter: %w", err)
	}

	t.balanceSheetDaysInserted, err = t.meter.Int64Counter(
		"reference_capital.sync.balance_sheet.days.inserted.total",
		metric.WithDescription("Balance-sheet days that started fresh (not a build correction), per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balanceSheetDaysInserted counter: %w", err)
	}

	t.balanceSheetPrimesUncovered, err = t.meter.Int64Counter(
		"reference_capital.sync.balance_sheet.primes.uncovered.total",
		metric.WithDescription("Tracked primes absent from the balance-sheet feed's fetch window, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating balanceSheetPrimesUncovered counter: %w", err)
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

// RecordAllocationsWritten records how many breakdown rows a cycle persisted.
func (t *Telemetry) RecordAllocationsWritten(ctx context.Context, count int) {
	if t == nil || t.allocationsWritten == nil {
		return
	}
	t.allocationsWritten.Add(ctx, int64(count))
}

// RecordPositionsWritten records how many position rows a cycle persisted.
func (t *Telemetry) RecordPositionsWritten(ctx context.Context, count int) {
	if t == nil || t.positionsWritten == nil {
		return
	}
	t.positionsWritten.Add(ctx, int64(count))
}

// RecordPrimeUncovered records one tracked prime the monitor did not cover.
func (t *Telemetry) RecordPrimeUncovered(ctx context.Context, star string) {
	if t == nil || t.primesUncovered == nil {
		return
	}
	t.primesUncovered.Add(ctx, 1, metric.WithAttributes(attribute.String("star", star)))
}

// RecordBalanceSheetDaysInserted records how many balance-sheet days a cycle
// started fresh, excluding rows that corrected an already-stored day.
func (t *Telemetry) RecordBalanceSheetDaysInserted(ctx context.Context, count int) {
	if t == nil || t.balanceSheetDaysInserted == nil {
		return
	}
	t.balanceSheetDaysInserted.Add(ctx, int64(count))
}

// RecordBalanceSheetPrimeUncovered records one tracked prime absent from the
// balance-sheet feed's fetch window this cycle.
func (t *Telemetry) RecordBalanceSheetPrimeUncovered(ctx context.Context, star string) {
	if t == nil || t.balanceSheetPrimesUncovered == nil {
		return
	}
	t.balanceSheetPrimesUncovered.Add(ctx, 1, metric.WithAttributes(attribute.String("star", star)))
}
