package reference_core_indexer

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/reference_core_indexer"

// Telemetry records what a cycle wrote and how fresh it was, which the shared
// cronjob outcome counter cannot express.
//
// A cycle that persists nothing still reports success, and a cycle whose rows
// all carry an old model_date reports success too: upstream stopped running
// its model, and nothing on the error path notices. These counters are what an
// alert can key on. staleRows is a dashboard figure — a few small markets
// always lag — while staleCycles is the alertable one: it moves only when the
// freshest row of a whole cycle is behind.
//
// Every counter is seeded at 0 on construction so its series exists from
// process start; an `unless`/`== 0` rule cannot fire on a series that was
// never emitted (alerts/AGENTS.md).
//
// All methods are nil-receiver-safe so the service runs without telemetry
// wired (tests, local runs).
type Telemetry struct {
	meter metric.Meter

	marketsWritten metric.Int64Counter
	vaultsWritten  metric.Int64Counter
	staleRows      metric.Int64Counter
	staleCycles    metric.Int64Counter
}

// NewTelemetry creates a Telemetry instance using the global meter provider.
func NewTelemetry(ctx context.Context) (*Telemetry, error) {
	return NewTelemetryWithProvider(ctx, otel.GetMeterProvider())
}

// NewTelemetryWithProvider creates a Telemetry instance with a custom provider.
func NewTelemetryWithProvider(ctx context.Context, mp metric.MeterProvider) (*Telemetry, error) {
	t := &Telemetry{meter: mp.Meter(instrumentationName)}

	var err error
	t.marketsWritten, err = t.meter.Int64Counter(
		"reference_core.sync.markets.written.total",
		metric.WithDescription("CORE market result rows persisted, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating marketsWritten counter: %w", err)
	}

	t.vaultsWritten, err = t.meter.Int64Counter(
		"reference_core.sync.vaults.written.total",
		metric.WithDescription("CORE vault result rows persisted, per cycle"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating vaultsWritten counter: %w", err)
	}

	t.staleRows, err = t.meter.Int64Counter(
		"reference_core.sync.stale_rows.total",
		metric.WithDescription("Persisted rows whose upstream model_date lagged the cycle by more than the freshness allowance"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating staleRows counter: %w", err)
	}

	t.staleCycles, err = t.meter.Int64Counter(
		"reference_core.sync.stale_cycles.total",
		metric.WithDescription("Cycles whose freshest row still lagged the cycle by more than the freshness allowance: upstream has not published a new day"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating staleCycles counter: %w", err)
	}

	telemetry.SeedCounter(ctx, t.marketsWritten)
	telemetry.SeedCounter(ctx, t.vaultsWritten)
	telemetry.SeedCounter(ctx, t.staleRows)
	telemetry.SeedCounter(ctx, t.staleCycles)
	return t, nil
}

// RecordMarketsWritten records how many market rows a cycle persisted.
func (t *Telemetry) RecordMarketsWritten(ctx context.Context, count int) {
	if t == nil || t.marketsWritten == nil {
		return
	}
	t.marketsWritten.Add(ctx, int64(count))
}

// RecordVaultsWritten records how many vault rows a cycle persisted.
func (t *Telemetry) RecordVaultsWritten(ctx context.Context, count int) {
	if t == nil || t.vaultsWritten == nil {
		return
	}
	t.vaultsWritten.Add(ctx, int64(count))
}

// RecordStaleRows records how many of a cycle's rows reported a model_date
// older than the freshness allowance.
func (t *Telemetry) RecordStaleRows(ctx context.Context, count int) {
	if t == nil || t.staleRows == nil {
		return
	}
	t.staleRows.Add(ctx, int64(count))
}

// RecordStaleCycle records one cycle in which no row was fresh.
func (t *Telemetry) RecordStaleCycle(ctx context.Context) {
	if t == nil || t.staleCycles == nil {
		return
	}
	t.staleCycles.Add(ctx, 1)
}
