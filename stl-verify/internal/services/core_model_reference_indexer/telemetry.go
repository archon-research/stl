package core_model_reference_indexer

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/core_model_reference_indexer"

// Telemetry records what a cycle wrote, what it dropped and how fresh it was,
// which the shared cronjob outcome counter cannot express.
//
// A cycle that persists nothing still reports success, and a cycle whose rows
// all carry an old model_date reports success too: upstream stopped running
// its model, and nothing on the error path notices. These counters are what an
// alert can key on. The written counters count rows the database inserted, not
// rows submitted, so a cycle whose rows all conflict away reads as zero.
// staleRows is a dashboard figure — a few small markets always lag — while
// staleCycles is the alertable one: it moves only when the freshest row of a
// whole cycle is behind. rowsRejected counts upstream rows dropped for failing
// validation, and unmappedNetworkRows the rows stored with a NULL chain id
// because the network map has no entry for them; both are silent otherwise.
//
// Every counter is seeded at 0 on construction so its series exists from
// process start; an `unless`/`== 0` rule cannot fire on a series that was
// never emitted (alerts/AGENTS.md).
//
// All methods are nil-receiver-safe so the service runs without telemetry
// wired (tests, local runs).
type Telemetry struct {
	meter metric.Meter

	marketsWritten      metric.Int64Counter
	vaultsWritten       metric.Int64Counter
	staleRows           metric.Int64Counter
	staleCycles         metric.Int64Counter
	rowsRejected        metric.Int64Counter
	unmappedNetworkRows metric.Int64Counter
}

// NewTelemetry creates a Telemetry instance using the global meter provider.
func NewTelemetry(ctx context.Context) (*Telemetry, error) {
	return NewTelemetryWithProvider(ctx, otel.GetMeterProvider())
}

// NewTelemetryWithProvider creates a Telemetry instance with a custom provider.
func NewTelemetryWithProvider(ctx context.Context, mp metric.MeterProvider) (*Telemetry, error) {
	t := &Telemetry{meter: mp.Meter(instrumentationName)}

	counters := []struct {
		target      *metric.Int64Counter
		name        string
		description string
	}{
		{&t.marketsWritten, "core_model_reference.sync.markets.written.total",
			"CORE market result rows the database inserted, per cycle"},
		{&t.vaultsWritten, "core_model_reference.sync.vaults.written.total",
			"CORE vault result rows the database inserted, per cycle"},
		{&t.staleRows, "core_model_reference.sync.stale_rows.total",
			"Persisted rows whose upstream model_date lagged the cycle by more than the freshness allowance"},
		{&t.staleCycles, "core_model_reference.sync.stale_cycles.total",
			"Cycles whose freshest row still lagged the cycle by more than the freshness allowance: upstream has not published a new day"},
		{&t.rowsRejected, "core_model_reference.sync.rows.rejected.total",
			"Upstream rows dropped from a cycle for failing validation (missing field, out-of-range figure, figure forbidden by the row's method)"},
		{&t.unmappedNetworkRows, "core_model_reference.sync.unmapped_network_rows.total",
			"Rows persisted with a NULL chain_id because the network map has no entry for their network"},
	}
	for _, c := range counters {
		counter, err := t.meter.Int64Counter(c.name, metric.WithDescription(c.description))
		if err != nil {
			return nil, fmt.Errorf("creating %s counter: %w", c.name, err)
		}
		*c.target = counter
		telemetry.SeedCounter(ctx, counter)
	}
	return t, nil
}

// RecordMarketsWritten records how many market rows a cycle inserted.
func (t *Telemetry) RecordMarketsWritten(ctx context.Context, count int) {
	if t == nil || t.marketsWritten == nil {
		return
	}
	t.marketsWritten.Add(ctx, int64(count))
}

// RecordVaultsWritten records how many vault rows a cycle inserted.
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

// RecordRowsRejected records how many upstream rows a cycle dropped.
func (t *Telemetry) RecordRowsRejected(ctx context.Context, count int) {
	if t == nil || t.rowsRejected == nil {
		return
	}
	t.rowsRejected.Add(ctx, int64(count))
}

// RecordUnmappedNetworkRows records rows persisted with a NULL chain id for one
// network the map does not know.
func (t *Telemetry) RecordUnmappedNetworkRows(ctx context.Context, network string, count int) {
	if t == nil || t.unmappedNetworkRows == nil {
		return
	}
	t.unmappedNetworkRows.Add(ctx, int64(count), metric.WithAttributes(attribute.String("network", network)))
}
