package db_statement_stats

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/db_statement_stats"

// Telemetry provides the OpenTelemetry counters that carry per-table INSERT write
// cost out of the database's statement statistics.
//
// All three are monotonic counters fed with per-tick deltas, so a dashboard reads
// them with rate() and mean cost is rate(exec_time) / rate(calls).
type Telemetry struct {
	calls    metric.Int64Counter
	execTime metric.Float64Counter
	rows     metric.Int64Counter
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
	if t.calls, err = meter.Int64Counter(
		"db.statements.insert.calls.total",
		metric.WithDescription("INSERT statement executions, by target table"),
	); err != nil {
		return nil, fmt.Errorf("creating calls counter: %w", err)
	}
	if t.execTime, err = meter.Float64Counter(
		"db.statements.insert.exec_time_seconds.total",
		metric.WithUnit("s"),
		metric.WithDescription("Server-side execution time of INSERT statements, by target table"),
	); err != nil {
		return nil, fmt.Errorf("creating execTime counter: %w", err)
	}
	if t.rows, err = meter.Int64Counter(
		"db.statements.insert.rows.total",
		metric.WithDescription("Rows written by INSERT statements, by target table"),
	); err != nil {
		return nil, fmt.Errorf("creating rows counter: %w", err)
	}
	return t, nil
}

// RecordInsertDelta adds one table's increment since the previous tick. A
// zero-valued delta is still recorded: it keeps a quiet table's series alive so
// rate() stays defined instead of the table vanishing from the dashboard.
// Nil-safe so unit tests may pass a nil Telemetry.
func (t *Telemetry) RecordInsertDelta(ctx context.Context, table string, delta StatementDelta) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("table", table))
	t.calls.Add(ctx, delta.Calls, attrs)
	t.execTime.Add(ctx, delta.ExecTimeSeconds, attrs)
	t.rows.Add(ctx, delta.Rows, attrs)
}
