package maple_graphql_indexer

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/maple_graphql_indexer"

// Telemetry provides OpenTelemetry metrics and tracing for the Maple GraphQL
// indexer. All methods are nil-receiver-safe so the service can run without
// telemetry wired (tests, local runs).
type Telemetry struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Counters
	cyclesTotal    metric.Int64Counter
	phasesTotal    metric.Int64Counter
	rowsWritten    metric.Int64Counter
	nullDowngrades metric.Int64Counter

	// Histograms
	phaseDuration metric.Float64Histogram

	// chainAttr is the constant per-chain attribute attached to every metric.
	// The Maple GraphQL API is mainnet-scoped, so the value is fixed.
	chainAttr attribute.KeyValue
}

// NewTelemetry creates a new Telemetry instance using the global providers.
func NewTelemetry() (*Telemetry, error) {
	return NewTelemetryWithProviders(otel.GetTracerProvider(), otel.GetMeterProvider())
}

// NewTelemetryWithProviders creates a new Telemetry instance with custom providers.
func NewTelemetryWithProviders(tp trace.TracerProvider, mp metric.MeterProvider) (*Telemetry, error) {
	t := &Telemetry{
		tracer:    tp.Tracer(instrumentationName),
		meter:     mp.Meter(instrumentationName),
		chainAttr: attribute.String("chain", "ethereum"),
	}

	var err error

	t.cyclesTotal, err = t.meter.Int64Counter(
		"maple.sync.cycles.total",
		metric.WithDescription("Total number of Maple GraphQL sync cycles"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating cyclesTotal counter: %w", err)
	}

	t.phasesTotal, err = t.meter.Int64Counter(
		"maple.sync.phases.total",
		metric.WithDescription("Total number of Maple GraphQL sync phases"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating phasesTotal counter: %w", err)
	}

	t.rowsWritten, err = t.meter.Int64Counter(
		"maple.sync.rows.written",
		metric.WithDescription("Total number of snapshot rows attempted per table (ON CONFLICT dedup on same-build retries may insert fewer)"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating rowsWritten counter: %w", err)
	}

	t.nullDowngrades, err = t.meter.Int64Counter(
		"maple.sync.null_downgrades.total",
		metric.WithDescription("API values that were null and persisted as SQL NULL, by field; a sustained jump means a Maple API regression"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating nullDowngrades counter: %w", err)
	}

	t.phaseDuration, err = t.meter.Float64Histogram(
		"maple.sync.phase.duration_seconds",
		metric.WithDescription("Duration of Maple GraphQL sync phases in seconds"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating phaseDuration histogram: %w", err)
	}

	return t, nil
}

// StartCycleSpan starts the top-level span for one sync cycle.
func (t *Telemetry) StartCycleSpan(ctx context.Context, syncedAt time.Time) (context.Context, trace.Span) {
	if t == nil {
		return ctx, telemetry.NoopSpan()
	}
	return t.tracer.Start(ctx, "maple.sync",
		trace.WithAttributes(attribute.String("synced_at", syncedAt.Format(time.RFC3339))),
	)
}

// StartPhaseSpan starts a child span for one sync phase.
func (t *Telemetry) StartPhaseSpan(ctx context.Context, phase string) (context.Context, trace.Span) {
	if t == nil {
		return ctx, telemetry.NoopSpan()
	}
	return t.tracer.Start(ctx, "maple.sync."+phase,
		trace.WithAttributes(attribute.String("phase", phase)),
	)
}

// RecordCycle records the outcome of one sync cycle.
func (t *Telemetry) RecordCycle(ctx context.Context, err error) {
	if t == nil {
		return
	}
	t.cyclesTotal.Add(ctx, 1, metric.WithAttributes(t.chainAttr, telemetry.StatusAttr(err)))
}

// RecordPhase records the outcome and duration of one sync phase.
func (t *Telemetry) RecordPhase(ctx context.Context, phase string, duration time.Duration, err error) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(t.chainAttr, attribute.String("phase", phase), telemetry.StatusAttr(err))
	t.phasesTotal.Add(ctx, 1, attrs)
	t.phaseDuration.Record(ctx, duration.Seconds(), attrs)
}

// RecordRowsWritten records the number of snapshot rows attempted for a
// table. ON CONFLICT DO NOTHING may insert fewer when a same-build retry
// re-sends rows that already exist.
func (t *Telemetry) RecordRowsWritten(ctx context.Context, table string, count int) {
	if t == nil {
		return
	}
	t.rowsWritten.Add(ctx, int64(count), metric.WithAttributes(
		t.chainAttr,
		attribute.String("table", table),
	))
}

// RecordNullDowngrade counts an API value that was null and is persisted as
// SQL NULL instead of failing the phase.
func (t *Telemetry) RecordNullDowngrade(ctx context.Context, field string) {
	if t == nil {
		return
	}
	t.nullDowngrades.Add(ctx, 1, metric.WithAttributes(
		t.chainAttr,
		attribute.String("field", field),
	))
}

// RecordCollateralPriceNull is the collateral-price specialisation of
// RecordNullDowngrade. It tags the null with why the price is missing —
// reason="pending" (collateral still DepositPending) vs reason="unpriceable"
// (Maple's oracle layer had no feed for the asset) — plus the collateral token,
// so a chronic upstream pricing gap is separable from a normal pending null.
func (t *Telemetry) RecordCollateralPriceNull(ctx context.Context, reason, token string) {
	if t == nil {
		return
	}
	t.nullDowngrades.Add(ctx, 1, metric.WithAttributes(
		t.chainAttr,
		attribute.String("field", "collateral_asset_value_usd"),
		attribute.String("reason", reason),
		attribute.String("token", token),
	))
}
