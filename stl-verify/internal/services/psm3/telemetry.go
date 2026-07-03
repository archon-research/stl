package psm3

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/psm3"

// Scaling factors that turn raw on-chain integers into human-readable units for
// the dashboard gauges. float64 loses precision past ~15 significant digits, but
// these gauges are for plotting magnitude/trend, not accounting — the
// append-only psm3_reserves rows remain the exact source of truth.
var (
	weiPerToken = new(big.Float).SetFloat64(1e18) // USDS/sUSDS/totalAssets are 1e18
	ray         = new(big.Float).SetFloat64(1e27) // conversion rate is 1e27 (ray)
)

// Telemetry provides OpenTelemetry metrics for the PSM3 indexer.
type Telemetry struct {
	sweeps         metric.Int64Counter
	sweepDuration  metric.Float64Histogram
	lastBlock      metric.Int64Gauge
	totalAssets    metric.Float64Gauge
	conversionRate metric.Float64Gauge

	// chainAttr is fixed at construction: one indexer process serves one chain.
	// It surfaces as the `chain` Prometheus label the Vector alerts group by;
	// without it those alerts render an empty chain.
	chainAttr attribute.KeyValue
}

// NewTelemetry creates a Telemetry using the global meter provider. chain is the
// chain name (e.g. "base") attached as the `chain` label.
func NewTelemetry(chain string) (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider(), chain)
}

// NewTelemetryWithProvider creates a Telemetry with a custom meter provider.
func NewTelemetryWithProvider(mp metric.MeterProvider, chain string) (*Telemetry, error) {
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{chainAttr: attribute.String("chain", chain)}

	var err error
	if t.sweeps, err = meter.Int64Counter(
		"psm3.sweeps.total",
		metric.WithDescription("Total PSM3 reserve sweeps by status (success|error)"),
	); err != nil {
		return nil, fmt.Errorf("creating sweeps counter: %w", err)
	}

	if t.sweepDuration, err = meter.Float64Histogram(
		"psm3.sweep.duration_seconds",
		metric.WithDescription("Duration of a PSM3 sweep (two multicall rounds + DB write)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	); err != nil {
		return nil, fmt.Errorf("creating sweepDuration histogram: %w", err)
	}

	if t.lastBlock, err = meter.Int64Gauge(
		"psm3.last_snapshot_block",
		metric.WithDescription("Block number of the most recent PSM3 snapshot written"),
	); err != nil {
		return nil, fmt.Errorf("creating lastBlock gauge: %w", err)
	}

	if t.totalAssets, err = meter.Float64Gauge(
		"psm3.total_assets",
		metric.WithDescription("PSM3 totalAssets from the latest snapshot, scaled to whole tokens"),
	); err != nil {
		return nil, fmt.Errorf("creating totalAssets gauge: %w", err)
	}

	if t.conversionRate, err = meter.Float64Gauge(
		"psm3.conversion_rate",
		metric.WithDescription("sUSDS conversion rate from the latest snapshot (ray-scaled to a ratio)"),
	); err != nil {
		return nil, fmt.Errorf("creating conversionRate gauge: %w", err)
	}

	return t, nil
}

// RecordSweep counts one sweep (status from err) and records its duration. The
// duration is recorded for both outcomes; an erroring sweep still measures how
// long the failing call took.
func (t *Telemetry) RecordSweep(ctx context.Context, duration time.Duration, err error) {
	if t == nil {
		return
	}
	t.sweeps.Add(ctx, 1, metric.WithAttributes(t.chainAttr, telemetry.StatusAttr(err)))
	t.sweepDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(t.chainAttr))
}

// RecordSnapshot sets the dashboard gauges from a successfully written snapshot.
func (t *Telemetry) RecordSnapshot(ctx context.Context, block int64, state entity.PSM3State) {
	if t == nil {
		return
	}
	attrs := metric.WithAttributes(t.chainAttr)
	t.lastBlock.Record(ctx, block, attrs)
	t.totalAssets.Record(ctx, scale(state.TotalAssets, weiPerToken), attrs)
	t.conversionRate.Record(ctx, scale(state.ConversionRate, ray), attrs)
}

func scale(v *big.Int, divisor *big.Float) float64 {
	f, _ := new(big.Float).Quo(new(big.Float).SetInt(v), divisor).Float64()
	return f
}
