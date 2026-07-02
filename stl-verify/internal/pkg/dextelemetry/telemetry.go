// Package dextelemetry exposes a small per-worker OpenTelemetry helper for
// the three DEX SQS workers (curve, uniswap-v3, balancer). The structure
// mirrors the per-package telemetry in services/morpho_indexer and
// services/oracle_price_worker but accepts a prefix so the three workers can
// share one implementation instead of duplicating it. Metric names follow the
// established `<prefix>_blocks_processed_total` / `<prefix>_errors_total`
// convention so the existing alert rule shape in alerts/vector-indexers.yaml
// applies unchanged.
package dextelemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
)

// Telemetry emits per-worker block/error counters plus a block-duration
// histogram. Every datapoint is tagged with the worker's chain NAME via the
// `chain` attribute (the same entity.ChainName value morpho/oracle emit) so
// shared dashboards and alerts can `sum by (chain)` across all indexers without
// the value spaces fragmenting. The receiver is nil-safe: every method becomes a
// no-op when called on a nil pointer, so production code can pass nil for
// "telemetry disabled" without guard checks at each call site.
type Telemetry struct {
	prefix           string
	chainAttr        attribute.KeyValue
	blocksProcessed  metric.Int64Counter
	errorsTotal      metric.Int64Counter
	blockDuration    metric.Float64Histogram
	stateRowsWritten metric.Int64Counter
}

// NewTelemetry registers three counters (`<prefix>.blocks.processed`,
// `<prefix>.errors.total`, `<prefix>.state.rows.written`) plus the `<prefix>.block.duration_seconds`
// histogram. The OTel-to-Prometheus exporter normalises the dots to
// underscores and adds the `_total` suffix, yielding the metric series names
// the alert rules expect. The chain NAME (via entity.ChainName) is baked into
// every datapoint as the `chain` attribute so multi-chain dashboards line up
// with the morpho/oracle indexers, which label the same way. An unknown chainID
// is rejected so a worker fails hard at startup rather than emitting an empty or
// mismatched `chain` label.
func NewTelemetry(prefix string, chainID int64) (*Telemetry, error) {
	if prefix == "" {
		return nil, fmt.Errorf("dextelemetry.NewTelemetry: prefix must be non-empty")
	}
	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return nil, fmt.Errorf("dextelemetry.NewTelemetry: %w", err)
	}
	meter := otel.Meter(prefix + "-dex-worker")

	blocks, err := meter.Int64Counter(
		prefix+".blocks.processed",
		metric.WithDescription("Total number of blocks processed"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.blocks.processed counter: %w", prefix, err)
	}

	errs, err := meter.Int64Counter(
		prefix+".errors.total",
		metric.WithDescription("Total number of block-processing errors"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.errors.total counter: %w", prefix, err)
	}

	dur, err := meter.Float64Histogram(
		prefix+".block.duration_seconds",
		metric.WithDescription("Wall-clock duration of processBlockEvent in seconds"),
		metric.WithUnit("s"),
		// Declare seconds-scale buckets on the instrument (matching morpho/oracle)
		// rather than relying on the global view: OTel's default millisecond-scale
		// buckets would pin histogram_quantile near the top bucket for sub-second
		// durations.
		metric.WithExplicitBucketBoundaries(telemetry.SecondsDurationBuckets...),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.block.duration_seconds histogram: %w", prefix, err)
	}

	stateRows, err := meter.Int64Counter(
		prefix+".state.rows.written",
		metric.WithDescription("Total state snapshot rows written"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.state.rows.written counter: %w", prefix, err)
	}

	return &Telemetry{
		prefix:           prefix,
		chainAttr:        attribute.String("chain", chainName),
		blocksProcessed:  blocks,
		errorsTotal:      errs,
		blockDuration:    dur,
		stateRowsWritten: stateRows,
	}, nil
}

// RecordBlockProcessed increments blocks_processed_total with
// `status="success"` or `status="error"` based on err, and records the
// wall-clock duration into <prefix>.block.duration_seconds. The histogram
// enables the RPCLatencyHigh-style alert class for DEX workers (the comment
// in alerts/vector-indexers.yaml previously noted "no histogram emitted yet";
// once an alert is desired, mirror the morpho-indexer p99 rule shape).
func (t *Telemetry) RecordBlockProcessed(ctx context.Context, dur time.Duration, err error) {
	if t == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	attrs := metric.WithAttributes(attribute.String("status", status), t.chainAttr)
	t.blocksProcessed.Add(ctx, 1, attrs)
	t.blockDuration.Record(ctx, dur.Seconds(), attrs)
}

// RecordError increments errors_total with the operation label and chain
// attribute. Nil error or nil receiver are no-ops.
func (t *Telemetry) RecordError(ctx context.Context, operation string, err error) {
	if t == nil || err == nil {
		return
	}
	t.errorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("operation", operation),
		t.chainAttr,
	))
}

// RecordStateRows increments state_rows_written_total by n. Nil receiver or
// n <= 0 are no-ops.
func (t *Telemetry) RecordStateRows(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.stateRowsWritten.Add(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}
