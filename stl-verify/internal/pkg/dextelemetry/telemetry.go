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
)

// Telemetry emits per-worker block/error counters plus a block-duration
// histogram. The receiver is nil-safe: every method becomes a no-op when
// called on a nil pointer, so production code can pass nil for "telemetry
// disabled" without guard checks at each call site.
type Telemetry struct {
	prefix          string
	blocksProcessed metric.Int64Counter
	errorsTotal     metric.Int64Counter
	blockDuration   metric.Float64Histogram
}

// NewTelemetry registers the two counters under `<prefix>.blocks.processed`
// and `<prefix>.errors.total`. The OTel-to-Prometheus exporter normalises the
// dots to underscores and adds the `_total` suffix, yielding the metric
// series names the alert rules expect.
func NewTelemetry(prefix string) (*Telemetry, error) {
	if prefix == "" {
		return nil, fmt.Errorf("dextelemetry.NewTelemetry: prefix must be non-empty")
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
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.block.duration_seconds histogram: %w", prefix, err)
	}

	return &Telemetry{
		prefix:          prefix,
		blocksProcessed: blocks,
		errorsTotal:     errs,
		blockDuration:   dur,
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
	attrs := metric.WithAttributes(attribute.String("status", status))
	t.blocksProcessed.Add(ctx, 1, attrs)
	t.blockDuration.Record(ctx, dur.Seconds(), attrs)
}

// RecordError increments errors_total with the operation label. Nil error or
// nil receiver are no-ops.
func (t *Telemetry) RecordError(ctx context.Context, operation string, err error) {
	if t == nil || err == nil {
		return
	}
	t.errorsTotal.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", operation)))
}
