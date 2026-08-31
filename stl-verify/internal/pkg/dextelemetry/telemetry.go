// Package dextelemetry emits per-worker DEX metrics under a caller-supplied
// prefix, named for the rules in alerts/vector-indexers.yaml.
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
	prefix              string
	chainAttr           attribute.KeyValue
	blocksProcessed     metric.Int64Counter
	errorsTotal         metric.Int64Counter
	blockDuration       metric.Float64Histogram
	stateRowsWritten    metric.Int64Counter
	stateRowsAttempted  metric.Int64Counter
	poolsTouched        metric.Int64Counter
	tickRowsWritten     metric.Int64Counter
	positionRowsWritten metric.Int64Counter
	poolsNeverIndexed   metric.Int64Gauge
}

// NewTelemetry registers the whole instrument set for one DEX; the
// OTel-to-Prometheus exporter normalises the dots to underscores and adds
// `_total`, yielding the series names the alert rules select.
func NewTelemetry(prefix string, chainID int64) (*Telemetry, error) {
	if prefix == "" {
		return nil, fmt.Errorf("dextelemetry.NewTelemetry: prefix must be non-empty")
	}
	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return nil, fmt.Errorf("dextelemetry.NewTelemetry: %w", err)
	}
	meter := otel.Meter(prefix + "-dex-worker")

	counter := func(suffix, description string) (metric.Int64Counter, error) {
		c, err := meter.Int64Counter(prefix+suffix, metric.WithDescription(description))
		if err != nil {
			return nil, fmt.Errorf("creating %s%s counter: %w", prefix, suffix, err)
		}
		return c, nil
	}

	blocks, err := counter(".blocks.processed", "Total number of blocks processed")
	if err != nil {
		return nil, err
	}

	errs, err := counter(".errors.total", "Total number of block-processing errors")
	if err != nil {
		return nil, err
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

	stateRows, err := counter(".state.rows.written", "Total state snapshot rows written")
	if err != nil {
		return nil, err
	}

	stateRowsAttempted, err := counter(".state.rows.attempted", "Total state snapshot rows a block queued for insert, conflicts included")
	if err != nil {
		return nil, err
	}

	touched, err := counter(".pools.touched", "Total registered pools touched by decoded events")
	if err != nil {
		return nil, err
	}

	tickRows, err := counter(".tick.rows.written", "Total per-tick rows offered to the append-on-change writer")
	if err != nil {
		return nil, err
	}

	positionRows, err := counter(".position.rows.written", "Total per-position rows offered to the append-on-change writer")
	if err != nil {
		return nil, err
	}

	neverIndexed, err := meter.Int64Gauge(
		prefix+".pools.never_indexed",
		metric.WithDescription("Registered, snapshot-supported pools that have never produced a state or tick row"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.pools.never_indexed gauge: %w", prefix, err)
	}

	return &Telemetry{
		prefix:              prefix,
		chainAttr:           attribute.String("chain", chainName),
		blocksProcessed:     blocks,
		errorsTotal:         errs,
		blockDuration:       dur,
		stateRowsWritten:    stateRows,
		stateRowsAttempted:  stateRowsAttempted,
		poolsTouched:        touched,
		tickRowsWritten:     tickRows,
		positionRowsWritten: positionRows,
		poolsNeverIndexed:   neverIndexed,
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

// RecordStateRows counts rows a block actually appended; an idempotent replay
// legitimately appends none.
func (t *Telemetry) RecordStateRows(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.stateRowsWritten.Add(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}

// RecordStateRowsAttempted counts rows queued for insert, conflicts included:
// a replay reusing one processing_version writes nothing while healthy, so the
// not-writing-state alerts key on attempted, never on written.
func (t *Telemetry) RecordStateRowsAttempted(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.stateRowsAttempted.Add(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}

// Record n from the receipts' touched-pool set, never from DueSet: an always-empty
// DueSet is the bug the sweepless silent-empty alerts catch, and they fire on this
// series being absent, which a nil receiver or n <= 0 leaves it.
func (t *Telemetry) RecordPoolsTouched(ctx context.Context, n int, attrs ...attribute.KeyValue) {
	if t == nil || n <= 0 {
		return
	}
	all := make([]attribute.KeyValue, 0, len(attrs)+1)
	all = append(all, t.chainAttr)
	all = append(all, attrs...)
	t.poolsTouched.Add(ctx, int64(n), metric.WithAttributes(all...))
}

// RecordPoolsNeverIndexed records 0 rather than skipping it: its alert compares
// a level, so the series must exist while the answer is "none".
func (t *Telemetry) RecordPoolsNeverIndexed(ctx context.Context, n int) {
	if t == nil {
		return
	}
	t.poolsNeverIndexed.Record(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}

// RecordTickRows counts the rows a committed block OFFERED to the
// append-on-change writer: an upper bound, since the writer drops any whose
// state is unchanged. Over-counting only makes the growth alert fire early.
func (t *Telemetry) RecordTickRows(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.tickRowsWritten.Add(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}

func (t *Telemetry) RecordPositionRows(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.positionRowsWritten.Add(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}
