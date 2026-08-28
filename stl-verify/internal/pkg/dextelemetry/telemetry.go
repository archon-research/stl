// Package dextelemetry exposes a small per-worker OpenTelemetry helper shared
// by the DEX SQS workers the dex-indexer binary can run (curve, uniswap-v3,
// uniswap-v4). The metric prefix is a parameter so one implementation serves
// every DEX; names follow the `<prefix>_blocks_processed_total` /
// `<prefix>_errors_total` convention the rules in alerts/vector-indexers.yaml
// are written against.
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
	prefix             string
	chainAttr          attribute.KeyValue
	blocksProcessed    metric.Int64Counter
	errorsTotal        metric.Int64Counter
	blockDuration      metric.Float64Histogram
	stateRowsWritten   metric.Int64Counter
	stateRowsAttempted metric.Int64Counter
	poolsTouched       metric.Int64Counter
	poolsNeverIndexed  metric.Int64Gauge
}

// NewTelemetry registers five counters (`<prefix>.blocks.processed`,
// `<prefix>.errors.total`, `<prefix>.state.rows.written`,
// `<prefix>.state.rows.attempted`, `<prefix>.pools.touched`), the
// `<prefix>.block.duration_seconds` histogram, and the
// `<prefix>.pools.never_indexed` gauge. Every DEX gets the whole set;
// an instrument a worker never records simply produces no series. The
// OTel-to-Prometheus exporter normalises the dots to underscores and adds the
// `_total` suffix to the counters, yielding the metric series names the alert
// rules expect. The chain NAME (via entity.ChainName) is baked into
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

	stateRowsAttempted, err := meter.Int64Counter(
		prefix+".state.rows.attempted",
		metric.WithDescription("Total state snapshot rows a block queued for insert, conflicts included"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.state.rows.attempted counter: %w", prefix, err)
	}

	touched, err := meter.Int64Counter(
		prefix+".pools.touched",
		metric.WithDescription("Total registered pools touched by decoded events"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.pools.touched counter: %w", prefix, err)
	}

	neverIndexed, err := meter.Int64Gauge(
		prefix+".pools.never_indexed",
		metric.WithDescription("Registered, snapshot-supported pools that have never produced a state or tick row"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating %s.pools.never_indexed gauge: %w", prefix, err)
	}

	return &Telemetry{
		prefix:             prefix,
		chainAttr:          attribute.String("chain", chainName),
		blocksProcessed:    blocks,
		errorsTotal:        errs,
		blockDuration:      dur,
		stateRowsWritten:   stateRows,
		stateRowsAttempted: stateRowsAttempted,
		poolsTouched:       touched,
		poolsNeverIndexed:  neverIndexed,
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

// RecordStateRows increments state_rows_written_total by n, the number of state
// snapshot rows a block actually appended. Volume observability: it answers
// "how much did this worker add", and an idempotent replay legitimately adds
// nothing. Nil receiver or n <= 0 are no-ops.
func (t *Telemetry) RecordStateRows(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.stateRowsWritten.Add(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}

// RecordStateRowsAttempted increments state_rows_attempted_total by n, the
// number of state snapshot rows a block QUEUED for insert — rows that
// conflicted away included. It is the health signal every DEX worker's
// not-writing-state alert keys on, and it is deliberately not the same question
// as RecordStateRows: replaying an already-committed range under one build_id
// reuses the processing_version, so every INSERT hits ON CONFLICT DO NOTHING
// and writes nothing while the worker is perfectly healthy. A conflict under
// that primary key means the identical row is already in the table, so zero
// written is provably not a data hole; zero ATTEMPTED, by contrast, is exactly
// what every bug those alerts exist for produces (an empty due set, a
// no-opping snapshot step, a dropped write set).
//
// n <= 0 is a no-op, matching RecordStateRows: the rules use
// `A > 0 unless B > 0` so the series must stay absent, not zero, when nothing
// was attempted. Nil receiver is a no-op too.
func (t *Telemetry) RecordStateRowsAttempted(ctx context.Context, n int) {
	if t == nil || n <= 0 {
		return
	}
	t.stateRowsAttempted.Add(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}

// RecordPoolsTouched increments pools_touched_total by n, the number of
// registered pools that a block's decoded events touched — the activity signal
// the sweepless silent-empty alerts gate on (rationale in
// alerts/vector-indexers.yaml, group vector-uniswap-v3-indexer). Nil receiver or
// n <= 0 are no-ops, so a worker that never touches a pool never creates the
// series at all; those rules use `unless` rather than `and … == 0` precisely to
// fire on the absent series.
//
// Callers must record it from the touched-pool set decoded off the receipts
// (upstream of DueSet), never from the due set itself: an always-empty DueSet is
// precisely the bug those alerts exist to catch, and sourcing the gate from
// DueSet would zero both sides of the comparison and go blind. The sweep (curve)
// also puts untouched pools in the due set, which would make it a false activity
// signal.
//
// attrs partition the count into series a rule can select one of, on top of the
// chain attribute every datapoint carries; a rule that wants the whole count
// aggregates them away with `sum by (chain, cluster)`. uniswap-v4 splits on
// snapshot_supported, because only the supported half can ever produce a state
// row and only that half may gate its not-writing-state rule.
func (t *Telemetry) RecordPoolsTouched(ctx context.Context, n int, attrs ...attribute.KeyValue) {
	if t == nil || n <= 0 {
		return
	}
	all := make([]attribute.KeyValue, 0, len(attrs)+1)
	all = append(all, t.chainAttr)
	all = append(all, attrs...)
	t.poolsTouched.Add(ctx, int64(n), metric.WithAttributes(all...))
}

// RecordPoolsNeverIndexed sets pools_never_indexed to n. Unlike the counters
// here, 0 is a value and not a no-op: the alert on it compares a level, so the
// series has to exist while the answer is "none". Nil receiver is still a no-op.
func (t *Telemetry) RecordPoolsNeverIndexed(ctx context.Context, n int) {
	if t == nil {
		return
	}
	t.poolsNeverIndexed.Record(ctx, int64(n), metric.WithAttributes(t.chainAttr))
}
