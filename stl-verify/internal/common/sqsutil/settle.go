package sqsutil

import (
	"context"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// SettleTimeout bounds one delete/release call that settles a message. It
// applies on the live path too: the SQS client carries no read timeout, so an
// unbounded settle against a silent connection parks the poll loop for good.
const SettleTimeout = 5 * time.Second

// SupersessionLookupTimeout bounds the block-record read that confirms a block
// was superseded before its message is discarded. It sits on the settle path, so
// ValidateVisibilityTimeout budgets it alongside the handler.
const SupersessionLookupTimeout = 5 * time.Second

// CleanupContext returns the context for a queue call that settles a message
// (delete, release, dead-letter publish): bounded by SettleTimeout, detached from
// the caller's cancellation so a shutdown landing mid-call cannot kill a call SQS
// may already have applied and leave it released against a vanished handle.
func CleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), SettleTimeout)
}

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/common/sqsutil"

// The OTel-to-Prometheus exporter normalises this to sqs_message_settles_total.
const settleCounterName = "sqs.message.settles.total"

const (
	settleOpDelete  = "delete"
	settleOpRelease = "release"

	settleStatusOK     = "ok"
	settleStatusFailed = "failed"
)

// The instrument is resolved per record rather than once at startup: these
// counters are reached through free functions, so there is no constructor to
// build them in, and the meter caches the instrument per name.
type chainCounter struct {
	counter metric.Int64Counter
	chain   attribute.KeyValue
}

func newChainCounter(logger *slog.Logger, chainID int64, name, description string) chainCounter {
	counter, err := otel.GetMeterProvider().Meter(instrumentationName).Int64Counter(
		name,
		metric.WithDescription(description),
	)
	if err != nil {
		// Metrics must never break the path they observe.
		logger.Error("building "+name+" counter; those metrics are disabled", "error", err)
		return chainCounter{}
	}
	return chainCounter{counter: counter, chain: chainAttribute(logger, chainID, name)}
}

func (c chainCounter) add(ctx context.Context, attrs ...attribute.KeyValue) {
	c.emit(ctx, 1, attrs...)
}

// seed exports a series at 0 so a rule reading it with increase() sees the step
// to 1; an OTel series that first appears at 1 hides that step for good.
func (c chainCounter) seed(ctx context.Context, attrs ...attribute.KeyValue) {
	c.emit(ctx, 0, attrs...)
}

func (c chainCounter) emit(ctx context.Context, delta int64, attrs ...attribute.KeyValue) {
	if c.counter == nil {
		return
	}
	c.counter.Add(ctx, delta, metric.WithAttributes(append([]attribute.KeyValue{c.chain}, attrs...)...))
}

// The chain name, not the ID: every sibling instrument labels `chain` that way
// and the backup-worker alerts group by it.
func chainAttribute(logger *slog.Logger, chainID int64, instrument string) attribute.KeyValue {
	name, err := entity.ChainName(chainID)
	if err != nil {
		logger.Error("resolving the chain name for "+instrument,
			"chainID", chainID,
			"error", err)
	}
	return attribute.String("chain", name)
}

type settleRecorder struct{ chainCounter }

func newSettleRecorder(logger *slog.Logger, chainID int64) settleRecorder {
	return settleRecorder{newChainCounter(logger, chainID, settleCounterName,
		"SQS messages settled by the consume loop, by operation and outcome")}
}

func (r settleRecorder) record(ctx context.Context, op, status string) {
	r.add(ctx, attribute.String("op", op), attribute.String("status", status))
}

func settleStatus(err error) string {
	if err == nil {
		return settleStatusOK
	}
	return settleStatusFailed
}
