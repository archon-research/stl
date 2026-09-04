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

// CleanupContext returns the context for the queue call that settles a message,
// bounded by SettleTimeout. Once shutdown cancelled the parent, that call must
// still go out, so it runs detached from it.
func CleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() != nil {
		ctx = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(ctx, SettleTimeout)
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

// The counter is resolved per settle rather than once at startup: settles reach
// this package through free functions, so there is no constructor to build it
// in, and the meter caches the instrument per name.
type settleRecorder struct {
	settles metric.Int64Counter
	chain   attribute.KeyValue
}

func newSettleRecorder(logger *slog.Logger, chainID int64) settleRecorder {
	settles, err := otel.GetMeterProvider().Meter(instrumentationName).Int64Counter(
		settleCounterName,
		metric.WithDescription("SQS messages settled by the consume loop, by operation and outcome"),
	)
	if err != nil {
		// Metrics must never break the settle path.
		logger.Error("building "+settleCounterName+" counter; settle metrics disabled", "error", err)
		return settleRecorder{}
	}
	return settleRecorder{settles: settles, chain: chainAttribute(logger, chainID)}
}

// The chain name, not the ID: every sibling instrument labels `chain` that way
// and the backup-worker alerts group by it.
func chainAttribute(logger *slog.Logger, chainID int64) attribute.KeyValue {
	name, err := entity.ChainName(chainID)
	if err != nil {
		logger.Error("resolving the chain name for "+settleCounterName,
			"chainID", chainID,
			"error", err)
	}
	return attribute.String("chain", name)
}

func (r settleRecorder) record(ctx context.Context, op, status string) {
	if r.settles == nil {
		return
	}
	r.settles.Add(ctx, 1, metric.WithAttributes(r.chain,
		attribute.String("op", op),
		attribute.String("status", status)))
}

func settleStatus(err error) string {
	if err == nil {
		return settleStatusOK
	}
	return settleStatusFailed
}
