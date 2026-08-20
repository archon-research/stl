package sqsutil

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ReleaseMessages hands received-but-unfinished messages straight back to the
// queue, because a message left in flight blocks its whole FIFO message group
// (one chain's block stream) from the successor pod until the visibility
// timeout expires. The whole batch shares one cleanup budget: a budget per
// message would scale the shutdown tail with the batch size and no longer fit
// lifecycle.ShutdownTimeout. A failed release only costs the successor the
// visibility timeout it would have waited anyway, and the process is on its way
// out, so it is logged rather than propagated.
func ReleaseMessages(ctx context.Context, consumer outbound.SQSConsumer, logger *slog.Logger, messages []outbound.SQSMessage) {
	if len(messages) == 0 {
		return
	}

	cleanupCtx, cancel := CleanupContext(ctx)
	defer cancel()
	recorder := newReleaseRecorder(logger)

	for _, msg := range messages {
		logger.Info("releasing in-flight message for successor", "messageID", msg.MessageID)
		if err := consumer.ChangeMessageVisibility(cleanupCtx, msg.ReceiptHandle, 0); err != nil {
			logger.Warn("failed to release in-flight message; it stays hidden until the visibility timeout expires",
				"messageID", msg.MessageID,
				"error", err)
			recorder.record(cleanupCtx, releaseStatusFailed)
			continue
		}
		recorder.record(cleanupCtx, releaseStatusReleased)
	}
}

func releaseMessages(ctx context.Context, cfg Config, messages []outbound.SQSMessage) {
	ReleaseMessages(ctx, cfg.Consumer, cfg.Logger, messages)
}

func releaseMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage) {
	releaseMessages(ctx, cfg, []outbound.SQSMessage{msg})
}

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/common/sqsutil"

// releaseCounterName counts messages handed back to the queue. The
// OTel-to-Prometheus exporter normalises it to sqs_message_releases_total.
const releaseCounterName = "sqs.message.releases.total"

// Outcome labels on releaseCounterName. A failed release means the message
// stays hidden for the queue's full visibility timeout, which on a FIFO queue
// stalls one chain's whole block stream — the blackout the release exists to
// prevent, so it must be distinguishable from a clean release.
const (
	releaseStatusReleased = "released"
	releaseStatusFailed   = "failed"
)

// releaseRecorder counts release attempts. Its counter is resolved from the
// global meter provider per ReleaseMessages call rather than once at startup:
// every consumer reaches releases through this package's free functions, not
// through a constructor, so there is no startup hook to build it in — and
// releases only happen on the settle and shutdown paths, which makes the
// lookup irrelevant next to the queue call it labels.
type releaseRecorder struct {
	releases metric.Int64Counter
}

func newReleaseRecorder(logger *slog.Logger) releaseRecorder {
	releases, err := otel.GetMeterProvider().Meter(instrumentationName).Int64Counter(
		releaseCounterName,
		metric.WithDescription("SQS messages handed back to the queue during settle/shutdown, by outcome"),
	)
	if err != nil {
		// A counter that fails to build must never break the release path, which
		// is what keeps the successor pod from stalling; log and record nothing.
		logger.Error("building "+releaseCounterName+" counter; release metrics disabled", "error", err)
		return releaseRecorder{}
	}
	return releaseRecorder{releases: releases}
}

func (r releaseRecorder) record(ctx context.Context, status string) {
	if r.releases == nil {
		return
	}
	r.releases.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
}
