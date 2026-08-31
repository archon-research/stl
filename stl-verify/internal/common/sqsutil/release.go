package sqsutil

import (
	"context"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ReleaseMessages hands received-but-unfinished messages straight back to the
// queue: a message left in flight blocks its whole FIFO message group (one
// chain's block stream) from the successor until the visibility timeout expires.
func ReleaseMessages(ctx context.Context, consumer outbound.SQSConsumer, logger *slog.Logger, messages []outbound.SQSMessage) {
	if len(messages) == 0 {
		return
	}

	cleanupCtx, cancel := CleanupContext(ctx)
	defer cancel()
	recorder := newReleaseRecorder(logger)

	for chunk := range slices.Chunk(messages, outbound.MaxVisibilityBatchSize) {
		releaseChunk(cleanupCtx, consumer, logger, recorder, chunk)
	}
}

// One queue call per chunk, not per message: under the shared cleanup budget
// the first throttled call would burn it in the SDK's retry chain and strand
// every message after it.
func releaseChunk(
	ctx context.Context,
	consumer outbound.SQSConsumer,
	logger *slog.Logger,
	recorder releaseRecorder,
	messages []outbound.SQSMessage,
) {
	handles := make([]string, 0, len(messages))
	for _, msg := range messages {
		logger.Info("releasing in-flight message for successor", "messageID", msg.MessageID)
		handles = append(handles, msg.ReceiptHandle)
	}

	refusals, err := consumer.ChangeMessageVisibilityBatch(ctx, handles, 0)
	if err != nil {
		refusals = make(map[string]error, len(handles))
		for _, handle := range handles {
			refusals[handle] = err
		}
	}

	for _, msg := range messages {
		recorder.record(ctx, releaseOutcome(logger, msg, refusals[msg.ReceiptHandle]))
	}
}

func releaseOutcome(logger *slog.Logger, msg outbound.SQSMessage, err error) string {
	if err == nil {
		return releaseStatusReleased
	}
	logger.Warn("failed to release in-flight message; it stays hidden until the visibility timeout expires",
		"messageID", msg.MessageID,
		"error", err)
	return releaseStatusFailed
}

func releaseMessages(ctx context.Context, cfg Config, messages []outbound.SQSMessage) {
	ReleaseMessages(ctx, cfg.Consumer, cfg.Logger, messages)
}

func releaseMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage) {
	releaseMessages(ctx, cfg, []outbound.SQSMessage{msg})
}

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/common/sqsutil"

// The OTel-to-Prometheus exporter normalises this to sqs_message_releases_total.
const releaseCounterName = "sqs.message.releases.total"

const (
	releaseStatusReleased = "released"
	releaseStatusFailed   = "failed"
)

// The counter is resolved per ReleaseMessages call, not once at startup:
// releases reach this package through free functions, so there is no
// constructor to build it in, and they only run on the settle/shutdown paths.
type releaseRecorder struct {
	releases metric.Int64Counter
}

func newReleaseRecorder(logger *slog.Logger) releaseRecorder {
	releases, err := otel.GetMeterProvider().Meter(instrumentationName).Int64Counter(
		releaseCounterName,
		metric.WithDescription("SQS messages handed back to the queue during settle/shutdown, by outcome"),
	)
	if err != nil {
		// Metrics must never break the release path.
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
