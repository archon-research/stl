package sqsutil

import (
	"context"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ReleaseMessages hands received-but-unfinished messages straight back to the
// queue: a message left in flight blocks its whole FIFO message group (one
// chain's block stream) from the successor until the visibility timeout expires.
func ReleaseMessages(ctx context.Context, consumer outbound.SQSConsumer, logger *slog.Logger, chainID int64, messages []outbound.SQSMessage) {
	if len(messages) == 0 {
		return
	}

	recorder := newReleaseRecorder(logger, chainID)

	for chunk := range slices.Chunk(messages, outbound.MaxVisibilityBatchSize) {
		releaseChunk(ctx, consumer, logger, recorder, chunk)
	}
}

// One queue call per chunk under its own cleanup budget: one call per message,
// or one budget shared across chunks, lets the first throttled call burn the
// whole budget in the SDK's retry chain and strand everything after it.
func releaseChunk(
	parent context.Context,
	consumer outbound.SQSConsumer,
	logger *slog.Logger,
	recorder releaseRecorder,
	messages []outbound.SQSMessage,
) {
	ctx, cancel := CleanupContext(parent)
	defer cancel()

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
	ReleaseMessages(ctx, cfg.Consumer, cfg.Logger, cfg.ChainID, messages)
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
	chain    attribute.KeyValue
}

func newReleaseRecorder(logger *slog.Logger, chainID int64) releaseRecorder {
	releases, err := otel.GetMeterProvider().Meter(instrumentationName).Int64Counter(
		releaseCounterName,
		metric.WithDescription("SQS messages handed back to the queue during settle/shutdown, by outcome"),
	)
	if err != nil {
		// Metrics must never break the release path.
		logger.Error("building "+releaseCounterName+" counter; release metrics disabled", "error", err)
		return releaseRecorder{}
	}
	return releaseRecorder{releases: releases, chain: chainAttribute(logger, chainID)}
}

// The chain name, not the ID: every sibling instrument labels `chain` that way
// and the backup-worker alerts group by it.
func chainAttribute(logger *slog.Logger, chainID int64) attribute.KeyValue {
	name, err := entity.ChainName(chainID)
	if err != nil {
		logger.Error("resolving the chain name for "+releaseCounterName,
			"chainID", chainID,
			"error", err)
	}
	return attribute.String("chain", name)
}

func (r releaseRecorder) record(ctx context.Context, status string) {
	if r.releases == nil {
		return
	}
	r.releases.Add(ctx, 1, metric.WithAttributes(r.chain, attribute.String("status", status)))
}
