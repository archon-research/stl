package sqsutil

import (
	"context"
	"log/slog"
	"slices"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ReleaseMessages hands received-but-unfinished messages straight back to the
// queue: a message left in flight blocks its whole FIFO message group (one
// chain's block stream) from the successor until the visibility timeout expires.
func ReleaseMessages(ctx context.Context, consumer outbound.SQSConsumer, logger *slog.Logger, chainID int64, messages []outbound.SQSMessage) {
	if len(messages) == 0 {
		return
	}

	recorder := newSettleRecorder(logger, chainID)

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
	recorder settleRecorder,
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
		refusals = refusalsForCallFailure(logger, handles, refusals, err)
	}

	for _, msg := range messages {
		recorder.record(ctx, settleOpRelease, releaseOutcome(logger, msg, refusals[msg.ReceiptHandle]))
	}
}

// Refusals alongside the error mean the batch was applied, so only the handles
// it names stayed hidden; a nil map means the call itself never landed.
func refusalsForCallFailure(logger *slog.Logger, handles []string, refusals map[string]error, err error) map[string]error {
	if refusals != nil {
		logger.Error("releasing in-flight messages reported a refusal it could not attribute", "error", err)
		return refusals
	}
	failed := make(map[string]error, len(handles))
	for _, handle := range handles {
		failed[handle] = err
	}
	return failed
}

func releaseOutcome(logger *slog.Logger, msg outbound.SQSMessage, err error) string {
	if err != nil {
		logger.Warn("failed to release in-flight message; it stays hidden until the visibility timeout expires",
			"messageID", msg.MessageID,
			"error", err)
	}
	return settleStatus(err)
}

func releaseMessages(ctx context.Context, cfg Config, messages []outbound.SQSMessage) {
	ReleaseMessages(ctx, cfg.Consumer, cfg.Logger, cfg.ChainID, messages)
}
