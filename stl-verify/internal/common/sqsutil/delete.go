package sqsutil

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// DeleteMessage settles a finished message and counts the outcome, so a refused
// delete — which hides the message for the queue's whole visibility timeout and
// then has the block processed again — is visible as
// sqs.message.settles.total{op="delete"} rather than only as a log line.
// Exported for the workers that run their own receive loop instead of
// ProcessMessages; a worker that settles without it is invisible to
// VectorSQSDeleteFailed.
func DeleteMessage(ctx context.Context, consumer outbound.SQSConsumer, logger *slog.Logger, chainID int64, msg outbound.SQSMessage) error {
	cleanupCtx, cancel := CleanupContext(ctx)
	defer cancel()

	err := consumer.DeleteMessage(cleanupCtx, msg.ReceiptHandle)
	newSettleRecorder(logger, chainID).record(cleanupCtx, settleOpDelete, settleStatus(err))
	if err != nil {
		logger.Error("failed to delete message",
			"messageID", msg.MessageID,
			"error", err)
		return fmt.Errorf("deleting message %s: %w", msg.MessageID, err)
	}
	return nil
}
