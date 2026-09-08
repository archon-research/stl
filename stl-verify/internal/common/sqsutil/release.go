package sqsutil

import (
	"context"
	"log/slog"
	"slices"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ReleaseMessages hands received-but-unfinished messages straight back to the
// queue: a message left in flight blocks its whole FIFO message group (one
// chain's block stream) from the successor until the visibility timeout expires.
func ReleaseMessages(ctx context.Context, consumer outbound.SQSConsumer, logger *slog.Logger, chainID int64, messages []outbound.SQSMessage) {
	if len(messages) == 0 {
		return
	}
	for _, msg := range messages {
		logger.Info("releasing in-flight message for successor", "messageID", msg.MessageID)
	}
	visibilityRequest{
		consumer:   consumer,
		logger:     logger,
		chainID:    chainID,
		visibility: 0,
		op:         visibilityOpRelease,
	}.apply(ctx, messages)
}

type visibilityOp struct {
	settleOp string
	refused  string
}

var (
	visibilityOpRelease = visibilityOp{
		settleOp: settleOpRelease,
		refused:  "failed to release in-flight message; it stays hidden until the visibility timeout expires",
	}
	visibilityOpBackoff = visibilityOp{
		settleOp: settleOpBackoff,
		refused:  "failed to apply the retry backoff; the failed message stays hidden until the visibility timeout expires",
	}
)

type visibilityRequest struct {
	consumer   outbound.SQSConsumer
	logger     *slog.Logger
	chainID    int64
	visibility time.Duration
	op         visibilityOp
}

func (r visibilityRequest) apply(ctx context.Context, messages []outbound.SQSMessage) {
	recorder := newSettleRecorder(r.logger, r.chainID)
	for chunk := range slices.Chunk(messages, outbound.MaxVisibilityBatchSize) {
		r.applyChunk(ctx, recorder, chunk)
	}
}

// One queue call per chunk under its own cleanup budget: one call per message,
// or one budget shared across chunks, lets the first throttled call burn the
// whole budget in the SDK's retry chain and strand everything after it.
func (r visibilityRequest) applyChunk(parent context.Context, recorder settleRecorder, messages []outbound.SQSMessage) {
	ctx, cancel := CleanupContext(parent)
	defer cancel()

	handles := make([]string, 0, len(messages))
	for _, msg := range messages {
		handles = append(handles, msg.ReceiptHandle)
	}

	refusals, err := r.consumer.ChangeMessageVisibilityBatch(ctx, handles, r.visibility)
	if err != nil {
		refusals = refusalsForCallFailure(r.logger, handles, refusals, err)
	}

	for _, msg := range messages {
		recorder.record(ctx, r.op.settleOp, r.outcome(msg, refusals[msg.ReceiptHandle]))
	}
}

// Refusals alongside the error mean the batch was applied, so only the handles
// it names stayed hidden; a nil map means the call itself never landed.
func refusalsForCallFailure(logger *slog.Logger, handles []string, refusals map[string]error, err error) map[string]error {
	if refusals != nil {
		logger.Error("changing message visibility reported a refusal it could not attribute", "error", err)
		return refusals
	}
	failed := make(map[string]error, len(handles))
	for _, handle := range handles {
		failed[handle] = err
	}
	return failed
}

func (r visibilityRequest) outcome(msg outbound.SQSMessage, err error) string {
	if err != nil {
		r.logger.Warn(r.op.refused,
			"messageID", msg.MessageID,
			"error", err)
	}
	return settleStatus(err)
}

func releaseMessages(ctx context.Context, cfg Config, messages []outbound.SQSMessage) {
	ReleaseMessages(ctx, cfg.Consumer, cfg.Logger, cfg.ChainID, messages)
}
