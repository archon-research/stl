// stl-verify/internal/services/shared/sqsutil/process_loop.go
package sqsutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// BlockEventHandler processes a single parsed block event.
type BlockEventHandler func(ctx context.Context, event outbound.BlockEvent) error

// Config holds configuration for the SQS processing loop.
type Config struct {
	Consumer     outbound.SQSConsumer
	MaxMessages  int
	PollInterval time.Duration
	Logger       *slog.Logger
}

// RunLoop polls SQS on a ticker interval and delegates each parsed BlockEvent
// to the handler. It blocks until ctx is cancelled.
func RunLoop(ctx context.Context, cfg Config, handler BlockEventHandler) {
	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ProcessMessages(ctx, cfg.Consumer, cfg.MaxMessages, cfg.Logger, handler); err != nil {
				cfg.Logger.Error("error processing messages", "error", err)
			}
		}
	}
}

// ProcessMessages receives a batch of SQS messages, parses each as a
// BlockEvent, calls the handler, and deletes successfully processed messages.
// Returns a joined error for any failures.
func ProcessMessages(
	ctx context.Context,
	consumer outbound.SQSConsumer,
	maxMessages int,
	logger *slog.Logger,
	handler BlockEventHandler,
) error {
	messages, err := consumer.ReceiveMessages(ctx, maxMessages)
	if err != nil {
		return fmt.Errorf("receiving messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	logger.Info("received messages", "count", len(messages))

	var errs []error
	for _, msg := range messages {
		var event outbound.BlockEvent
		if err := json.Unmarshal([]byte(msg.Body), &event); err != nil {
			logger.Error("failed to parse block event",
				"messageID", msg.MessageID,
				"error", err)
			errs = append(errs, fmt.Errorf("parsing message %s: %w", msg.MessageID, err))
			continue
		}

		if err := handler(ctx, event); err != nil {
			logger.Error("failed to process message",
				"messageID", msg.MessageID,
				"error", err)
			errs = append(errs, err)
			continue
		}

		if deleteErr := consumer.DeleteMessage(ctx, msg.ReceiptHandle); deleteErr != nil {
			logger.Error("failed to delete message",
				"messageID", msg.MessageID,
				"error", deleteErr)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
