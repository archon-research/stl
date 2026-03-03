// Package sqsutil provides shared utilities for SQS message processing.
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

	// ChainID is the expected chain ID for incoming events. Events with a
	// different chain ID are rejected. Must be set (non-zero).
	ChainID int64
}

// Validate checks that required fields are set.
func (c Config) Validate() error {
	if c.ChainID == 0 {
		return fmt.Errorf("sqsutil.Config: ChainID must be set")
	}
	return nil
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
			if err := ProcessMessages(ctx, cfg, handler); err != nil {
				cfg.Logger.Error("error processing messages", "error", err)
			}
		}
	}
}

// ProcessMessages receives a batch of SQS messages, parses each as a
// BlockEvent, calls the handler, and deletes successfully processed messages.
// Events whose chain ID does not match cfg.ChainID are rejected.
// Returns a joined error for any failures.
func ProcessMessages(
	ctx context.Context,
	cfg Config,
	handler BlockEventHandler,
) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	messages, err := cfg.Consumer.ReceiveMessages(ctx, cfg.MaxMessages)
	if err != nil {
		return fmt.Errorf("receiving messages: %w", err)
	}

	if len(messages) == 0 {
		return nil
	}

	cfg.Logger.Info("received messages", "count", len(messages))

	var errs []error
	for _, msg := range messages {
		var event outbound.BlockEvent
		if err := json.Unmarshal([]byte(msg.Body), &event); err != nil {
			cfg.Logger.Error("failed to parse block event",
				"messageID", msg.MessageID,
				"error", err)
			errs = append(errs, fmt.Errorf("parsing message %s: %w", msg.MessageID, err))
			continue
		}

		if event.ChainID != cfg.ChainID {
			cfg.Logger.Error("chain ID mismatch, deleting message",
				"messageID", msg.MessageID,
				"expected", cfg.ChainID,
				"got", event.ChainID,
				"block", event.BlockNumber)
			// Chain ID is immutable in the message — retrying will never succeed.
			// Delete to avoid infinite retry. Investigate queue subscription if this occurs.
			if deleteErr := cfg.Consumer.DeleteMessage(ctx, msg.ReceiptHandle); deleteErr != nil {
				cfg.Logger.Error("failed to delete mismatched message",
					"messageID", msg.MessageID,
					"error", deleteErr)
			}
			continue
		}

		if err := handler(ctx, event); err != nil {
			cfg.Logger.Error("failed to process message",
				"messageID", msg.MessageID,
				"error", err)
			errs = append(errs, err)
			continue
		}

		if deleteErr := cfg.Consumer.DeleteMessage(ctx, msg.ReceiptHandle); deleteErr != nil {
			cfg.Logger.Error("failed to delete message",
				"messageID", msg.MessageID,
				"error", deleteErr)
		}
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
