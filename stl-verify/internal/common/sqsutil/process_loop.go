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

	// HandlerTimeout bounds a single message's handler invocation. When it
	// elapses the handler's context is cancelled; the handler is expected to
	// return promptly with an error, and the message is left undeleted so SQS
	// redelivers it (and eventually DLQs it via the queue's redrive policy).
	// It MUST be less than the queue's visibility timeout so a message is not
	// redelivered while its handler is still running. Zero uses
	// DefaultHandlerTimeout. A worker must never run a handler unbounded: an
	// unbounded handler that blocks on a stuck dependency (e.g. a Postgres lock
	// wait) parks the poll loop forever and silently stalls the queue.
	HandlerTimeout time.Duration
}

// Validate checks that required fields are set.
func (c Config) Validate() error {
	if c.ChainID == 0 {
		return fmt.Errorf("sqsutil.Config: ChainID must be set")
	}
	return nil
}

// DefaultHandlerTimeout bounds a message handler when Config.HandlerTimeout is
// unset. It sits well above observed p99.9 block-processing latency (~28s for
// the slowest worker, morpho) while still turning an indefinite hang into a
// bounded, redeliverable failure. Exported so an SQS consumer can keep its
// visibility timeout above the handler budget (visibility must exceed it).
const DefaultHandlerTimeout = 120 * time.Second

func (c Config) handlerTimeout() time.Duration {
	if c.HandlerTimeout > 0 {
		return c.HandlerTimeout
	}
	return DefaultHandlerTimeout
}

// ValidateVisibilityTimeout returns an error if the SQS visibility timeout does
// not strictly exceed the per-message handler budget. If it does not, a message
// can be redelivered while its handler is still running (duplicate processing /
// re-entrant lock contention). handlerTimeout <= 0 means DefaultHandlerTimeout.
// Callers building an SQS consumer should validate their configured visibility
// timeout against the handler budget they pass to RunLoop.
func ValidateVisibilityTimeout(visibilityTimeout, handlerTimeout time.Duration) error {
	budget := handlerTimeout
	if budget <= 0 {
		budget = DefaultHandlerTimeout
	}
	if visibilityTimeout <= budget {
		return fmt.Errorf("sqsutil: SQS visibility timeout %s must exceed the handler budget %s, "+
			"otherwise a message can be redelivered while its handler is still running", visibilityTimeout, budget)
	}
	return nil
}

// RunLoop polls SQS on a ticker interval and delegates each parsed BlockEvent
// to the handler. It blocks until ctx is cancelled.
//
// At startup it logs an error (but still runs) if the consumer's visibility
// timeout does not exceed the handler budget: that misconfiguration lets a
// message be redelivered while its handler is still running.
func RunLoop(ctx context.Context, cfg Config, handler BlockEventHandler) {
	if cfg.Consumer != nil {
		if err := ValidateVisibilityTimeout(cfg.Consumer.VisibilityTimeout(), cfg.HandlerTimeout); err != nil {
			cfg.Logger.Error("SQS visibility timeout is misconfigured", "error", err)
		}
	}

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

		// Bound each handler so a stuck dependency (e.g. a Postgres lock wait)
		// cannot park the poll loop forever. On timeout the handler's ctx is
		// cancelled and it returns an error, so the message is left undeleted
		// and SQS redelivers it.
		hctx, cancel := context.WithTimeout(ctx, cfg.handlerTimeout())
		err := handler(hctx, event)
		budgetExceeded := errors.Is(hctx.Err(), context.DeadlineExceeded)
		cancel()
		if err != nil {
			cfg.Logger.Error("failed to process message",
				"messageID", msg.MessageID,
				"error", err)
			errs = append(errs, err)
			continue
		}
		if budgetExceeded {
			// Handler returned nil but ran past its budget — it ignored ctx
			// cancellation. The work completed, so we still delete; log it so a
			// handler that does not honour its deadline is visible.
			cfg.Logger.Warn("handler returned nil after exceeding its timeout budget; deleting anyway",
				"messageID", msg.MessageID)
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
