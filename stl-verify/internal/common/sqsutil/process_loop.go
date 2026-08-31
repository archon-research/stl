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

	// DrainTimeout bounds the grace a handler that is already running when ctx
	// is cancelled (SIGTERM) gets to finish its message. Within it the handler
	// keeps working on a context detached from the shutdown, so the message can
	// be deleted normally; past it the handler is cancelled and its message
	// released to the successor. It must fit, together with the cleanup calls,
	// inside lifecycle.ShutdownTimeout. Zero uses DefaultDrainTimeout.
	DrainTimeout time.Duration
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

// DefaultDrainTimeout is the grace a mid-message handler gets after SIGTERM
// when Config.DrainTimeout is unset. It plus the settle calls that follow it
// (up to 3x ShutdownCleanupTimeout, see lifecycle.ShutdownTimeout) must stay
// under lifecycle.ShutdownTimeout, past which the process is killed with the
// message still in flight — the failure this budget exists to avoid.
const DefaultDrainTimeout = 15 * time.Second

// ShutdownCleanupTimeout bounds one delete/release call that settles a message
// after shutdown cancelled the parent context. A single drained message can
// need three of them in sequence (delete, the release that follows a failed
// delete, then the release of the rest of the batch), which is how it enters
// the chain derived on lifecycle.ShutdownTimeout.
const ShutdownCleanupTimeout = 5 * time.Second

func (c Config) drainTimeout() time.Duration {
	if c.DrainTimeout > 0 {
		return c.DrainTimeout
	}
	return DefaultDrainTimeout
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
				if isShutdownCancellation(ctx, err) {
					return
				}
				cfg.Logger.Error("error processing messages", "error", err)
			}
			// A batch that drained and released cleanly returns no error, so the
			// cancelled context is the only signal left to stop polling on.
			if ctx.Err() != nil {
				return
			}
		}
	}
}

// isShutdownCancellation reports whether err is only the shutdown itself — the
// loop's context cancelled (SIGTERM), or a handler the drain budget abandoned —
// rather than a processing failure. The loop's context never carries a deadline,
// so a DeadlineExceeded here always came from a nested budget (handler timeout,
// RPC or DB per-attempt deadline) and stays a genuine, logged failure.
func isShutdownCancellation(ctx context.Context, err error) bool {
	return ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, ErrDrainAbandoned))
}

// ProcessMessages receives a batch of SQS messages, parses each as a
// BlockEvent, calls the handler, and deletes successfully processed messages.
// Events whose chain ID does not match cfg.ChainID are rejected.
//
// Cancelling ctx (SIGTERM) never kills work in progress: the handler already
// running keeps a context detached from the shutdown until the drain budget
// expires, so its message can still be deleted. Every message the shutdown
// leaves unfinished — drain expired, handler failed, body unparseable, never
// started, or arrived in a poll that completed after the signal — is released
// back to the queue, because a message left in flight blocks its whole FIFO
// message group (one chain's block stream) from the successor pod until the
// visibility timeout expires.
//
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
	var inFlight []outbound.SQSMessage
	for i, msg := range messages {
		// The shutdown guard covers a batch that landed after the signal (i == 0,
		// because the consumer never abandons an in-flight poll) as well as one
		// the signal interrupted.
		if ctx.Err() != nil {
			inFlight = append(inFlight, messages[i:]...)
			break
		}
		if err := processMessage(ctx, cfg, msg, handler); err != nil {
			errs = append(errs, err)
			inFlight = append(inFlight, msg)
		}
	}
	releaseUnsettledOnShutdown(ctx, cfg, inFlight)

	return errors.Join(errs...)
}

// releaseUnsettledOnShutdown releases poison pills too: a visibility change
// leaves ApproximateReceiveCount alone, so redrive still walks them to the DLQ.
// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
func releaseUnsettledOnShutdown(ctx context.Context, cfg Config, messages []outbound.SQSMessage) {
	if ctx.Err() == nil {
		return
	}
	releaseMessages(ctx, cfg, messages)
}

// processMessage parses one message, runs its handler within the handler
// budget, and then settles the message: deleted, or kept for redelivery.
func processMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, handler BlockEventHandler) error {
	var event outbound.BlockEvent
	if err := json.Unmarshal([]byte(msg.Body), &event); err != nil {
		cfg.Logger.Error("failed to parse block event",
			"messageID", msg.MessageID,
			"error", err)
		return fmt.Errorf("parsing message %s: %w", msg.MessageID, err)
	}

	if event.ChainID != cfg.ChainID {
		discardForeignChainMessage(ctx, cfg, msg, event)
		return nil
	}

	outcome := runHandler(ctx, cfg, event, handler)
	return settleMessage(ctx, cfg, msg, outcome)
}

// discardForeignChainMessage deletes a message for another chain: chain ID is
// immutable in the message, so redelivery would never succeed. Investigate the
// queue subscription if this occurs.
func discardForeignChainMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, event outbound.BlockEvent) {
	cfg.Logger.Error("chain ID mismatch, deleting message",
		"messageID", msg.MessageID,
		"expected", cfg.ChainID,
		"got", event.ChainID,
		"block", event.BlockNumber)

	cleanupCtx, cancel := CleanupContext(ctx)
	defer cancel()
	if err := cfg.Consumer.DeleteMessage(cleanupCtx, msg.ReceiptHandle); err != nil {
		cfg.Logger.Error("failed to delete mismatched message",
			"messageID", msg.MessageID,
			"error", err)
		if ctx.Err() != nil {
			releaseMessage(ctx, cfg, msg)
		}
	}
}

// runHandler invokes the handler under the shared drain primitive, so a SIGTERM
// mid-message does not kill work that can still finish.
func runHandler(ctx context.Context, cfg Config, event outbound.BlockEvent, handler BlockEventHandler) DrainOutcome {
	budget := DrainBudget{Work: cfg.handlerTimeout(), Drain: cfg.drainTimeout()}
	return RunDrainable(ctx, budget, func(hctx context.Context) error {
		return handler(hctx, event)
	})
}

// settleMessage deletes a message whose handler completed, or keeps an
// unfinished one for redelivery.
func settleMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, outcome DrainOutcome) error {
	if outcome.Err != nil {
		return keepMessageForRedelivery(cfg, msg, outcome)
	}
	deleteProcessedMessage(ctx, cfg, msg, outcome)
	return nil
}

// keepMessageForRedelivery leaves an unfinished message undeleted, returning
// the failure that ProcessMessages tracks it by: a message that comes back with
// an error is one releaseUnsettledOnShutdown must hand over if the shutdown has
// begun by the time the batch ends.
func keepMessageForRedelivery(cfg Config, msg outbound.SQSMessage, outcome DrainOutcome) error {
	if !outcome.Abandoned {
		cfg.Logger.Error("failed to process message",
			"messageID", msg.MessageID,
			"error", outcome.Err)
	}
	return outcome.Err
}

func deleteProcessedMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, outcome DrainOutcome) {
	if outcome.BudgetExceeded {
		// The work completed, so we still delete; log it so a handler that does
		// not honour its deadline is visible.
		cfg.Logger.Warn("handler returned nil after exceeding its timeout budget; deleting anyway",
			"messageID", msg.MessageID)
	}

	cleanupCtx, cancel := CleanupContext(ctx)
	defer cancel()
	if err := cfg.Consumer.DeleteMessage(cleanupCtx, msg.ReceiptHandle); err != nil {
		cfg.Logger.Error("failed to delete message",
			"messageID", msg.MessageID,
			"error", err)
		if ctx.Err() != nil {
			releaseMessage(ctx, cfg, msg)
		}
	}
}

// CleanupContext returns the context for the queue call that settles a message
// (delete, dead-letter publish, release). Once shutdown cancelled the parent,
// that call must still go out, so it runs detached and bounded by
// ShutdownCleanupTimeout instead. Exported so a worker that runs its own
// message loop settles messages on the same budget this package does.
func CleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(context.WithoutCancel(ctx), ShutdownCleanupTimeout)
}
