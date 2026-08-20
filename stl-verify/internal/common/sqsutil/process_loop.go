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
	// inside lifecycle.Run's 25s shutdown window. Zero uses DefaultDrainTimeout.
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
// when Config.DrainTimeout is unset. It plus shutdownCleanupTimeout must stay
// under lifecycle.Run's 25s shutdown window, past which the process is killed
// with the message still in flight — the failure this budget exists to avoid.
const DefaultDrainTimeout = 15 * time.Second

// shutdownCleanupTimeout bounds the delete/release call that settles a message
// after shutdown cancelled the parent context.
const shutdownCleanupTimeout = 5 * time.Second

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

// isShutdownCancellation reports whether err is only the loop's context being
// cancelled (SIGTERM), rather than a processing failure. Both conditions are
// required so a genuine error racing shutdown is still logged.
func isShutdownCancellation(ctx context.Context, err error) bool {
	return ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))
}

// ProcessMessages receives a batch of SQS messages, parses each as a
// BlockEvent, calls the handler, and deletes successfully processed messages.
// Events whose chain ID does not match cfg.ChainID are rejected.
//
// Cancelling ctx (SIGTERM) never kills work in progress: the handler already
// running keeps a context detached from the shutdown until the drain budget
// expires, so its message can still be deleted. Every message the shutdown
// leaves unfinished — drain expired, handler failed, or never started — is
// released back to the queue immediately, because a message left in flight
// blocks its whole FIFO message group (one chain's block stream) from the
// successor pod until the visibility timeout expires.
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
	for i, msg := range messages {
		if ctx.Err() != nil {
			releaseMessages(ctx, cfg, messages[i:])
			break
		}
		if err := processMessage(ctx, cfg, msg, handler); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
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

	cleanupCtx, cancel := cleanupContext(ctx)
	defer cancel()
	if err := cfg.Consumer.DeleteMessage(cleanupCtx, msg.ReceiptHandle); err != nil {
		cfg.Logger.Error("failed to delete mismatched message",
			"messageID", msg.MessageID,
			"error", err)
	}
}

// handlerOutcome reports how one handler invocation ended.
type handlerOutcome struct {
	err error

	// budgetExceeded marks a handler that returned nil only after running past
	// HandlerTimeout, i.e. one that ignored its context.
	budgetExceeded bool

	// abandoned marks a handler still running when the shutdown drain expired.
	// Its message goes back to the queue, so the failure is expected, not logged.
	abandoned bool
}

// runHandler invokes the handler on a context detached from ctx, so a SIGTERM
// mid-message does not kill work that can still finish. The detached context
// keeps the handler budget, which bounds a stuck dependency (e.g. a Postgres
// lock wait) whether or not the process is shutting down.
func runHandler(ctx context.Context, cfg Config, event outbound.BlockEvent, handler BlockEventHandler) handlerOutcome {
	hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.handlerTimeout())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- handler(hctx, event) }()

	select {
	case err := <-done:
		return handlerOutcome{err: err, budgetExceeded: exceededBudget(hctx)}
	case <-ctx.Done():
		return drainHandler(hctx, cancel, cfg, done)
	}
}

// drainHandler gives a handler caught mid-message by shutdown the drain budget
// to finish. Past it the handler is cancelled and left to unwind while the
// caller releases its message: the successor pod reprocesses that block, which
// beats holding it for the queue's visibility timeout.
func drainHandler(hctx context.Context, cancelHandler context.CancelFunc, cfg Config, done <-chan error) handlerOutcome {
	drain := time.NewTimer(cfg.drainTimeout())
	defer drain.Stop()

	select {
	case err := <-done:
		return handlerOutcome{err: err, budgetExceeded: exceededBudget(hctx)}
	case <-drain.C:
		cancelHandler()
		return handlerOutcome{
			err:       fmt.Errorf("handler still running after the %s shutdown drain: %w", cfg.drainTimeout(), context.DeadlineExceeded),
			abandoned: true,
		}
	}
}

func exceededBudget(hctx context.Context) bool {
	return errors.Is(hctx.Err(), context.DeadlineExceeded)
}

// settleMessage deletes a message whose handler completed, or keeps an
// unfinished one for redelivery.
func settleMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, outcome handlerOutcome) error {
	if outcome.err != nil {
		return keepMessageForRedelivery(ctx, cfg, msg, outcome)
	}
	deleteProcessedMessage(ctx, cfg, msg, outcome)
	return nil
}

// keepMessageForRedelivery leaves an unfinished message undeleted. Outside
// shutdown that is deliberate retry pacing — the queue's visibility timeout is
// the backoff a poison pill gets — but during shutdown the successor pod must
// not wait it out, so the message is released instead.
func keepMessageForRedelivery(ctx context.Context, cfg Config, msg outbound.SQSMessage, outcome handlerOutcome) error {
	if !outcome.abandoned {
		cfg.Logger.Error("failed to process message",
			"messageID", msg.MessageID,
			"error", outcome.err)
	}
	if ctx.Err() != nil {
		releaseMessage(ctx, cfg, msg)
	}
	return outcome.err
}

func deleteProcessedMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, outcome handlerOutcome) {
	if outcome.budgetExceeded {
		// The work completed, so we still delete; log it so a handler that does
		// not honour its deadline is visible.
		cfg.Logger.Warn("handler returned nil after exceeding its timeout budget; deleting anyway",
			"messageID", msg.MessageID)
	}

	cleanupCtx, cancel := cleanupContext(ctx)
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

func releaseMessages(ctx context.Context, cfg Config, messages []outbound.SQSMessage) {
	for _, msg := range messages {
		releaseMessage(ctx, cfg, msg)
	}
}

// releaseMessage hands a received-but-unfinished message straight back to the
// queue. A failed release only costs the successor the visibility timeout it
// would have waited anyway, and the process is on its way out, so it is logged
// rather than propagated.
func releaseMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage) {
	cleanupCtx, cancel := cleanupContext(ctx)
	defer cancel()

	cfg.Logger.Info("releasing in-flight message for successor", "messageID", msg.MessageID)
	if err := cfg.Consumer.ChangeMessageVisibility(cleanupCtx, msg.ReceiptHandle, 0); err != nil {
		cfg.Logger.Warn("failed to release in-flight message; it stays hidden until the visibility timeout expires",
			"messageID", msg.MessageID,
			"error", err)
	}
}

// cleanupContext returns the context for the queue call that settles a message.
// Once shutdown cancelled the parent, that call must still go out, so it runs
// detached and briefly bounded instead.
func cleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(context.WithoutCancel(ctx), shutdownCleanupTimeout)
}
