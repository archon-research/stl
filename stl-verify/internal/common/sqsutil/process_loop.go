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
	Consumer outbound.SQSConsumer

	// MaxMessages is how many messages one receive may put in flight. The loop
	// settles them one at a time against a visibility clock SQS starts for the
	// whole batch, so raising it multiplies the visibility timeout Validate
	// demands. Zero means one.
	MaxMessages int

	PollInterval time.Duration
	Logger       *slog.Logger

	// ChainID is the expected chain ID for incoming events. Events with a
	// different chain ID are rejected. Must be set (non-zero).
	ChainID int64

	// HandlerTimeout bounds a single message's handler invocation. When it
	// elapses the handler's context is cancelled; the handler is expected to
	// return promptly with an error, and the message is left undeleted so SQS
	// redelivers it (and eventually DLQs it via the queue's redrive policy).
	// Validate rejects a queue whose visibility timeout cannot cover every
	// message of a receive at this budget, so a message is not redelivered while
	// its handler is still running. Zero uses DefaultHandlerTimeout. A worker
	// must never run a handler unbounded: an unbounded handler that blocks on a
	// stuck dependency (e.g. a Postgres lock wait) parks the poll loop forever
	// and silently stalls the queue.
	HandlerTimeout time.Duration

	// DrainTimeout is the grace a handler already running at SIGTERM gets to
	// finish; past it its message is released to the successor. Zero uses
	// DefaultDrainTimeout.
	DrainTimeout time.Duration
}

// Validate checks the config at boot: a worker whose visibility timeout cannot
// cover a receive must refuse to start rather than duplicate every message it
// processes.
func (c Config) Validate() error {
	if c.ChainID == 0 {
		return fmt.Errorf("sqsutil.Config: ChainID must be set")
	}
	if c.Consumer == nil {
		return nil
	}
	return ValidateVisibilityTimeout(c.Consumer.VisibilityTimeout(), c.HandlerTimeout, c.MaxMessages)
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

// DefaultDrainTimeout is the drain grace used when Config.DrainTimeout is
// unset. It plus the settle calls that follow it must stay under
// lifecycle.ShutdownTimeout; lifecycle's shutdown_budget_test.go asserts that.
const DefaultDrainTimeout = 15 * time.Second

func (c Config) drainTimeout() time.Duration {
	if c.DrainTimeout > 0 {
		return c.DrainTimeout
	}
	return DefaultDrainTimeout
}

// ValidateVisibilityTimeout returns an error unless the SQS visibility timeout
// strictly exceeds the wall time a whole receive can take. SQS starts one
// visibility clock for every message it hands back, the loop settles them
// serially, and shutdown adds a drain plus the two settle calls that follow it;
// a timeout below that sum lets a message be redelivered while its handler is
// still running (duplicate processing / re-entrant lock contention).
// handlerTimeout <= 0 means DefaultHandlerTimeout, inFlightPerReceive <= 0 means
// one, and the drain is budgeted at DefaultDrainTimeout.
func ValidateVisibilityTimeout(visibilityTimeout, handlerTimeout time.Duration, inFlightPerReceive int) error {
	budget := handlerTimeout
	if budget <= 0 {
		budget = DefaultHandlerTimeout
	}
	inFlight := max(inFlightPerReceive, 1)
	needed := time.Duration(inFlight)*budget + DefaultDrainTimeout + 2*SettleTimeout
	if visibilityTimeout <= needed {
		return fmt.Errorf("sqsutil: SQS visibility timeout %s must exceed %s, the wall time %d message(s) per receive "+
			"take at a %s handler budget plus the %s shutdown drain and two %s settle calls, "+
			"otherwise a message can be redelivered while its handler is still running",
			visibilityTimeout, needed, inFlight, budget, DefaultDrainTimeout, SettleTimeout)
	}
	return nil
}

// RunLoop polls SQS and delegates each parsed BlockEvent to the handler. It
// blocks until ctx is cancelled.
//
// A receive that returned messages is followed immediately by the next one:
// PollInterval paces an idle queue, and pacing a backlog by it instead would
// cap catch-up at one receive per interval.
func RunLoop(ctx context.Context, cfg Config, handler BlockEventHandler) {
	for ctx.Err() == nil {
		received, err := ProcessMessages(ctx, cfg, handler)
		if err != nil {
			if isShutdownCancellation(ctx, err) {
				return
			}
			cfg.Logger.Error("error processing messages", "error", err)
		}
		// A batch that drained and released cleanly returns no error.
		if ctx.Err() != nil {
			return
		}
		if received == 0 && !waitBeforeNextPoll(ctx, cfg.PollInterval) {
			return
		}
	}
}

func waitBeforeNextPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// The loop's context never carries a deadline, so a DeadlineExceeded here came
// from a nested budget (handler, RPC, DB) and stays a genuine, logged failure.
func isShutdownCancellation(ctx context.Context, err error) bool {
	return ctx.Err() != nil &&
		(errors.Is(err, context.Canceled) || errors.Is(err, ErrDrainAbandoned))
}

// ProcessMessages receives a batch of SQS messages, parses each as a
// BlockEvent, calls the handler, and deletes successfully processed messages.
// Events whose chain ID does not match cfg.ChainID are rejected.
//
// Cancelling ctx (SIGTERM) drains the running handler rather than killing it,
// and every message the shutdown leaves unfinished is released to the successor.
//
// It reports how many messages the receive returned, so a caller can poll again
// straight away while the queue still has a backlog, plus a joined error for any
// failures.
func ProcessMessages(
	ctx context.Context,
	cfg Config,
	handler BlockEventHandler,
) (received int, err error) {
	if err := cfg.Validate(); err != nil {
		return 0, fmt.Errorf("invalid config: %w", err)
	}

	messages, err := cfg.Consumer.ReceiveMessages(ctx, cfg.MaxMessages)
	if err != nil {
		return 0, fmt.Errorf("receiving messages: %w", err)
	}

	if len(messages) == 0 {
		return 0, nil
	}

	cfg.Logger.Info("received messages", "count", len(messages))

	var errs []error
	var inFlight []outbound.SQSMessage
	for i, msg := range messages {
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

	return len(messages), errors.Join(errs...)
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

func processMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, handler BlockEventHandler) error {
	var event outbound.BlockEvent
	if err := json.Unmarshal([]byte(msg.Body), &event); err != nil {
		cfg.Logger.Error("failed to parse block event",
			"messageID", msg.MessageID,
			"error", err)
		return fmt.Errorf("parsing message %s: %w", msg.MessageID, err)
	}

	if event.ChainID != cfg.ChainID {
		return discardForeignChainMessage(ctx, cfg, msg, event)
	}

	outcome := runHandler(ctx, cfg, event, handler)
	return settleMessage(ctx, cfg, msg, event, outcome)
}

// Chain ID is immutable in the message, so redelivery would never succeed.
func discardForeignChainMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, event outbound.BlockEvent) error {
	cfg.Logger.Error("chain ID mismatch, deleting message",
		"messageID", msg.MessageID,
		"expected", cfg.ChainID,
		"got", event.ChainID,
		"block", event.BlockNumber)

	return deleteSettledMessage(ctx, cfg, msg)
}

func runHandler(ctx context.Context, cfg Config, event outbound.BlockEvent, handler BlockEventHandler) DrainOutcome {
	budget := DrainBudget{Work: cfg.handlerTimeout(), Drain: cfg.drainTimeout()}
	return RunDrainable(ctx, budget, func(hctx context.Context) error {
		return handler(hctx, event)
	})
}

func settleMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, event outbound.BlockEvent, outcome DrainOutcome) error {
	if outcome.Err != nil {
		return keepMessageForRedelivery(cfg, msg, event, outcome)
	}
	return deleteProcessedMessage(ctx, cfg, msg, outcome)
}

func keepMessageForRedelivery(cfg Config, msg outbound.SQSMessage, event outbound.BlockEvent, outcome DrainOutcome) error {
	if outcome.Abandoned {
		cfg.Logger.Warn("shutdown drain expired with the handler still running; releasing its message",
			"messageID", msg.MessageID,
			"block", event.BlockNumber,
			"drainBudget", cfg.drainTimeout())
	} else {
		cfg.Logger.Error("failed to process message",
			"messageID", msg.MessageID,
			"error", outcome.Err)
	}
	return outcome.Err
}

func deleteProcessedMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage, outcome DrainOutcome) error {
	if outcome.BudgetExceeded {
		cfg.Logger.Warn("handler returned nil after exceeding its timeout budget; deleting anyway",
			"messageID", msg.MessageID)
	}
	return deleteSettledMessage(ctx, cfg, msg)
}

// A refused delete is returned, not released here: the message then travels
// with every other unsettled one and releaseUnsettledOnShutdown is the single
// place that hands any of them back.
func deleteSettledMessage(ctx context.Context, cfg Config, msg outbound.SQSMessage) error {
	return DeleteMessage(ctx, cfg.Consumer, cfg.Logger, cfg.ChainID, msg)
}
