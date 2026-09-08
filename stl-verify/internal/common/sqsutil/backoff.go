package sqsutil

import (
	"context"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// FailureBackoff is how long a failed message stays hidden before SQS
// redelivers it: Base after its first receive, doubled per further receive,
// never above Cap. Left alone it would stay hidden for the queue's whole
// visibility timeout, and its chain's FIFO group delivers nothing behind it.
// Both fields are required; Config treats the zero value as the default.
type FailureBackoff struct {
	Base time.Duration
	Cap  time.Duration
}

// DefaultFailureBackoffBase and DefaultFailureBackoffCap give 15s, 30s, 60s.
// At the queues' current maxReceiveCount of 3 a poison pill reaches the DLQ
// in under two minutes instead of three visibility timeouts, and a provider
// outage has 45s to clear before a healthy block's last attempt.
const (
	DefaultFailureBackoffBase = 15 * time.Second
	DefaultFailureBackoffCap  = 60 * time.Second
)

// Delay reports how long a message SQS has delivered receiveCount times stays
// hidden. A count below one, from a consumer that does not report it, is a
// first receive.
func (b FailureBackoff) Delay(receiveCount int) time.Duration {
	delay := b.Base
	for range max(receiveCount, 1) - 1 {
		if delay >= b.Cap {
			break
		}
		delay *= 2
	}
	return min(delay, b.Cap)
}

// Validate rejects a schedule that would hand a failed message straight back:
// the redrive policy would then dead-letter a healthy block within seconds of
// a provider blip. SQS takes whole seconds, so under one rounds to zero.
func (b FailureBackoff) Validate() error {
	if b.Base < time.Second {
		return fmt.Errorf("sqsutil: failure backoff base %s must be at least one second", b.Base)
	}
	if b.Cap < b.Base {
		return fmt.Errorf("sqsutil: failure backoff cap %s must be at least the base %s", b.Cap, b.Base)
	}
	return nil
}

// A failure that lands during shutdown is not backed off: the message travels
// with the batch's other unsettled ones to releaseUnsettledOnShutdown, so the
// successor gets it at once instead of waiting the backoff out.
func backOffFailedMessageUnlessShuttingDown(ctx context.Context, cfg Config, msg outbound.SQSMessage) {
	if ctx.Err() != nil {
		return
	}
	delay := cfg.failureBackoff().Delay(msg.ReceiveCount)
	cfg.Logger.Info("hiding failed message for its retry backoff",
		"messageID", msg.MessageID,
		"receiveCount", msg.ReceiveCount,
		"backoff", delay)
	visibilityRequest{
		consumer:   cfg.Consumer,
		logger:     cfg.Logger,
		chainID:    cfg.ChainID,
		visibility: delay,
		op:         visibilityOpBackoff,
	}.apply(ctx, []outbound.SQSMessage{msg})
}
