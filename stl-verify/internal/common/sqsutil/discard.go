package sqsutil

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/rpcerr"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// SupersededBlockLookup reports whether a canonical, published block already
// stands at blockNumber under a hash other than blockHash. Only that answer
// carries both halves of a safe discard: blockHash lost its reorg, and an event
// went out for the height under the winner, so the height still gets indexed. A
// lookup that cannot establish it returns an error, and the message keeps
// retrying.
type SupersededBlockLookup func(ctx context.Context, blockNumber int64, blockHash string) (bool, error)

// CanonicalBlockReader is the watcher's block record, narrowed to the single
// read a supersession check needs.
type CanonicalBlockReader interface {
	// GetBlockByNumber returns the canonical block at number, or nil when the
	// watcher holds none.
	GetBlockByNumber(ctx context.Context, number int64) (*outbound.BlockState, error)
}

// NewSupersededBlockLookup reads the discard authority off the watcher's own
// record rather than off chain RPC: a differing canonical hash says only that a
// fork was replaced, while a published row is what says an event went out for
// the height — and without that event a discard leaves a hole nothing refills.
func NewSupersededBlockLookup(blocks CanonicalBlockReader) SupersededBlockLookup {
	return func(ctx context.Context, blockNumber int64, blockHash string) (bool, error) {
		state, err := blocks.GetBlockByNumber(ctx, blockNumber)
		if err != nil {
			return false, fmt.Errorf("reading the canonical block at %d: %w", blockNumber, err)
		}
		if state == nil || !state.BlockPublished {
			return false, nil
		}
		return !strings.EqualFold(state.Hash, blockHash), nil
	}
}

// The OTel-to-Prometheus exporter normalises this to sqs_message_discards_total.
const discardCounterName = "sqs.message.discards.total"

const (
	discardReasonNonCanonicalBlock = "non_canonical_block"
	discardReasonForeignChain      = "foreign_chain"
)

// Everything short of a confirmed supersession is recoverable, and the
// never-ack-until-success invariant keeps such a message retrying.
func confirmSupersededBlock(
	ctx context.Context,
	cfg Config,
	msg outbound.SQSMessage,
	event outbound.BlockEvent,
	handlerErr error,
) bool {
	if cfg.SupersededBlock == nil || !errors.Is(handlerErr, rpcerr.ErrBlockUnavailableAtHash) {
		return false
	}
	lookupCtx, cancel := supersessionContext(ctx)
	defer cancel()

	superseded, err := cfg.SupersededBlock(lookupCtx, event.BlockNumber, event.BlockHash)
	if err != nil {
		cfg.Logger.Warn("could not establish whether the block was superseded; keeping its message",
			"messageID", msg.MessageID,
			"chainID", cfg.ChainID,
			"block", event.BlockNumber,
			"blockHash", event.BlockHash,
			"error", err)
		return false
	}
	return superseded
}

// Detached like CleanupContext: a shutdown landing mid-check must not leave a
// message the loop has already decided about unsettled.
func supersessionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), SupersessionLookupTimeout)
}

// Left alone the message reaches the DLQ instead, head-of-line blocking its
// whole chain's FIFO group for the visibility timeouts that takes.
func discardSupersededBlockMessage(
	ctx context.Context,
	cfg Config,
	msg outbound.SQSMessage,
	event outbound.BlockEvent,
) error {
	cfg.Logger.Warn("block was orphaned by a reorg and its height republished; discarding its message",
		"messageID", msg.MessageID,
		"chainID", cfg.ChainID,
		"block", event.BlockNumber,
		"blockHash", event.BlockHash)
	recordDiscard(ctx, cfg, discardReasonNonCanonicalBlock)

	return deleteSettledMessage(ctx, cfg, msg)
}

func recordDiscard(ctx context.Context, cfg Config, reason string) {
	discardCounter(cfg).add(ctx, attribute.String("reason", reason))
}

// seedDiscardCounter exports every reason series before the first poll, so the
// alert that watches for discards can see a worker's very first one.
func seedDiscardCounter(ctx context.Context, cfg Config) {
	counter := discardCounter(cfg)
	for _, reason := range []string{discardReasonNonCanonicalBlock, discardReasonForeignChain} {
		counter.seed(ctx, attribute.String("reason", reason))
	}
}

func discardCounter(cfg Config) chainCounter {
	return newChainCounter(cfg.Logger, cfg.ChainID, discardCounterName,
		"SQS messages the consume loop discarded as unprocessable, by reason")
}
