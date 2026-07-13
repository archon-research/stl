// Package dexconsumer holds the SQS-consumer scaffolding shared by the DEX
// workers (curve today; uniswap-v3 and balancer planned). The per-protocol
// decode/dispatch logic stays in each worker; only the protocol-agnostic
// plumbing lives here: the block-event handler, protocol-id resolution,
// protocol_event persistence, and dependency validation. Extracting it keeps
// each per-DEX worker PR small and removes the copy that would otherwise land
// once per worker.
package dexconsumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ReorgChecker reports whether a published block was reorged out of the
// canonical chain. BlockProcessor uses it to tell such a block (nothing valid to
// index — the watcher republishes the canonical block at that height as a new
// version, which is processed separately) apart from a genuine processing
// failure, which must retry.
//
// The verdict MUST be final: an implementation may only answer true for a block
// at or below the chain's finalized head. Above that boundary our own RPC node's
// view of "canonical" can differ from the watcher's (tip churn, a lagging or
// minority-fork node behind a load balancer), and a false positive there would
// permanently drop a still-canonical block that the watcher never republishes —
// the exact data loss this feature must not cause. Below the finalized head the
// answer is identical on every honest node, so the verdict is safe.
//
// Optional: a nil checker disables reorg-skipping, preserving the original
// always-retry behaviour.
type ReorgChecker interface {
	// ReorgedOut reports true only when blockHash is provably no longer the
	// canonical block at blockNumber AND blockNumber is at or below the finalized
	// head. It returns false for a still-canonical block and for any block it
	// cannot conclusively judge (e.g. not yet finalized). A non-nil error means
	// the check could not be performed — the caller must retry, never skip.
	ReorgedOut(ctx context.Context, blockNumber int64, blockHash common.Hash) (bool, error)
}

// BlockHandler processes every receipt of one block within a single SQS message
// scope. It is the one protocol-specific seam each DEX worker supplies: match
// its pools, decode its event set, do any per-block work (e.g. state snapshots),
// and persist atomically. A non-nil return leaves the SQS message undeleted for
// redelivery; nil acks it. The handler must return an error (never nil) on ctx
// cancellation or any failure, so a block is never acked without being
// persisted ("never ack until success"). Because the handler owns all per-block
// state in local variables, an SQS redelivery reprocesses from scratch with no
// carryover. The handler owns its own error metrics; the block processor does not
// record metrics for handler errors.
type BlockHandler func(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error

// BlockProcessor turns a cached block into a single whole-block handler call. It
// owns the protocol-agnostic steps every DEX worker repeats: read receipts from
// the cache (with S3 fallback), unmarshal them, hand the block to the worker's
// BlockHandler, and record telemetry.
type BlockProcessor struct {
	cache     outbound.BlockCacheReader
	telemetry *dextelemetry.Telemetry
	handle    BlockHandler
	reorg     ReorgChecker // optional; nil disables reorg-skipping
	logger    *slog.Logger
}

// Option configures optional BlockProcessor behaviour.
type Option func(*BlockProcessor)

// WithReorgChecker enables reorg-skipping: on a handler error the processor
// consults the checker and, only when the block is proven reorged out at or
// below the finalized head, acks the message instead of retrying it into the
// DLQ. A nil checker is ignored (reorg-skipping stays off).
func WithReorgChecker(c ReorgChecker) Option {
	return func(p *BlockProcessor) {
		if c != nil {
			p.reorg = c
		}
	}
}

// WithLogger sets the logger used for reorg-skip notices. A nil logger is ignored.
func WithLogger(l *slog.Logger) Option {
	return func(p *BlockProcessor) {
		if l != nil {
			p.logger = l
		}
	}
}

// NewBlockProcessor wires the shared block handler. cache must be the
// S3-fallback reader (dexbootstrap.Deps.CacheReader, built by
// cache.NewReaderWithFallback), so a Redis miss is served transparently from the
// S3 archive; a nil receipts result then means the block is missing from both
// tiers, not merely from Redis. telemetry may be nil (its methods are nil-safe),
// e.g. when metrics are disabled.
func NewBlockProcessor(cache outbound.BlockCacheReader, telemetry *dextelemetry.Telemetry, handle BlockHandler, opts ...Option) *BlockProcessor {
	p := &BlockProcessor{cache: cache, telemetry: telemetry, handle: handle, logger: slog.Default()}
	for _, opt := range opts {
		opt(p)
	}
	// Log the resolved state once: reorg-skipping is an optional dependency, so a
	// wiring regression would otherwise silently revert to always-retry (the DLQ
	// churn this exists to remove) with no signal that it happened.
	p.logger.Info("dex block processor ready", "reorgSkipEnabled", p.reorg != nil)
	return p
}

// ProcessBlockEvent is the sqsutil.BlockEventHandler for a DEX worker. It reads
// the block's receipts (cache, then S3) and hands the whole block to the
// worker's BlockHandler. A non-nil return leaves the SQS message undeleted for
// redelivery.
func (p *BlockProcessor) ProcessBlockEvent(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	start := time.Now()
	reorgSkipped := false
	defer func() {
		// A reorg-skip is neither a processed block nor a failure: counting it as
		// status="success" would keep blocks_processed above zero and so mask a
		// skip storm from the Stalled alert. It is counted via RecordReorgSkip only.
		if reorgSkipped {
			return
		}
		p.telemetry.RecordBlockProcessed(ctx, time.Since(start), retErr)
	}()

	receiptsJSON, err := p.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		p.telemetry.RecordError(ctx, "fetchReceipts", err)
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		// The fallback reader already consulted S3, so a nil here means neither
		// tier has the block. Error (not skip) so the SQS message is redelivered
		// until a backfill populates it.
		err := fmt.Errorf("receipts not found in cache or S3 for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
		p.telemetry.RecordError(ctx, "fetchReceipts", err)
		return err
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		p.telemetry.RecordError(ctx, "unmarshalReceipts", err)
		return fmt.Errorf("unmarshalling receipts: %w", err)
	}

	if err := p.handle(ctx, event, receipts); err != nil {
		if p.reorgedOut(ctx, event) {
			reorgSkipped = true
			return p.ackReorgedOut(ctx, event, err)
		}
		return fmt.Errorf("handling block %d (chain=%d, version=%d): %w", event.BlockNumber, event.ChainID, event.Version, err)
	}
	return nil
}

// ackReorgedOut discards a block proven reorged out: its hash-pinned state read
// can never succeed and there is nothing valid to index, because the block is no
// longer on the canonical chain. The watcher republishes the canonical block at
// this height as a new version, which is processed separately, so acking here
// leaves no gap — it only avoids burning retries into the DLQ. This is an
// explicit, observable discard (WARN + metric), never a silent swallow.
func (p *BlockProcessor) ackReorgedOut(ctx context.Context, event outbound.BlockEvent, handlerErr error) error {
	p.telemetry.RecordReorgSkip(ctx)
	p.logger.Warn("skipping reorged-out block: finalized chain has a different block at this height; canonical version is indexed separately",
		"chainID", event.ChainID,
		"block", event.BlockNumber,
		"version", event.Version,
		"blockHash", event.BlockHash,
		"handlerError", handlerErr.Error(),
	)
	return nil
}

// reorgedOut reports whether the event's block was reorged out of the canonical
// chain. It returns true ONLY on a positive, FINAL confirmation from the checker
// (see ReorgChecker: a verdict is only given at or below the finalized head). A
// nil checker, an unparseable event hash, or a failed check all return false, so
// a genuine (retryable) failure is never mistaken for a reorg and dropped: never
// discard a block we could not prove is reorged out.
func (p *BlockProcessor) reorgedOut(ctx context.Context, event outbound.BlockEvent) bool {
	if p.reorg == nil {
		return false
	}
	hash, err := event.ParsedBlockHash()
	if err != nil {
		// A malformed/empty hash is a real defect, not a reorg. Let the handler
		// error propagate (retry) rather than treating this as reorged.
		p.logger.Warn("cannot check reorg status: unusable block hash on event; treating as retryable",
			"chainID", event.ChainID, "block", event.BlockNumber, "version", event.Version, "error", err)
		return false
	}
	out, err := p.reorg.ReorgedOut(ctx, event.BlockNumber, hash)
	if err != nil {
		// Inconclusive (e.g. the canonical-check RPC is down). Never skip on an
		// unknown; surface it so a persistently-failing checker — which silently
		// disables reorg-skipping and refills the DLQ — is diagnosable.
		p.logger.Warn("reorg check inconclusive; treating block as retryable",
			"chainID", event.ChainID, "block", event.BlockNumber, "version", event.Version, "error", err)
		return false
	}
	return out
}
