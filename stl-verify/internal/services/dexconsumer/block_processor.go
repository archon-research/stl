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
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// BlockHandler processes every receipt of one block within a single SQS message
// scope. It is the one protocol-specific seam each DEX worker supplies: match
// its pools, decode its event set, do any per-block work (e.g. state snapshots),
// and persist atomically. A non-nil return leaves the SQS message undeleted for
// redelivery; nil acks it. The handler must return an error (never nil) on ctx
// cancellation or any failure, so a block is never acked without being
// persisted ("never ack until success"). Because the handler owns all per-block
// state in local variables, an SQS redelivery reprocesses from scratch with no
// carryover.
type BlockHandler func(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error

// BlockProcessor turns a cached block into a single whole-block handler call. It
// owns the protocol-agnostic steps every DEX worker repeats: read receipts from
// the cache (with S3 fallback), unmarshal them, hand the block to the worker's
// BlockHandler, and record telemetry.
type BlockProcessor struct {
	cache     outbound.BlockCacheReader
	telemetry *dextelemetry.Telemetry
	handle    BlockHandler
}

// NewBlockProcessor wires the shared block handler. cache must be the
// S3-fallback reader (dexbootstrap.Deps.CacheReader, built by
// cache.NewReaderWithFallback), so a Redis miss is served transparently from the
// S3 archive; a nil receipts result then means the block is missing from both
// tiers, not merely from Redis. telemetry may be nil (its methods are nil-safe),
// e.g. when metrics are disabled.
func NewBlockProcessor(cache outbound.BlockCacheReader, telemetry *dextelemetry.Telemetry, handle BlockHandler) *BlockProcessor {
	return &BlockProcessor{cache: cache, telemetry: telemetry, handle: handle}
}

// ProcessBlockEvent is the sqsutil.BlockEventHandler for a DEX worker. It reads
// the block's receipts (cache, then S3) and hands the whole block to the
// worker's BlockHandler. A non-nil return leaves the SQS message undeleted for
// redelivery.
func (p *BlockProcessor) ProcessBlockEvent(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	start := time.Now()
	defer func() {
		p.telemetry.RecordBlockProcessed(ctx, time.Since(start), retErr)
		if retErr != nil {
			p.telemetry.RecordError(ctx, "processBlockEvent", retErr)
		}
	}()

	receiptsJSON, err := p.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		// The fallback reader already consulted S3, so a nil here means neither
		// tier has the block. Error (not skip) so the SQS message is redelivered
		// until a backfill populates it.
		return fmt.Errorf("receipts not found in cache or S3 for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return fmt.Errorf("unmarshalling receipts: %w", err)
	}

	if err := p.handle(ctx, event, receipts); err != nil {
		return fmt.Errorf("handling block %d (chain=%d, version=%d): %w", event.BlockNumber, event.ChainID, event.Version, err)
	}
	return nil
}
