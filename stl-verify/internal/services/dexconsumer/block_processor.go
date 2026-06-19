// Package dexconsumer holds the SQS-consumer scaffolding shared by the three
// DEX workers (curve, uniswap-v3, balancer). The per-protocol decode/dispatch
// logic stays in each worker; only the protocol-agnostic plumbing lives here:
// the block-event handler, protocol-id resolution, protocol_event persistence,
// and dependency validation. Extracting it (review S14) keeps each per-DEX
// worker PR small and removes the ~110-line copy that would otherwise land
// three times.
package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ReceiptHandler decodes and persists one transaction receipt. It is the single
// protocol-specific seam in the shared block handler: each DEX worker supplies
// its own (matching on its pool/factory addresses, decoding its event set).
type ReceiptHandler func(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, version int, blockTimestamp time.Time) error

// BlockProcessor turns a cached block into per-receipt work. It owns the
// protocol-agnostic steps every DEX worker repeats: read receipts from the cache
// (with S3 fallback), decode them, fan out to the protocol's ReceiptHandler, and
// record telemetry.
type BlockProcessor struct {
	cache          outbound.BlockCacheReader
	telemetry      *dextelemetry.Telemetry
	processReceipt ReceiptHandler
}

// NewBlockProcessor wires the shared block handler. cache must be the
// S3-fallback reader (dexbootstrap.Deps.CacheReader, built by
// cache.NewReaderWithFallback), so a Redis miss is served transparently from the
// S3 archive; a nil receipts result then means the block is missing from both
// tiers, not merely from Redis. telemetry may be nil (its methods are nil-safe),
// e.g. when metrics are disabled.
func NewBlockProcessor(cache outbound.BlockCacheReader, telemetry *dextelemetry.Telemetry, processReceipt ReceiptHandler) *BlockProcessor {
	return &BlockProcessor{cache: cache, telemetry: telemetry, processReceipt: processReceipt}
}

// ProcessBlockEvent is the sqsutil.BlockEventHandler for a DEX worker. Per-receipt
// errors are collected, not short-circuited, so one undecodable receipt does not
// hide work in the rest of the block; a non-nil return leaves the SQS message
// undeleted for redelivery.
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

	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()

	var errs []error
	for _, r := range receipts {
		if err := p.processReceipt(ctx, r, event.ChainID, event.BlockNumber, event.Version, blockTimestamp); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
