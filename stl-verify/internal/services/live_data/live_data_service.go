package live_data

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const (
	// tracerName is the instrumentation name for this service.
	tracerName = "github.com/archon-research/stl/stl-verify/internal/services/live_data"
)

// LiveConfig holds configuration for the LiveService.
type LiveConfig struct {
	// ChainID is the blockchain chain ID (e.g., 1 for Ethereum mainnet).
	ChainID int64

	// FinalityBlockCount is how many blocks behind the tip before considered finalized.
	// This also limits how far back reorg detection will look for orphaned blocks.
	FinalityBlockCount int

	// EnableTraces enables fetching execution traces (trace_block on supported nodes).
	EnableTraces bool

	// EnableBlobs enables fetching blob sidecars (post-Dencun blocks on supported nodes).
	EnableBlobs bool

	// Logger is the structured logger.
	Logger *slog.Logger

	// Metrics is the metrics recorder for telemetry (optional).
	Metrics outbound.ReorgRecorder
}

// LiveConfigDefaults returns default configuration.
func LiveConfigDefaults() LiveConfig {
	return LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 64,
		Logger:             slog.Default(),
	}
}

// LightBlock represents a minimal block for chain tracking.
type LightBlock struct {
	Number     int64
	Hash       string
	ParentHash string
}

// prefetchResult holds the result of a prefetched RPC call.
// This enables pipelining: we can start fetching the next block's data
// while still processing the current block's cache/publish operations.
type prefetchResult struct {
	header    outbound.BlockHeader
	blockNum  int64
	blockData outbound.BlockData
	err       error
	fetchedAt time.Time
}

// LiveService handles live block subscription, reorg detection, and event publishing.
// It does NOT handle backfilling - that's the responsibility of BackfillService.
//
// Reorg detection uses the database as the source of truth, querying for the latest
// canonical block and walking back through parent hashes to find common ancestors.
// This ensures consistency with blocks added by other services (e.g., BackfillService).
type LiveService struct {
	config LiveConfig

	subscriber outbound.BlockSubscriber
	client     outbound.BlockchainClient
	stateRepo  outbound.BlockStateRepository
	cache      outbound.BlockCache
	eventSink  outbound.EventSink
	metrics    outbound.ReorgRecorder

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewLiveService creates a new LiveService.
func NewLiveService(
	config LiveConfig,
	subscriber outbound.BlockSubscriber,
	client outbound.BlockchainClient,
	stateRepo outbound.BlockStateRepository,
	cache outbound.BlockCache,
	eventSink outbound.EventSink,
) (*LiveService, error) {
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber is required")
	}
	if client == nil {
		return nil, fmt.Errorf("client is required")
	}
	if stateRepo == nil {
		return nil, fmt.Errorf("stateRepo is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("cache is required")
	}
	if eventSink == nil {
		return nil, fmt.Errorf("eventSink is required")
	}

	// Apply defaults
	defaults := LiveConfigDefaults()
	if config.ChainID == 0 {
		config.ChainID = defaults.ChainID
	}
	if config.FinalityBlockCount == 0 {
		config.FinalityBlockCount = defaults.FinalityBlockCount
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &LiveService{
		config:     config,
		subscriber: subscriber,
		client:     client,
		stateRepo:  stateRepo,
		cache:      cache,
		eventSink:  eventSink,
		metrics:    config.Metrics,
		logger:     config.Logger.With("component", "live-service"),
	}, nil
}

// Start begins watching for new blocks.
func (s *LiveService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	// Subscribe to new block headers
	headers, err := s.subscriber.Subscribe(s.ctx)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %w", err)
	}

	// Process incoming block headers
	go s.processHeaders(headers)

	s.logger.Info("live service started", "chainID", s.config.ChainID)
	return nil
}

// Stop stops the live service.
func (s *LiveService) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	return s.subscriber.Unsubscribe()
}

// processHeaders processes incoming block headers using a prefetch pipeline.
// This overlaps expensive RPC fetches with state operations for better throughput.
//
// Pipeline design:
//
//	Header arrives → Start prefetch (async) → Do state ops → Wait for prefetch → Cache/Publish
//
// Without prefetch:  [state: 30ms] → [RPC: 150ms] → [cache: 50ms] = 230ms total
// With prefetch:     [state: 30ms ─────────────────] → [cache: 50ms] = 80ms total
//
// The prefetch completes during state ops, so we only wait ~120ms (150ms - 30ms).
func (s *LiveService) processHeaders(headers <-chan outbound.BlockHeader) {
	for {
		select {
		case <-s.ctx.Done():
			return
		case header, ok := <-headers:
			if !ok {
				return
			}

			receivedAt := time.Now()

			// Process the block with prefetch
			if err := s.processBlockWithPrefetch(header, receivedAt); err != nil {
				blockNum, _ := hexutil.ParseInt64(header.Number)
				s.logger.Warn("failed to process live block",
					"block", blockNum,
					"hash", header.Hash,
					"parentHash", header.ParentHash,
					"error", err)
			}
		}
	}
}

// startPrefetch starts an async RPC fetch for a block's data.
// The provided context should contain trace information so RPC spans are linked to the parent trace.
// Returns a channel that will receive the result when complete.
func (s *LiveService) startPrefetch(ctx context.Context, header outbound.BlockHeader, blockNum int64) <-chan prefetchResult {
	resultCh := make(chan prefetchResult, 1)

	go func() {
		start := time.Now()

		// Fetch all data in a single batched HTTP request
		// Use the provided context which carries trace information
		bd, err := s.client.GetBlockDataByHash(ctx, blockNum, header.Hash, true)

		resultCh <- prefetchResult{
			header:    header,
			blockNum:  blockNum,
			blockData: bd,
			err:       err,
			fetchedAt: time.Now(),
		}
		close(resultCh)

		s.logger.Debug("prefetch completed", "block", blockNum, "duration", time.Since(start))
	}()

	return resultCh
}

// processBlockWithPrefetch processes a block with RPC data being fetched concurrently.
// State operations run while the prefetch happens in the background, then we wait
// for the prefetch and proceed to cache/publish.
//
// Timeline:
//
//	t=0ms:  Start prefetch (async), begin state ops
//	t=30ms: State ops complete, wait for prefetch
//	t=0-150ms: Prefetch running in background
//	t=150ms: Prefetch complete (waited ~120ms if state ops took 30ms)
//	t=200ms: Cache/publish complete
func (s *LiveService) processBlockWithPrefetch(header outbound.BlockHeader, receivedAt time.Time) error {
	blockNum, err := hexutil.ParseInt64(header.Number)
	if err != nil {
		return fmt.Errorf("failed to parse block number: %w", err)
	}

	start := time.Now()
	ctx, span := s.startProcessBlockSpan(blockNum, header)

	// Start prefetching RPC data immediately so it overlaps with state ops.
	prefetchCtx, cancelPrefetch := context.WithCancel(ctx)
	prefetchCh := s.startPrefetch(prefetchCtx, header, blockNum)

	defer func() {
		cancelPrefetch()
		span.SetAttributes(attribute.Int64("block.duration_ms", time.Since(start).Milliseconds()))
		span.End()
		s.logger.Info("processBlock completed", "block", blockNum, "duration", time.Since(start))
	}()

	block := LightBlock{
		Number:     blockNum,
		Hash:       normalizeHash(header.Hash),
		ParentHash: normalizeHash(header.ParentHash),
	}

	// === PHASE 1: State operations (runs while prefetch is in progress) ===
	result, err := s.runStateOps(ctx, span, block, header, receivedAt)
	if err != nil {
		return err
	}
	if result.skip {
		return nil
	}
	span.SetAttributes(attribute.Int64("block.state_ops_ms", time.Since(start).Milliseconds()))

	// === PHASE 2: Wait for prefetch (should be mostly done by now) ===
	prefetch, err := s.awaitPrefetch(ctx, span, prefetchCh, blockNum)
	if err != nil {
		return err
	}

	// === PHASE 3: Cache and publish ===
	if err := s.cacheAndPublishBlockData(ctx, header, blockNum, result.version, receivedAt, result.isReorg, prefetch.blockData); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to cache and publish block data")
		// Log at ERROR level: block is saved to DB but NOT published.
		// The backfill service will retry, but this needs operator attention.
		s.logger.Error("block saved to DB but cache/publish FAILED - block_published=false, will be retried by backfill",
			"block", blockNum,
			"hash", header.Hash,
			"version", result.version,
			"error", err)
		return fmt.Errorf("failed to cache and publish block data: %w", err)
	}

	return nil
}

// startProcessBlockSpan opens the parent span for the block-processing pipeline.
// All prefetch RPC spans become children of this span.
func (s *LiveService) startProcessBlockSpan(blockNum int64, header outbound.BlockHeader) (context.Context, trace.Span) {
	tracer := otel.Tracer(tracerName)
	return tracer.Start(s.ctx, "live.processBlock",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", header.Hash),
			attribute.String("block.parent_hash", header.ParentHash),
			attribute.Bool("block.prefetched", true),
		),
	)
}

// stateOpsResult is the outcome of Phase 1 (state operations) — see runStateOps.
type stateOpsResult struct {
	// version is the version assigned to the block by the state repository.
	// Only meaningful when skip is false.
	version int

	// isReorg is true when the block was processed as part of a reorg path.
	// Used downstream when publishing the BlockEvent.
	isReorg bool

	// skip indicates the block was dropped (duplicate, stale-fork, or
	// verification error) and the caller must short-circuit without entering
	// the prefetch-wait or cache/publish phases.
	skip bool
}

// runStateOps performs the Phase-1 state operations for an incoming block:
// duplicate check → reorg decision (with RPC verification) → persist.
// Returns skip=true when the block must be dropped without further processing.
func (s *LiveService) runStateOps(ctx context.Context, span trace.Span, block LightBlock, header outbound.BlockHeader, receivedAt time.Time) (stateOpsResult, error) {
	// Dedup: skip blocks we've already seen by hash.
	isDup, err := s.isDuplicateBlock(ctx, block.Hash, block.Number)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to check for duplicate block")
		return stateOpsResult{}, fmt.Errorf("failed to check for duplicate block: %w", err)
	}
	if isDup {
		span.SetAttributes(attribute.Bool("block.duplicate", true))
		return stateOpsResult{skip: true}, nil
	}

	// Decide whether this block is a save, a reorg, or a drop.
	decision, err := s.decideReorgAction(ctx, span, block, receivedAt)
	if err != nil {
		return stateOpsResult{}, err
	}
	if decision.action == reorgActionDrop {
		return stateOpsResult{skip: true}, nil
	}

	// Persist the block (SaveBlock or HandleReorgAtomic).
	state, err := buildBlockState(block, header, receivedAt)
	if err != nil {
		return stateOpsResult{}, err
	}
	version, err := s.persistBlockState(ctx, span, state, decision)
	if err != nil {
		return stateOpsResult{}, err
	}
	return stateOpsResult{
		version: version,
		isReorg: decision.action == reorgActionReorg,
	}, nil
}

// reorgAction is the outcome of the reorg-decision phase: save the block
// canonically, run a reorg, or drop the block (stale fork / verification error).
type reorgAction int

const (
	reorgActionSave reorgAction = iota
	reorgActionReorg
	reorgActionDrop
)

// reorgDecision holds the action plus any reorg metadata required to commit.
type reorgDecision struct {
	action         reorgAction
	commonAncestor int64
	reorgEvent     *outbound.ReorgEvent
	depth          int
}

// decideReorgAction runs detectReorg and, when a reorg is detected, gates the
// commit on RPC verification (VEC-202): the incoming block's hash must equal
// RPC's canonical hash at this number. A stale-fork broadcast or transient RPC
// error returns reorgActionDrop so the caller bails without mutating state.
//
// To close the TOCTOU window the verification round-trip introduces, we
// snapshot `latestBlock` immediately after detectReorg (the value the
// reorgEvent was effectively computed against) and re-read it after verify.
// If a concurrent writer (e.g. BackfillService) committed to block_states
// during the RPC round-trip, the snapshot will differ from the post-verify
// read and we drop the reorg — the next live broadcast will trigger a fresh
// detect→verify→commit pass against the new state.
func (s *LiveService) decideReorgAction(ctx context.Context, span trace.Span, block LightBlock, receivedAt time.Time) (reorgDecision, error) {
	isReorg, depth, commonAncestor, reorgEvent, err := s.detectReorg(ctx, block, receivedAt)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "reorg detection failed")
		return reorgDecision{}, fmt.Errorf("reorg detection failed: %w", err)
	}
	if !isReorg {
		return reorgDecision{action: reorgActionSave}, nil
	}

	// Span attributes describe the detected (provisional) reorg so traces show
	// the full picture even when the block is later dropped. The "reorg
	// detected" log line and the chain.reorgs.total metric, however, only fire
	// after every gate passes — otherwise stale-fork broadcasts and verify
	// failures inflate the dashboards/alerts that operators use to spot real
	// chain reorganizations.
	span.SetAttributes(
		attribute.Bool("block.reorg", true),
		attribute.Int("block.reorg_depth", depth),
		attribute.Int64("block.common_ancestor", commonAncestor),
	)

	preVerifyLatest, err := s.stateRepo.GetLastBlock(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "snapshot latestBlock failed")
		return reorgDecision{}, fmt.Errorf("snapshot latestBlock pre-verify: %w", err)
	}

	canonical, verr := s.verifyIncomingIsCanonical(ctx, block.Number, block.Hash)
	if verr != nil {
		span.SetAttributes(attribute.Bool("block.dropped_verify_error", true))
		s.logger.Warn("dropping reorg block: canonical verification failed",
			"block", block.Number,
			"hash", block.Hash,
			"error", verr)
		s.recordReorgDropped(ctx, outbound.ReorgDropReasonVerifyError)
		return reorgDecision{action: reorgActionDrop}, nil
	}
	if !canonical {
		span.SetAttributes(attribute.Bool("block.dropped_stale_fork", true))
		s.logger.Info("dropping stale-fork reorg broadcast",
			"block", block.Number,
			"hash", block.Hash,
			"depth", depth,
			"commonAncestor", commonAncestor)
		s.recordReorgDropped(ctx, outbound.ReorgDropReasonStaleFork)
		return reorgDecision{action: reorgActionDrop}, nil
	}

	postVerifyLatest, err := s.stateRepo.GetLastBlock(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "re-read latestBlock failed")
		return reorgDecision{}, fmt.Errorf("re-read latestBlock post-verify: %w", err)
	}
	if !sameBlockSnapshot(preVerifyLatest, postVerifyLatest) {
		span.SetAttributes(attribute.Bool("block.dropped_state_shifted", true))
		s.logger.Info("dropping reorg block: latestBlock shifted during verify",
			"block", block.Number,
			"preNumber", snapshotNumber(preVerifyLatest),
			"postNumber", snapshotNumber(postVerifyLatest),
			"preHash", snapshotHashShort(preVerifyLatest),
			"postHash", snapshotHashShort(postVerifyLatest))
		s.recordReorgDropped(ctx, outbound.ReorgDropReasonStateShifted)
		return reorgDecision{action: reorgActionDrop}, nil
	}

	// All gates passed — this is a real, committed reorg. Emit the
	// detection log and metric here (not earlier) so chain.reorgs.total
	// counts only actual reorganizations, not provisional detections that
	// were dropped on verify or state-shift.
	s.logger.Warn("reorg detected", "block", block.Number, "depth", depth, "commonAncestor", commonAncestor)
	if s.metrics != nil {
		s.metrics.RecordReorg(ctx, depth, commonAncestor, block.Number)
	}

	return reorgDecision{
		action:         reorgActionReorg,
		commonAncestor: commonAncestor,
		reorgEvent:     reorgEvent,
		depth:          depth,
	}, nil
}

// recordReorgDropped emits the chain.reorgs.dropped.total counter with the
// given reason label, no-op when no metrics recorder is configured. The
// reason argument must come from outbound.ReorgDropReason* so dashboards stay
// stable.
func (s *LiveService) recordReorgDropped(ctx context.Context, reason string) {
	if s.metrics == nil {
		return
	}
	s.metrics.RecordReorgDropped(ctx, reason)
}

// sameBlockSnapshot returns true when two BlockState pointers represent the
// same canonical tip — both nil, or both non-nil with matching number and hash.
// Used by decideReorgAction's TOCTOU guard.
func sameBlockSnapshot(a, b *outbound.BlockState) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Number == b.Number && a.Hash == b.Hash
}

// snapshotNumber returns the block number for logging, or -1 when nil.
func snapshotNumber(s *outbound.BlockState) int64 {
	if s == nil {
		return -1
	}
	return s.Number
}

// snapshotHashShort returns a truncated hash for logging, or "<nil>" when nil.
func snapshotHashShort(s *outbound.BlockState) string {
	if s == nil {
		return "<nil>"
	}
	return truncateHashLive(s.Hash)
}

// truncateHashLive returns a shortened hex hash for log lines.
func truncateHashLive(hash string) string {
	if len(hash) > 18 {
		return hash[:10] + "..." + hash[len(hash)-6:]
	}
	return hash
}

// buildBlockState assembles the BlockState row to persist from the normalized
// incoming block + header timestamp.
func buildBlockState(block LightBlock, header outbound.BlockHeader, receivedAt time.Time) (outbound.BlockState, error) {
	blockTimestamp, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		return outbound.BlockState{}, fmt.Errorf("failed to parse block timestamp: %w", err)
	}
	return outbound.BlockState{
		Number:         block.Number,
		Hash:           block.Hash,
		ParentHash:     block.ParentHash,
		ReceivedAt:     receivedAt.Unix(),
		BlockTimestamp: blockTimestamp,
		IsOrphaned:     false,
	}, nil
}

// persistBlockState commits the block state via SaveBlock or HandleReorgAtomic
// depending on the decision and returns the assigned version.
func (s *LiveService) persistBlockState(ctx context.Context, span trace.Span, state outbound.BlockState, decision reorgDecision) (int, error) {
	if decision.action == reorgActionReorg && decision.reorgEvent != nil {
		version, err := s.stateRepo.HandleReorgAtomic(ctx, decision.commonAncestor, *decision.reorgEvent, state)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to handle reorg atomically")
			return 0, fmt.Errorf("failed to handle reorg atomically: %w", err)
		}
		return version, nil
	}
	version, err := s.stateRepo.SaveBlock(ctx, state)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to save block state")
		return 0, fmt.Errorf("failed to save block state: %w", err)
	}
	return version, nil
}

// awaitPrefetch blocks on the prefetch result, records timing, and propagates
// any prefetch error.
func (s *LiveService) awaitPrefetch(ctx context.Context, span trace.Span, prefetchCh <-chan prefetchResult, blockNum int64) (prefetchResult, error) {
	prefetchWaitStart := time.Now()
	var prefetch prefetchResult
	select {
	case prefetch = <-prefetchCh:
	case <-ctx.Done():
		return prefetchResult{}, ctx.Err()
	}
	span.SetAttributes(attribute.Int64("block.prefetch_wait_ms", time.Since(prefetchWaitStart).Milliseconds()))

	if prefetch.err != nil {
		span.RecordError(prefetch.err)
		span.SetStatus(codes.Error, "prefetch failed")
		return prefetchResult{}, fmt.Errorf("failed to fetch block data for block %d: %w", blockNum, prefetch.err)
	}
	return prefetch, nil
}

// isDuplicateBlock checks if a block has already been processed by querying the database.
func (s *LiveService) isDuplicateBlock(ctx context.Context, hash string, blockNum int64) (bool, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.isDuplicateBlock",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", hash),
		),
	)
	defer span.End()

	// Check DB for duplicates (includes blocks added by backfill)
	existing, err := s.stateRepo.GetBlockByHash(ctx, hash)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to check DB for duplicate")
		return false, fmt.Errorf("failed to check DB for duplicate: %w", err)
	}
	if existing != nil {
		span.SetAttributes(attribute.Bool("duplicate", true))
		s.logger.Debug("block already in DB, skipping", "block", blockNum)
		return true, nil
	}

	span.SetAttributes(attribute.Bool("duplicate", false))
	return false, nil
}

// detectReorg detects chain reorganizations by querying the database for the latest
// canonical block and comparing parent hashes.
// Returns: isReorg, reorgDepth, commonAncestor, reorgEvent (if reorg), error.
func (s *LiveService) detectReorg(ctx context.Context, block LightBlock, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.detectReorg",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", block.Number),
			attribute.String("block.hash", block.Hash),
			attribute.String("block.parent_hash", block.ParentHash),
		),
	)
	defer span.End()

	// Query DB for the latest canonical block
	latestBlock, err := s.stateRepo.GetLastBlock(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get latest block from DB")
		return false, 0, 0, nil, fmt.Errorf("failed to get latest block from DB: %w", err)
	}

	// No blocks in DB yet - this is the first block, no reorg possible
	if latestBlock == nil {
		span.SetAttributes(attribute.Bool("reorg.detected", false))
		return false, 0, 0, nil, nil
	}

	span.SetAttributes(
		attribute.Int64("chain.latest_block", latestBlock.Number),
		attribute.String("chain.latest_hash", latestBlock.Hash),
	)

	// Block number decreased or same - possible reorg, OR a late/out-of-order
	// arrival filling a gap. Distinguish the two before treating it as a reorg:
	// blanket-orphaning on a misclassified late arrival is the VEC-277 root
	// cause. A block is a late arrival (NOT a reorg) when no canonical block
	// occupies its height AND it links cleanly onto our canonical chain (its
	// parent is the canonical block at number-1). In that case we save it
	// normally and leave its canonical successors untouched.
	if block.Number <= latestBlock.Number {
		existingAtHeight, err := s.stateRepo.GetBlockByNumber(ctx, block.Number)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to check existing block at height")
			return false, 0, 0, nil, fmt.Errorf("failed to check existing block at height %d: %w", block.Number, err)
		}
		if existingAtHeight == nil {
			parent, err := s.stateRepo.GetBlockByNumber(ctx, block.Number-1)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, "failed to check parent block")
				return false, 0, 0, nil, fmt.Errorf("failed to check parent block %d: %w", block.Number-1, err)
			}
			if parent != nil && parent.Hash == block.ParentHash {
				// Clean late-arrival fill: links onto the canonical chain and
				// no competing block holds this height.
				span.SetAttributes(
					attribute.Bool("reorg.detected", false),
					attribute.Bool("block.late_arrival", true),
				)
				return false, 0, 0, nil, nil
			}
		}
		// A different canonical block holds this height, or the incoming block
		// does not link onto our chain → genuine reorg.
		return s.handleReorg(ctx, block, receivedAt)
	}

	// Block is exactly one ahead - check parent hash
	if block.Number == latestBlock.Number+1 {
		if block.ParentHash == latestBlock.Hash {
			span.SetAttributes(attribute.Bool("reorg.detected", false))
			return false, 0, 0, nil, nil
		}
		// Parent hash mismatch - reorg
		return s.handleReorg(ctx, block, receivedAt)
	}

	// Gap in blocks - will be backfilled by BackfillService
	span.SetAttributes(
		attribute.Bool("reorg.detected", false),
		attribute.Bool("block.gap", true),
	)
	return false, 0, 0, nil, nil
}

// handleReorg processes a detected reorg and returns the reorg event for atomic handling.
// The actual database operations (save reorg event, mark orphans, save new block) are
// done atomically in processBlock via HandleReorgAtomic.
//
// This method queries the database to find the common ancestor, ensuring consistency
// with blocks added by other services (e.g., BackfillService).
func (s *LiveService) handleReorg(ctx context.Context, block LightBlock, receivedAt time.Time) (bool, int, int64, *outbound.ReorgEvent, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.handleReorg",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", block.Number),
			attribute.String("block.hash", block.Hash),
		),
	)
	defer span.End()

	// Get the latest block from DB to determine finality boundary
	latestBlock, err := s.stateRepo.GetLastBlock(ctx)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get latest block")
		return false, 0, 0, nil, fmt.Errorf("failed to get latest block from DB: %w", err)
	}

	// Calculate finality boundary based on latest known block
	var finalityBoundary int64 = 0
	if latestBlock != nil {
		finalityBoundary = max(latestBlock.Number-int64(s.config.FinalityBlockCount), 0)
	}

	// Walk back to find common ancestor.
	// Start with the incoming block (already normalized), then walk to its parent each iteration.
	walkBlock := block

	var commonAncestor int64 = -1
	walkCount := 0
	for walkCount = 0; walkCount < s.config.FinalityBlockCount; walkCount++ {
		// Check if parent exists in our canonical chain (DB query)
		parentInDB, err := s.stateRepo.GetBlockByHash(ctx, walkBlock.ParentHash)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to query parent block")
			return false, 0, 0, nil, fmt.Errorf("failed to query parent block %s from DB: %w", walkBlock.ParentHash, err)
		}
		if parentInDB != nil && !parentInDB.IsOrphaned {
			commonAncestor = parentInDB.Number
			break
		}

		// Check finality boundary
		if walkBlock.Number <= finalityBoundary {
			err := fmt.Errorf("block %d is at or below finality boundary %d (likely late arrival after pruning)",
				walkBlock.Number, finalityBoundary)
			span.RecordError(err)
			span.SetStatus(codes.Error, "block below finality")
			return false, 0, 0, nil, err
		}

		// Fetch parent from network
		parentHeader, err := s.client.GetBlockByHash(ctx, walkBlock.ParentHash, false)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to fetch parent block")
			return false, 0, 0, nil, fmt.Errorf("failed to fetch parent block %s during reorg walk: %w", walkBlock.ParentHash, err)
		}

		// Walk to parent block to continue searching for common ancestor.
		// Normalize RPC response at the point of ingestion.
		parentNum, err := hexutil.ParseInt64(parentHeader.Number)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to parse parent block number")
			return false, 0, 0, nil, fmt.Errorf("failed to parse parent block number %q: %w", parentHeader.Number, err)
		}

		walkBlock = LightBlock{
			Number:     parentNum,
			ParentHash: normalizeHash(parentHeader.ParentHash),
		}
	}

	span.SetAttributes(attribute.Int("reorg.walk_count", walkCount))

	if commonAncestor < 0 {
		err := fmt.Errorf("no common ancestor found after walking %d blocks (chain diverged beyond finality window)", s.config.FinalityBlockCount)
		span.RecordError(err)
		span.SetStatus(codes.Error, "no common ancestor found")
		return false, 0, 0, nil, err
	}

	// Query DB for blocks that will be orphaned (non-orphaned blocks > commonAncestor)
	// We only need to look back FinalityBlockCount blocks since reorgs can't go deeper
	recentBlocks, err := s.stateRepo.GetRecentBlocks(ctx, s.config.FinalityBlockCount)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get recent blocks")
		return false, 0, 0, nil, fmt.Errorf("failed to get recent blocks from DB: %w", err)
	}

	orphanedBlocks := make([]LightBlock, 0)
	for i := len(recentBlocks) - 1; i >= 0; i-- {
		if recentBlocks[i].Number > commonAncestor {
			orphanedBlocks = append(orphanedBlocks, LightBlock{
				Number:     recentBlocks[i].Number,
				Hash:       recentBlocks[i].Hash,
				ParentHash: recentBlocks[i].ParentHash,
			})
		}
	}
	reorgDepth := len(orphanedBlocks)

	span.SetAttributes(
		attribute.Bool("reorg.detected", true),
		attribute.Int("reorg.depth", reorgDepth),
		attribute.Int64("reorg.common_ancestor", commonAncestor),
		attribute.Int("reorg.orphaned_count", len(orphanedBlocks)),
	)

	// Build reorg event to be saved atomically with the new block
	// We always create a reorg event when a reorg is detected, even if depth is 0
	// (depth can be 0 when the incoming block is ahead of our chain but on a different fork)
	var reorgEvent *outbound.ReorgEvent
	if len(orphanedBlocks) > 0 {
		// Use the first orphaned block (closest to incoming block number) for OldHash
		reorgEvent = &outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: block.Number,
			OldHash:     orphanedBlocks[0].Hash, // Most recent orphaned block
			NewHash:     block.Hash,
			Depth:       reorgDepth,
		}
	} else {
		// No blocks orphaned, but we still detected a chain divergence
		// This can happen when the incoming block is ahead but on a different fork
		// We still need to record this as a reorg event
		reorgEvent = &outbound.ReorgEvent{
			DetectedAt:  receivedAt,
			BlockNumber: block.Number,
			OldHash:     "", // No block orphaned at this exact number
			NewHash:     block.Hash,
			Depth:       0,
		}
	}

	return true, reorgDepth, commonAncestor, reorgEvent, nil
}

// verifyIncomingIsCanonical asks RPC whether the incoming block's hash is the
// canonical hash at its number. Returns:
//
//   - (true,  nil)  : RPC confirms the incoming hash is canonical → real reorg, proceed.
//   - (false, nil)  : RPC's canonical hash differs → stale-fork broadcast, drop the block.
//   - (false, err)  : RPC lookup, parse, or response-shape failure → caller should
//     drop the block to be safe; the next live block on the
//     canonical chain will trigger another detection once RPC
//     stabilises. An empty/absent canonical hash (RPC `null`,
//     malformed header, etc.) is intentionally classified as an
//     error here, NOT a stale fork — silently downgrading these
//     to stale_fork would conflate transient RPC inconsistency
//     with the genuine fork-loss signal we alert on.
//
// Every call site that mutates state via HandleReorgAtomic must be gated by this
// check, otherwise a stale-fork broadcast over-orphans canonical blocks (creating
// phantom-orphan rows that no later flow can heal — see VEC-202).
//
// IMPORTANT: this function MUST hit the upstream JSON-RPC endpoint on every
// invocation. If the BlockchainClient is ever extended to read GetBlockByNumber
// through a cache populated by the same broadcast pipeline (e.g., a
// number-keyed pre-fetch cache), this verification would be checking the
// broadcast against itself and the safety property silently disappears. The
// current alchemy adapter (internal/adapters/outbound/alchemy/client.go)
// dispatches directly via JSON-RPC; preserve that contract for any future
// implementation, or introduce a dedicated uncached method on the port.
func (s *LiveService) verifyIncomingIsCanonical(ctx context.Context, blockNum int64, incomingHash string) (bool, error) {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.verifyIncomingIsCanonical",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", incomingHash),
		),
	)
	defer span.End()

	blockJSON, err := s.client.GetBlockByNumber(ctx, blockNum, false)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "canonical lookup failed")
		return false, fmt.Errorf("canonical lookup for block %d: %w", blockNum, err)
	}

	var canonicalHeader outbound.BlockHeader
	if err := shared.ParseBlockHeader(blockJSON, &canonicalHeader); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "canonical parse failed")
		return false, fmt.Errorf("parse canonical block %d: %w", blockNum, err)
	}

	canonicalHash := normalizeHash(canonicalHeader.Hash)
	if canonicalHash == "" {
		// RPC returned an unusable response: JSON-RPC `null` (json.Unmarshal of
		// "null" leaves the struct zero-valued), an empty header object, or a
		// header whose hash field is genuinely empty. Surface this as a
		// verification error so the caller records `verify_error` rather than
		// silently misattributing the failure to `stale_fork`.
		err := fmt.Errorf("canonical block %d has empty hash (likely RPC null result or malformed header)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "canonical hash missing")
		return false, err
	}
	span.SetAttributes(attribute.String("canonical.hash", canonicalHash))
	if canonicalHash != incomingHash {
		span.SetAttributes(attribute.Bool("verify.stale_fork", true))
		return false, nil
	}
	return true, nil
}

func (s *LiveService) publishBlockEvent(ctx context.Context, chainID, blockNum int64, version int, blockHash, parentHash string, blockTimestamp int64, receivedAt time.Time, isReorg bool) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.publishBlockEvent",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", blockHash),
			attribute.Int64("chain.id", chainID),
			attribute.Int("block.version", version),
			attribute.Bool("block.is_reorg", isReorg),
		),
	)
	defer span.End()

	event := outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNum,
		Version:        version,
		BlockHash:      blockHash,
		ParentHash:     parentHash,
		BlockTimestamp: blockTimestamp,
		ReceivedAt:     receivedAt,
		IsReorg:        isReorg,
		IsBackfill:     false,
	}

	snsStart := time.Now()
	if err := s.eventSink.Publish(ctx, event); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to publish block event")
		return fmt.Errorf("failed to publish block event for block %d: %w", blockNum, err)
	}
	snsDuration := time.Since(snsStart)
	span.SetAttributes(attribute.Int64("sns.duration_ms", snsDuration.Milliseconds()))

	// Mark block publish as complete in DB for crash recovery.
	// Note: If this fails, the block remains marked as "unpublished" in the DB, and the
	// backfill service will retry publishing it later. This can result in duplicate publishes
	// to SNS/SQS. Downstream consumers MUST be idempotent to handle this correctly.
	// We intentionally don't fail here because the publish already succeeded - failing would
	// be worse as we'd lose the block entirely.
	dbStart := time.Now()
	if err := s.stateRepo.MarkPublishComplete(ctx, blockHash); err != nil {
		s.logger.Warn("failed to mark block publish complete", "block", blockNum, "error", err)
	}
	dbDuration := time.Since(dbStart)
	span.SetAttributes(attribute.Int64("db.mark_complete_duration_ms", dbDuration.Milliseconds()))

	s.logger.Debug("published block event", "block", blockNum, "sns_ms", snsDuration.Milliseconds(), "db_mark_ms", dbDuration.Milliseconds())

	return nil
}

// cacheAndPublishBlockData caches prefetched block data and publishes the event.
// This is the optimized version that skips the RPC fetch (data already prefetched).
func (s *LiveService) cacheAndPublishBlockData(ctx context.Context, header outbound.BlockHeader, blockNum int64, version int, receivedAt time.Time, isReorg bool, bd outbound.BlockData) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "live.cacheAndPublishBlockData",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.String("block.hash", header.Hash),
			attribute.Int("block.version", version),
		),
	)
	defer span.End()

	chainID := s.config.ChainID
	blockHash := header.Hash
	parentHash := header.ParentHash
	blockTimestamp, err := hexutil.ParseInt64(header.Timestamp)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to parse block timestamp")
		return fmt.Errorf("failed to parse block timestamp %q for block %d: %w", header.Timestamp, blockNum, err)
	}

	// Check for fetch errors before caching
	if bd.BlockErr != nil {
		span.RecordError(bd.BlockErr)
		span.SetStatus(codes.Error, "block fetch error")
		return fmt.Errorf("failed to fetch block %d: %w", blockNum, bd.BlockErr)
	}
	if bd.ReceiptsErr != nil {
		span.RecordError(bd.ReceiptsErr)
		span.SetStatus(codes.Error, "receipts fetch error")
		return fmt.Errorf("failed to fetch receipts for block %d: %w", blockNum, bd.ReceiptsErr)
	}
	if s.config.EnableTraces && bd.TracesErr != nil {
		span.RecordError(bd.TracesErr)
		span.SetStatus(codes.Error, "traces fetch error")
		return fmt.Errorf("failed to fetch traces for block %d: %w", blockNum, bd.TracesErr)
	}
	if s.config.EnableBlobs && bd.BlobsErr != nil {
		span.RecordError(bd.BlobsErr)
		span.SetStatus(codes.Error, "blobs fetch error")
		return fmt.Errorf("failed to fetch blobs for block %d: %w", blockNum, bd.BlobsErr)
	}

	// Verify all required data is present and non-null. The `[]byte("null")`
	// case is the VEC-242 regression: an upstream JSON-RPC null response
	// surfaced as a non-nil 4-byte slice and slipped past the prior nil-check.
	if rpcutil.IsNullOrEmpty(bd.Block) {
		err := fmt.Errorf("missing block data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing block data")
		return err
	}
	if rpcutil.IsNullOrEmpty(bd.Receipts) {
		err := fmt.Errorf("missing receipts data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing receipts data")
		return err
	}
	if s.config.EnableTraces && rpcutil.IsNullOrEmpty(bd.Traces) {
		err := fmt.Errorf("missing traces data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing traces data")
		return err
	}
	if s.config.EnableBlobs && rpcutil.IsNullOrEmpty(bd.Blobs) {
		err := fmt.Errorf("missing blobs data for block %d (no error reported)", blockNum)
		span.RecordError(err)
		span.SetStatus(codes.Error, "missing blobs data")
		return err
	}

	// Cache all data types - create a child span for the cache operation
	cacheCtx, cacheSpan := tracer.Start(ctx, "live.cacheBlockData",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("block.number", blockNum),
			attribute.Int64("chain.id", chainID),
			attribute.Int("block.version", version),
			attribute.Bool("blobs.enabled", s.config.EnableBlobs),
		),
	)
	cacheStart := time.Now()

	// Cache all data types in a single pipelined operation (single network round-trip)
	cacheInput := outbound.BlockDataInput{
		Block:    bd.Block,
		Receipts: bd.Receipts,
	}
	if s.config.EnableTraces {
		cacheInput.Traces = bd.Traces
	}
	if s.config.EnableBlobs {
		cacheInput.Blobs = bd.Blobs
	}

	if err := s.cache.SetBlockData(cacheCtx, chainID, blockNum, version, cacheInput); err != nil {
		cacheSpan.RecordError(err)
		cacheSpan.SetStatus(codes.Error, "failed to cache block data")
		cacheSpan.End()
		return fmt.Errorf("failed to cache block data for block %d: %w", blockNum, err)
	}

	cacheDuration := time.Since(cacheStart)
	cacheSpan.SetAttributes(attribute.Int64("cache.duration_ms", cacheDuration.Milliseconds()))
	cacheSpan.End()
	s.logger.Debug("cached all block data", "block", blockNum, "cache_ms", cacheDuration.Milliseconds())

	// All data cached successfully - now publish the block event
	return s.publishBlockEvent(ctx, chainID, blockNum, version, blockHash, parentHash, blockTimestamp, receivedAt, isReorg)
}

// normalizeHash normalizes a hex hash to lowercase for consistent comparisons.
// Ethereum hashes are case-insensitive (0xAAA == 0xaaa), but Go string comparison
// is case-sensitive. Normalizing to lowercase prevents false mismatches.
func normalizeHash(hash string) string {
	return strings.ToLower(hash)
}
