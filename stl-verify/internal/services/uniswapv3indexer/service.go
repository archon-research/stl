package uniswapv3indexer

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// UniswapV3ServiceDeps groups the UniswapV3Service's constructor arguments.
// No doc comments on self-evident fields: the field names explain themselves.
// Telemetry is optional (nil = no-op); Pools may be nil/empty (a worker with
// no registered pools yet is a valid, if quiet, boot state).
type UniswapV3ServiceDeps struct {
	Pools       []RegisteredPool
	Multicaller outbound.Multicaller
	Repo        outbound.UniswapV3Repository
	EventWriter *dexconsumer.ProtocolEventWriter
	TxManager   outbound.TxManager
	ChainID     int64
	Logger      *slog.Logger
	Telemetry   *dextelemetry.Telemetry
}

// UniswapV3Service drives per-block event decoding and transactional
// persistence for the Uniswap V3 indexer.
//
// Single-goroutine contract: sqsutil.RunLoop processes one SQS message at a
// time, so no synchronisation is required on any UniswapV3Service field. All
// per-block work happens in local variables inside handleBlock; the only
// cross-block state is the pool registry, the snapshot tracker (deploy-gate
// tracking; sweepBlocks=0 disables the periodic-sweep half of it, see
// NewUniswapV3Service), and baselineSeen (which pools' initialized ticks have
// already been enumerated once).
type UniswapV3Service struct {
	poolsByAddr map[common.Address]RegisteredPool
	pools       []RegisteredPool // ordered for deterministic iteration
	multicaller outbound.Multicaller
	repo        outbound.UniswapV3Repository
	eventWriter *dexconsumer.ProtocolEventWriter
	txMgr       outbound.TxManager
	chainID     int64
	logger      *slog.Logger
	telemetry   *dextelemetry.Telemetry

	tracker      *dexconsumer.SnapshotTracker
	baselineSeen map[int64]bool
}

// validate checks that every required dependency is present, so
// NewUniswapV3Service reads as its build steps rather than a wall of nil
// guards.
func (d UniswapV3ServiceDeps) validate() error {
	switch {
	case d.Multicaller == nil:
		return fmt.Errorf("multicaller is required")
	case d.Repo == nil:
		return fmt.Errorf("repo is required")
	case d.EventWriter == nil:
		return fmt.Errorf("eventWriter is required")
	case d.TxManager == nil:
		return fmt.Errorf("txManager is required")
	case d.Logger == nil:
		return fmt.Errorf("logger is required")
	}
	return nil
}

// NewUniswapV3Service validates deps and builds a UniswapV3Service. The
// snapshot tracker is built with sweepBlocks=0: V3 pool state is
// piecewise-constant between touches (slot0/liquidity/fee-growth only change
// when a Swap/Mint/Burn/Collect/Flash/etc. log fires), so unlike Curve there
// is no periodic heartbeat snapshot to keep quiet pools fresh — only touched
// pools are ever due (design VEC-261 §6.1).
func NewUniswapV3Service(deps UniswapV3ServiceDeps) (*UniswapV3Service, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}

	return &UniswapV3Service{
		poolsByAddr:  indexPoolsByAddress(deps.Pools),
		pools:        deps.Pools,
		multicaller:  deps.Multicaller,
		repo:         deps.Repo,
		eventWriter:  deps.EventWriter,
		txMgr:        deps.TxManager,
		chainID:      deps.ChainID,
		logger:       deps.Logger,
		telemetry:    deps.Telemetry,
		tracker:      dexconsumer.NewSnapshotTracker(0),
		baselineSeen: make(map[int64]bool),
	}, nil
}

// BlockHandler returns the dexconsumer.BlockHandler for this service. It
// records uniswap_v3_errors_total once, at this boundary, on any non-nil
// handler return: handleBlock's inner steps wrap their errors with stage
// context (for the logs) but do not touch the counter, so no future error
// path can silently skip the metric. RecordError is a no-op on a nil error or
// nil telemetry.
func (s *UniswapV3Service) BlockHandler() dexconsumer.BlockHandler {
	return func(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error {
		if err := s.handleBlock(ctx, event, receipts); err != nil {
			s.telemetry.RecordError(ctx, "blockHandler", err)
			return err
		}
		return nil
	}
}

// handleBlock decodes every receipt in the block, snapshots the touched
// pools' state and tick rows via multicall (before opening the transaction),
// and persists swaps, liquidity events, pool events, state, ticks, and
// captured logs in one transaction. Returning a non-nil error leaves the
// block for SQS redelivery; nil is returned only after a successful commit.
// All per-block state is local, so a redelivery reprocesses from scratch with
// no carryover.
func (s *UniswapV3Service) handleBlock(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error {
	bn := event.BlockNumber
	ver := event.Version
	ts := time.Unix(event.BlockTimestamp, 0).UTC()

	// common.HexToHash never errors: an empty string would silently become the
	// zero hash and reach the RPC as a real eth_call. Both producers always
	// populate BlockHash (it's part of the dedup key), so this guards a
	// malformed message rather than an expected path.
	if event.BlockHash == "" {
		return fmt.Errorf("block %d v%d: missing block hash on event", bn, ver)
	}
	blockHash := common.HexToHash(event.BlockHash)

	acc, err := s.decodeBlockEvents(ctx, receipts, bn, ver, ts)
	if err != nil {
		return err
	}

	dueSet, err := dexconsumer.DueSet(s.tracker, s.pools, acc.touchedIDs, bn, ver)
	if err != nil {
		return err
	}

	// Read pool state and tick data via multicall BEFORE opening the
	// transaction so archive-RPC latency never pins a pgx connection
	// (connection-pool exhaustion is a stall cause).
	states, ticks, baselined, err := s.snapshotDueSet(ctx, dueSet, acc, blockHash, bn, ver, ts)
	if err != nil {
		return err
	}

	if !acc.hasEvents() && len(states) == 0 && len(ticks) == 0 {
		return nil
	}

	writes, capturedIns := s.buildBlockWrites(acc, states, ticks, bn, ver, ts)

	stateRows, err := s.persistBlock(ctx, writes, capturedIns, bn)
	if err != nil {
		return err
	}

	s.markSnapshotted(dueSet, baselined, bn, ver)
	s.telemetry.RecordStateRows(ctx, int(stateRows))
	return nil
}

// blockAccumulators holds every event decoded from a single block's receipts,
// keyed for the downstream due-set and write-building steps.
type blockAccumulators struct {
	swaps      []*entity.UniswapV3Swap
	liquidity  []*entity.UniswapV3LiquidityEvent
	poolEvts   []*entity.UniswapV3PoolEvent
	captured   []CapturedLog
	touchedIDs map[int64]bool
	liqByPool  map[int64][]*entity.UniswapV3LiquidityEvent
}

func (acc blockAccumulators) hasEvents() bool {
	return len(acc.swaps) > 0 || len(acc.liquidity) > 0 || len(acc.poolEvts) > 0 || len(acc.captured) > 0
}

// decodeBlockEvents iterates every receipt, identifies every pool touched by
// each receipt's logs (matched via poolsByAddr), decodes each one's events, and
// accumulates them plus the touched-pool-ID set. ctx is checked at the start
// of each receipt to honour cancellation without silently acking a
// partially-decoded block.
func (s *UniswapV3Service) decodeBlockEvents(ctx context.Context, receipts []shared.TransactionReceipt, bn int64, ver int, ts time.Time) (blockAccumulators, error) {
	acc := blockAccumulators{
		touchedIDs: make(map[int64]bool),
		liqByPool:  make(map[int64][]*entity.UniswapV3LiquidityEvent),
	}
	for _, receipt := range receipts {
		// Bail early (with an error, never a silent ack) if the handler-timeout
		// budget or a shutdown cancelled ctx, rather than decoding the rest.
		if err := ctx.Err(); err != nil {
			return blockAccumulators{}, err
		}
		pools, err := s.poolsTouchedByReceipt(receipt)
		if err != nil {
			return blockAccumulators{}, err
		}
		for _, pool := range pools {
			decoded, err := DecodeEvents(receipt, pool, s.chainID, bn, ver, ts)
			if err != nil {
				return blockAccumulators{}, fmt.Errorf("decoding events for pool %s block %d: %w", pool.Address, bn, err)
			}
			acc.swaps = append(acc.swaps, decoded.Swaps...)
			acc.liquidity = append(acc.liquidity, decoded.LiquidityEvents...)
			acc.poolEvts = append(acc.poolEvts, decoded.PoolEvents...)
			acc.captured = append(acc.captured, decoded.Captured...)
			acc.liqByPool[pool.ID] = append(acc.liqByPool[pool.ID], decoded.LiquidityEvents...)
			acc.touchedIDs[pool.ID] = true
		}
	}
	return acc, nil
}

// poolsTouchedByReceipt returns the deduplicated registered pools whose address
// appears on any of the receipt's logs, in deterministic pool-ID order. A single
// transaction can touch more than one seeded pool (e.g. an aggregator splitting a
// route across two fee tiers of the same pair), so every matched pool must be
// decoded; a pool touched by several logs is returned once (DecodeEvents scans
// all logs and filters to the pool's own address).
func (s *UniswapV3Service) poolsTouchedByReceipt(receipt shared.TransactionReceipt) ([]RegisteredPool, error) {
	byID := make(map[int64]RegisteredPool)
	for _, log := range receipt.Logs {
		if !common.IsHexAddress(log.Address) {
			return nil, fmt.Errorf("invalid log address %q", log.Address)
		}
		if pool, ok := s.poolsByAddr[common.HexToAddress(log.Address)]; ok {
			byID[pool.ID] = pool
		}
	}
	pools := make([]RegisteredPool, 0, len(byID))
	for _, pool := range byID {
		pools = append(pools, pool)
	}
	sort.Slice(pools, func(i, j int) bool { return pools[i].ID < pools[j].ID })
	return pools, nil
}

// snapshotDueSet reads each due pool's state and tick rows via multicall,
// pinned to blockHash so the read cannot silently answer from a post-reorg
// fork. It must run BEFORE the DB transaction opens (see handleBlock doc).
// Returns the pool IDs whose baseline ticks were read this call, so the
// caller can mark baselineSeen only after a successful persist.
func (s *UniswapV3Service) snapshotDueSet(ctx context.Context, dueSet []RegisteredPool, acc blockAccumulators, blockHash common.Hash, bn int64, ver int, ts time.Time) ([]*entity.UniswapV3PoolState, []*entity.UniswapV3Tick, []int64, error) {
	var states []*entity.UniswapV3PoolState
	var ticks []*entity.UniswapV3Tick
	var baselined []int64

	for _, pool := range dueSet {
		state, err := SnapshotState(ctx, s.multicaller, pool, blockHash, bn, ver, ts)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("snapshotting pool %s block %d: %w", pool.Address, bn, err)
		}
		states = append(states, state)

		poolTicks, isFirstSeen, err := s.snapshotPoolTicks(ctx, pool, blockHash, bn, ver, ts, acc.liqByPool[pool.ID])
		if err != nil {
			return nil, nil, nil, err
		}
		ticks = append(ticks, poolTicks...)
		if isFirstSeen {
			baselined = append(baselined, pool.ID)
		}
	}
	return states, ticks, baselined, nil
}

// snapshotPoolTicks reads pool's touched ticks (the bounds of this block's
// liquidity events) plus, on the pool's first-ever touch, every currently
// initialized tick (the baseline enumeration). Returning isFirstSeen lets the
// caller defer marking baselineSeen until after a successful persist, so a
// failed block re-enumerates the baseline on redelivery instead of silently
// skipping it forever.
func (s *UniswapV3Service) snapshotPoolTicks(ctx context.Context, pool RegisteredPool, blockHash common.Hash, bn int64, ver int, ts time.Time, liqEvents []*entity.UniswapV3LiquidityEvent) ([]*entity.UniswapV3Tick, bool, error) {
	touched := TouchedTicks(DecodedEvents{LiquidityEvents: liqEvents})

	isFirstSeen := !s.baselineSeen[pool.ID]
	ticksToRead := touched
	if isFirstSeen {
		baseline, err := BaselineTicks(ctx, s.multicaller, pool, blockHash)
		if err != nil {
			return nil, false, fmt.Errorf("enumerating baseline ticks for pool %s block %d: %w", pool.Address, bn, err)
		}
		ticksToRead = mergeTickSets(touched, baseline)
	}

	rows, err := s.readTicks(ctx, pool, blockHash, bn, ver, ts, ticksToRead)
	if err != nil {
		return nil, false, err
	}
	return rows, isFirstSeen, nil
}

// readTicks issues one ticks() multicall batch for the given tick positions
// and decodes every result into an authoritative entity.UniswapV3Tick.
func (s *UniswapV3Service) readTicks(ctx context.Context, pool RegisteredPool, blockHash common.Hash, bn int64, ver int, ts time.Time, ticksToRead []int32) ([]*entity.UniswapV3Tick, error) {
	if len(ticksToRead) == 0 {
		return nil, nil
	}

	calls, err := BuildTickCalls(pool, ticksToRead)
	if err != nil {
		return nil, fmt.Errorf("building tick calls for pool %s block %d: %w", pool.Address, bn, err)
	}
	results, err := s.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("executing tick multicall for pool %s block %d: %w", pool.Address, bn, err)
	}
	if len(results) != len(ticksToRead) {
		return nil, fmt.Errorf("pool %s block %d: got %d tick results, want %d", pool.Address, bn, len(results), len(ticksToRead))
	}

	rows := make([]*entity.UniswapV3Tick, 0, len(ticksToRead))
	for i, tick := range ticksToRead {
		row, err := DecodeTick(pool, tick, bn, ver, ts, results[i])
		if err != nil {
			return nil, fmt.Errorf("decoding tick %d for pool %s block %d: %w", tick, pool.Address, bn, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// mergeTickSets returns the deduplicated, ascending-sorted union of touched
// and baseline: the pool's first-touch persist must write every initialized
// tick exactly once, even where the baseline and this block's own mint/burn
// bounds overlap.
func mergeTickSets(touched, baseline []int32) []int32 {
	seen := make(map[int32]struct{}, len(touched)+len(baseline))
	out := make([]int32, 0, len(touched)+len(baseline))
	for _, sets := range [][]int32{touched, baseline} {
		for _, tick := range sets {
			if _, ok := seen[tick]; ok {
				continue
			}
			seen[tick] = struct{}{}
			out = append(out, tick)
		}
	}
	sortInt32s(out)
	return out
}

func sortInt32s(s []int32) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// buildBlockWrites converts decoded accumulators and snapshots into the typed
// input structs that the repo and event writer expect. Called before the
// transaction opens so any future conversion errors would fail fast without
// touching the connection pool.
func (s *UniswapV3Service) buildBlockWrites(acc blockAccumulators, states []*entity.UniswapV3PoolState, ticks []*entity.UniswapV3Tick, bn int64, ver int, ts time.Time) (outbound.UniswapV3BlockWrites, []dexconsumer.ProtocolEventInput) {
	writes := outbound.UniswapV3BlockWrites{
		States:          states,
		Swaps:           acc.swaps,
		LiquidityEvents: acc.liquidity,
		Ticks:           ticks,
		PoolEvents:      acc.poolEvts,
	}

	capturedIns := make([]dexconsumer.ProtocolEventInput, 0, len(acc.captured))
	for _, c := range acc.captured {
		capturedIns = append(capturedIns, dexconsumer.ProtocolEventInput{
			ContractAddress: c.Address,
			ChainID:         s.chainID,
			BlockNumber:     bn,
			BlockVersion:    ver,
			BlockTimestamp:  ts,
			TxHash:          c.TxHash,
			LogIndex:        c.LogIndex,
			EventName:       c.EventName,
			Payload:         c.Payload,
		})
	}
	return writes, capturedIns
}

// persistBlock saves the block writes and captured events in a single DB
// transaction. SaveBlock and SaveBatch share one pgx.Tx so both commit or both
// roll back together. Returns the number of state rows actually inserted (may
// be zero on an idempotent ON CONFLICT DO NOTHING replay).
func (s *UniswapV3Service) persistBlock(ctx context.Context, writes outbound.UniswapV3BlockWrites, capturedIns []dexconsumer.ProtocolEventInput, bn int64) (int64, error) {
	var stateRows int64
	err := s.txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		var txErr error
		stateRows, txErr = s.repo.SaveBlock(ctx, tx, writes)
		if txErr != nil {
			return fmt.Errorf("persisting uniswap v3 block %d: %w", bn, txErr)
		}
		if txErr := s.eventWriter.SaveBatch(ctx, tx, capturedIns); txErr != nil {
			return fmt.Errorf("persisting captured events block %d: %w", bn, txErr)
		}
		return nil
	})
	return stateRows, err
}

// markSnapshotted records the tracker and baselineSeen bookkeeping AFTER a
// successful persist, so a failed block leaves both pools still due and their
// baselines unenumerated for the next (redelivered) attempt.
func (s *UniswapV3Service) markSnapshotted(dueSet []RegisteredPool, baselined []int64, bn int64, ver int) {
	ids := make([]int64, len(dueSet))
	for i, pool := range dueSet {
		ids[i] = pool.ID
	}
	s.tracker.MarkSnapshotted(ids, bn, ver)
	for _, id := range baselined {
		s.baselineSeen[id] = true
	}
}

// indexPoolsByAddress builds the address -> pool index. Unlike Curve, a V3
// pool is reachable only by its own contract address (no separate LP-token
// contract), so no collision detection is needed here: RegisteredPool rows
// come from a single-column registry lookup keyed by that same address.
func indexPoolsByAddress(pools []RegisteredPool) map[common.Address]RegisteredPool {
	byAddr := make(map[common.Address]RegisteredPool, len(pools))
	for _, p := range pools {
		byAddr[p.Address] = p
	}
	return byAddr
}
