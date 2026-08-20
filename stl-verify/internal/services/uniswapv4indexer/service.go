package uniswapv4indexer

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// UniswapV4ServiceDeps groups the UniswapV4Service's constructor arguments.
// Telemetry is optional (nil = no-op); every other field is required.
type UniswapV4ServiceDeps struct {
	Pools       []RegisteredPool
	Multicaller outbound.Multicaller
	Repo        outbound.UniswapV4Repository
	EventWriter *dexconsumer.ProtocolEventWriter
	TxManager   outbound.TxManager
	ChainID     int64
	Logger      *slog.Logger
	Telemetry   *dextelemetry.Telemetry
}

// UniswapV4Service drives per-block event decoding and transactional
// persistence for the Uniswap V4 indexer.
//
// Single-goroutine contract: sqsutil.RunLoop processes one SQS message at a
// time, so no synchronisation is required on any field. All per-block work
// happens in local variables inside handleBlock; the only cross-block state is
// the pool registry, the snapshot tracker (deploy-gate tracking; sweepBlocks=0
// disables its periodic-sweep half, see NewUniswapV4Service), baselineSeen and
// neverIndexed. The last two are seeded from the database at construction, so
// neither is reset by a restart.
type UniswapV4Service struct {
	poolsByID   map[common.Hash]RegisteredPool
	poolsByRow  map[int64]RegisteredPool
	pools       []RegisteredPool // ordered for deterministic iteration
	poolManager common.Address
	multicaller outbound.Multicaller
	repo        outbound.UniswapV4Repository
	eventWriter *dexconsumer.ProtocolEventWriter
	txMgr       outbound.TxManager
	chainID     int64
	logger      *slog.Logger
	telemetry   *dextelemetry.Telemetry

	tracker      *dexconsumer.SnapshotTracker
	baselineSeen map[int64]bool
	neverIndexed map[int64]bool
}

func (d UniswapV4ServiceDeps) validate() error {
	switch {
	case len(d.Pools) == 0:
		return fmt.Errorf("at least one pool is required")
	case d.Multicaller == nil:
		return fmt.Errorf("multicaller is required")
	case d.Repo == nil:
		return fmt.Errorf("repo is required")
	case d.EventWriter == nil:
		return fmt.Errorf("eventWriter is required")
	case d.TxManager == nil:
		return fmt.Errorf("txManager is required")
	case d.ChainID <= 0:
		return fmt.Errorf("chainID must be positive, got %d", d.ChainID)
	case d.Logger == nil:
		return fmt.Errorf("logger is required")
	}
	return nil
}

// NewUniswapV4Service validates deps and builds a UniswapV4Service. It refuses
// to boot on an empty registry, on a registry whose PoolIds disagree with their
// keys (see ValidatePoolKeys), or on one spanning more than one PoolManager:
// each would leave an indexer that looks healthy while silently writing wrong
// or no rows.
//
// The snapshot tracker is built with sweepBlocks=0: for a static-fee pool V4
// state is piecewise-constant between touches — every field this indexer
// snapshots changes only through a PoolManager log keyed by the pool's own
// PoolId — so unlike Curve there is no periodic heartbeat snapshot to keep quiet
// pools fresh. A dynamic-fee pool's lp_fee has no log to ride on and no sweep to
// fall back to, which is why the registry excludes it from snapshots
// (snapshot_supported = false) while still indexing its events.
//
// It reads PoolIDsEverSnapshotted once here, which is why it takes a ctx: that
// read is what makes baselineSeen and neverIndexed survive a restart.
func NewUniswapV4Service(ctx context.Context, deps UniswapV4ServiceDeps) (*UniswapV4Service, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}
	if err := ValidatePoolKeys(deps.Pools); err != nil {
		return nil, err
	}
	poolManager, err := poolManagerFor(deps.Pools)
	if err != nil {
		return nil, err
	}
	indexed, err := deps.Repo.PoolIDsEverSnapshotted(ctx, deps.ChainID)
	if err != nil {
		return nil, fmt.Errorf("reading which uniswap v4 pools have ever been indexed on chain %d: %w", deps.ChainID, err)
	}

	// baselineSeen IS the ever-snapshotted set: see PoolIDsEverSnapshotted.
	baselineSeen := seenSet(indexed)
	svc := &UniswapV4Service{
		poolsByID:    indexPoolsByHash(deps.Pools),
		poolsByRow:   indexPoolsByRowID(deps.Pools),
		pools:        deps.Pools,
		poolManager:  poolManager,
		multicaller:  deps.Multicaller,
		repo:         deps.Repo,
		eventWriter:  deps.EventWriter,
		txMgr:        deps.TxManager,
		chainID:      deps.ChainID,
		logger:       deps.Logger,
		telemetry:    deps.Telemetry,
		tracker:      dexconsumer.NewSnapshotTracker(0),
		baselineSeen: baselineSeen,
		neverIndexed: neverIndexedPools(deps.Pools, baselineSeen),
	}
	svc.reportNeverIndexed(ctx)
	return svc, nil
}

func seenSet(indexed []int64) map[int64]bool {
	seen := make(map[int64]bool, len(indexed))
	for _, id := range indexed {
		seen[id] = true
	}
	return seen
}

// neverIndexedPools skips pools the registry excludes from snapshots: they
// produce no state or tick rows by design, so counting them would leave the
// alert permanently firing.
func neverIndexedPools(pools []RegisteredPool, indexed map[int64]bool) map[int64]bool {
	never := make(map[int64]bool)
	for _, pool := range SnapshottablePools(pools) {
		if !indexed[pool.ID] {
			never[pool.ID] = true
		}
	}
	return never
}

// reportNeverIndexed publishes the count and, while it is non-zero, names the
// pools: the gauge is what alerts, the log is what says which registry row to
// check (the gauge carries no per-pool label, on purpose).
func (s *UniswapV4Service) reportNeverIndexed(ctx context.Context) {
	s.telemetry.RecordPoolsNeverIndexed(ctx, len(s.neverIndexed))
	if len(s.neverIndexed) == 0 {
		return
	}
	ids := slices.Sorted(maps.Keys(s.neverIndexed))
	hashes := make([]string, len(ids))
	for i, id := range ids {
		hashes[i] = s.poolsByRow[id].PoolIDHash.Hex()
	}
	s.logger.Warn("uniswap-v4 pools have never produced a state or tick row",
		"chainId", s.chainID, "count", len(ids), "poolRowIds", ids, "poolIds", hashes)
}

// poolManagerFor returns the one PoolManager address the registry shares. One
// worker serves one chain's PoolManager, and the StateView is that deployment's
// periphery: a registry mixing either address would mean two deployments in one
// worker, which the log filter and the state reads both silently mis-handle.
func poolManagerFor(pools []RegisteredPool) (common.Address, error) {
	first := pools[0]
	for _, pool := range pools[1:] {
		if pool.PoolManager != first.PoolManager {
			return common.Address{}, fmt.Errorf("pools %d and %d have different PoolManager addresses (%s, %s): one worker serves one deployment", first.ID, pool.ID, first.PoolManager, pool.PoolManager)
		}
		if pool.StateView != first.StateView {
			return common.Address{}, fmt.Errorf("pools %d and %d have different StateView addresses (%s, %s): one worker serves one deployment", first.ID, pool.ID, first.StateView, pool.StateView)
		}
	}
	return first.PoolManager, nil
}

// indexPoolsByHash builds the on-chain PoolId -> pool index used to route every
// PoolManager log. ValidatePoolKeys has already rejected duplicate PoolIds.
func indexPoolsByHash(pools []RegisteredPool) map[common.Hash]RegisteredPool {
	byHash := make(map[common.Hash]RegisteredPool, len(pools))
	for _, p := range pools {
		byHash[p.PoolIDHash] = p
	}
	return byHash
}

// indexPoolsByRowID indexes by the uniswap_v4_pool surrogate id, which is how
// persisted fact rows name a pool.
func indexPoolsByRowID(pools []RegisteredPool) map[int64]RegisteredPool {
	byRow := make(map[int64]RegisteredPool, len(pools))
	for _, p := range pools {
		byRow[p.ID] = p
	}
	return byRow
}

// BlockHandler returns the dexconsumer.BlockHandler for this service. It
// records uniswap_v4_errors_total once, at this boundary, on any non-nil
// handler return: handleBlock's inner steps wrap their errors with stage
// context (for the logs) but do not touch the counter, so no future error path
// can silently skip the metric.
func (s *UniswapV4Service) BlockHandler() dexconsumer.BlockHandler {
	return func(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error {
		if err := s.handleBlock(ctx, event, receipts); err != nil {
			s.telemetry.RecordError(ctx, "blockHandler", err)
			return err
		}
		return nil
	}
}

// handleBlock decodes every receipt in the block, snapshots the due pools'
// state and tick rows via multicall (before opening the transaction), and
// persists swaps, liquidity events, pool events, state, ticks, and captured
// logs in one transaction. Returning a non-nil error leaves the block for SQS
// redelivery; nil is returned only after a successful commit. All per-block
// state is local, so a redelivery reprocesses from scratch with no carryover.
func (s *UniswapV4Service) handleBlock(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error {
	blockHash, err := event.ParsedBlockHash()
	if err != nil {
		return err
	}
	blockTime, err := event.BlockTime()
	if err != nil {
		return err
	}
	coords := blockCoords{
		hash:    blockHash,
		number:  event.BlockNumber,
		version: event.Version,
		ts:      blockTime,
	}

	acc, err := s.decodeBlockEvents(ctx, receipts, coords.number, coords.version, coords.ts)
	if err != nil {
		return err
	}

	dueSet, err := s.dueSetForBlock(ctx, acc.touchedIDs, coords)
	if err != nil {
		return err
	}

	states, ticks, baselined, err := s.snapshotDueSet(ctx, dueSet, acc, coords)
	if err != nil {
		return err
	}

	if !acc.hasEvents() && len(states) == 0 && len(ticks) == 0 {
		return nil
	}

	writes, capturedIns := s.buildBlockWrites(acc, states, ticks, coords.number, coords.version, coords.ts)

	stateRows, err := s.persistBlock(ctx, writes, capturedIns, coords.number)
	if err != nil {
		return err
	}

	s.markSnapshotted(dueSet, baselined, coords.number, coords.version)
	s.markIndexed(ctx, dueSet)
	// Recorded only after a successful commit, so the alerts compare pools this
	// block touched against the rows that same block persisted.
	s.telemetry.RecordPoolsTouched(ctx, len(acc.touchedIDs))
	s.telemetry.RecordStateRows(ctx, int(stateRows))
	return nil
}

// markIndexed clears the pools this block snapshotted from the never-indexed
// set and republishes the gauge when it shrank. Every due pool got a state row
// in the commit that just succeeded — an ON CONFLICT DO NOTHING replay means the
// row was already there — so a due pool is indexed either way.
func (s *UniswapV4Service) markIndexed(ctx context.Context, dueSet []RegisteredPool) {
	changed := false
	for _, pool := range dueSet {
		if s.neverIndexed[pool.ID] {
			delete(s.neverIndexed, pool.ID)
			changed = true
		}
	}
	if changed {
		s.reportNeverIndexed(ctx)
	}
}

// dueSetForBlock selects the pools this block must snapshot: everything
// dexconsumer.DueSet picks that the registry marks snapshot_supported, plus —
// on a reorg redelivery — every pool that already has a state row at this
// height. The tracker lives only in memory, so after a restart DueSet's reorg
// rule sees nothing and the orphaned fork's (N, v0) rows would stay
// canonical-latest forever. A pool the registry has since excluded is still
// re-snapshotted on that path: its rows exist and have to be superseded.
//
// DueSet runs over the whole registry rather than the snapshottable subset, so
// its unregistered-touch and deploy-block guards keep covering excluded pools.
func (s *UniswapV4Service) dueSetForBlock(ctx context.Context, touched map[int64]bool, coords blockCoords) ([]RegisteredPool, error) {
	all, err := dexconsumer.DueSet(s.tracker, s.pools, touched, coords.number, coords.version)
	if err != nil {
		return nil, err
	}
	due := SnapshottablePools(all)
	if coords.version == 0 {
		return due, nil
	}

	priorIDs, err := s.repo.PoolIDsWithStateAtBlock(ctx, s.chainID, coords.number, coords.ts)
	if err != nil {
		return nil, fmt.Errorf("reading pools already snapshotted at block %d: %w", coords.number, err)
	}
	return s.withRegisteredPools(due, priorIDs, coords.number)
}

// withRegisteredPools adds the pools named by ids that due does not already
// carry, restoring the ascending-ID order the snapshot loop and the tests rely
// on. ids are already resolved to current registry versions on the worker's own
// chain, so one the registry does not name is a genuine registry bug rather
// than a superseded or foreign row. Pools registered above bn are skipped, not
// errored, matching DueSet's deploy gate: there is no state to read below their
// deploy height.
func (s *UniswapV4Service) withRegisteredPools(due []RegisteredPool, ids []int64, bn int64) ([]RegisteredPool, error) {
	present := make(map[int64]bool, len(due))
	for _, pool := range due {
		present[pool.ID] = true
	}
	for _, id := range ids {
		if present[id] {
			continue
		}
		pool, known := s.poolsByRow[id]
		if !known {
			return nil, fmt.Errorf("pool %d has uniswap_v4_pool_state rows but is absent from the registry: registry bug", id)
		}
		if pool.DeployBlock > bn {
			continue
		}
		present[id] = true
		due = append(due, pool)
	}
	slices.SortFunc(due, func(a, b RegisteredPool) int { return cmp.Compare(a.ID, b.ID) })
	return due, nil
}

// blockCoords is the block identity a hash-pinned read needs: the hash the read
// is pinned to plus the height/version/timestamp its rows are stamped with. It
// travels as one value so the snapshot helpers take the pool and the work
// instead of repeating a four-argument tail (same shape as receiptDecoder).
type blockCoords struct {
	hash    common.Hash
	number  int64
	version int
	ts      time.Time
}

type blockAccumulators struct {
	swaps      []*entity.UniswapV4Swap
	liquidity  []*entity.UniswapV4LiquidityEvent
	poolEvts   []*entity.UniswapV4PoolEvent
	captured   []dexconsumer.CapturedLog
	touchedIDs map[int64]bool
	liqByPool  map[int64][]*entity.UniswapV4LiquidityEvent
}

func (acc blockAccumulators) hasEvents() bool {
	return len(acc.swaps) > 0 || len(acc.liquidity) > 0 || len(acc.poolEvts) > 0 || len(acc.captured) > 0
}

// decodeBlockEvents decodes each receipt once against the whole registry — the
// PoolManager is a single contract, so there is no per-pool pass — and
// accumulates the typed events plus the touched-pool-ID set.
func (s *UniswapV4Service) decodeBlockEvents(ctx context.Context, receipts []shared.TransactionReceipt, bn int64, ver int, ts time.Time) (blockAccumulators, error) {
	acc := blockAccumulators{
		touchedIDs: make(map[int64]bool),
		liqByPool:  make(map[int64][]*entity.UniswapV4LiquidityEvent),
	}
	for _, receipt := range receipts {
		// Bail with an error, never a silent ack, when the handler-timeout budget
		// or a shutdown cancelled ctx mid-block.
		if err := ctx.Err(); err != nil {
			return blockAccumulators{}, err
		}
		decoded, touched, err := DecodeEvents(receipt, s.poolsByID, s.poolManager, bn, ver, ts)
		if err != nil {
			return blockAccumulators{}, fmt.Errorf("decoding PoolManager events at block %d: %w", bn, err)
		}
		acc.swaps = append(acc.swaps, decoded.Swaps...)
		acc.liquidity = append(acc.liquidity, decoded.LiquidityEvents...)
		acc.poolEvts = append(acc.poolEvts, decoded.PoolEvents...)
		acc.captured = append(acc.captured, decoded.Captured...)
		for _, e := range decoded.LiquidityEvents {
			acc.liqByPool[e.PoolID] = append(acc.liqByPool[e.PoolID], e)
		}
		maps.Copy(acc.touchedIDs, touched)
	}
	return acc, nil
}

// snapshotDueSet reads each due pool's state and tick rows via multicall,
// pinned to coords.hash so the read cannot silently answer from a post-reorg
// fork. It must run BEFORE the DB transaction opens, so archive-RPC latency
// never pins a pgx connection (pool exhaustion is a stall cause). Returns the
// pool IDs whose baseline ticks were read this call, so the caller can mark
// baselineSeen only after a successful persist.
func (s *UniswapV4Service) snapshotDueSet(ctx context.Context, dueSet []RegisteredPool, acc blockAccumulators, coords blockCoords) ([]*entity.UniswapV4PoolState, []*entity.UniswapV4Tick, []int64, error) {
	var states []*entity.UniswapV4PoolState
	var ticks []*entity.UniswapV4Tick
	var baselined []int64

	for _, pool := range dueSet {
		state, err := SnapshotState(ctx, s.multicaller, pool, coords.hash, coords.number, coords.version, coords.ts)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("snapshotting pool %s block %d: %w", pool.PoolIDHash, coords.number, err)
		}
		states = append(states, state)

		poolTicks, isFirstSeen, err := s.snapshotPoolTicks(ctx, pool, coords, acc.liqByPool[pool.ID])
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
// liquidity-changing events) plus, on the pool's first-ever touch, every
// currently initialized tick (the baseline enumeration). Returning isFirstSeen
// lets the caller defer marking baselineSeen until after a successful persist,
// so a failed block re-enumerates the baseline on redelivery instead of
// silently skipping it forever.
//
// On a reorg redelivery (coords.version > 0) it additionally re-reads every
// tick that already has a row at this block height. dueSetForBlock re-snapshots a pool at
// (N, v_new) even when the new fork's receipts don't touch it, but TouchedTicks
// is then empty and the baseline (bitmap scan) only enumerates ticks still
// initialized on v_new — so a tick initialized only on the orphaned fork would
// never be re-read and its stale (N, v0) row would stay canonical-latest
// forever. Re-reading the prior version's ticks at coords.hash produces a
// superseding (N, v_new) row for each: a now-cleared tick reads back zeroed
// (getTickInfo never reverts), a still-initialized one gets its v_new value.
func (s *UniswapV4Service) snapshotPoolTicks(ctx context.Context, pool RegisteredPool, coords blockCoords, liqEvents []*entity.UniswapV4LiquidityEvent) ([]*entity.UniswapV4Tick, bool, error) {
	ticksToRead := TouchedTicks(liqEvents)

	isFirstSeen := !s.baselineSeen[pool.ID]
	if isFirstSeen {
		baseline, err := BaselineTicks(ctx, s.multicaller, pool, coords.hash)
		if err != nil {
			return nil, false, fmt.Errorf("enumerating baseline ticks for pool %s block %d: %w", pool.PoolIDHash, coords.number, err)
		}
		ticksToRead = tickbitmap.MergeTickSets(ticksToRead, baseline)
	}

	if coords.version > 0 {
		prior, err := s.repo.TicksForPoolAtBlock(ctx, pool.ID, coords.number)
		if err != nil {
			return nil, false, fmt.Errorf("reading prior-version ticks for pool %s block %d: %w", pool.PoolIDHash, coords.number, err)
		}
		ticksToRead = tickbitmap.MergeTickSets(ticksToRead, prior)
	}

	rows, err := s.readTicks(ctx, pool, coords, ticksToRead)
	if err != nil {
		return nil, false, err
	}
	return rows, isFirstSeen, nil
}

// readTicks reads the given tick positions in bounded multicall batches (see
// tickbitmap.TicksPerCall), decoding every result into an authoritative
// entity.UniswapV4Tick. Batches are issued in order and their rows concatenated
// so the returned slice stays aligned with ticksToRead.
func (s *UniswapV4Service) readTicks(ctx context.Context, pool RegisteredPool, coords blockCoords, ticksToRead []int32) ([]*entity.UniswapV4Tick, error) {
	if len(ticksToRead) == 0 {
		return nil, nil
	}

	rows := make([]*entity.UniswapV4Tick, 0, len(ticksToRead))
	for chunk := range slices.Chunk(ticksToRead, tickbitmap.TicksPerCall) {
		chunkRows, err := s.readTickChunk(ctx, pool, coords, chunk)
		if err != nil {
			return nil, err
		}
		rows = append(rows, chunkRows...)
	}
	return rows, nil
}

// readTickChunk issues one getTickInfo multicall for a single bounded batch of
// tick positions and decodes every result positionally.
func (s *UniswapV4Service) readTickChunk(ctx context.Context, pool RegisteredPool, coords blockCoords, chunk []int32) ([]*entity.UniswapV4Tick, error) {
	calls, err := BuildTickCalls(pool, chunk)
	if err != nil {
		return nil, fmt.Errorf("building tick calls for pool %s block %d: %w", pool.PoolIDHash, coords.number, err)
	}
	results, err := s.multicaller.ExecuteAtHash(ctx, calls, coords.hash)
	if err != nil {
		return nil, fmt.Errorf("executing tick multicall for pool %s block %d: %w", pool.PoolIDHash, coords.number, err)
	}
	if len(results) != len(chunk) {
		return nil, fmt.Errorf("pool %s block %d: got %d tick results, want %d", pool.PoolIDHash, coords.number, len(results), len(chunk))
	}

	rows := make([]*entity.UniswapV4Tick, 0, len(chunk))
	for i, tick := range chunk {
		row, err := DecodeTick(pool, tick, coords.number, coords.version, coords.ts, results[i])
		if err != nil {
			return nil, fmt.Errorf("decoding tick %d for pool %s block %d: %w", tick, pool.PoolIDHash, coords.number, err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// buildBlockWrites runs before the transaction opens, so any future conversion
// error fails fast without holding a pooled connection.
func (s *UniswapV4Service) buildBlockWrites(acc blockAccumulators, states []*entity.UniswapV4PoolState, ticks []*entity.UniswapV4Tick, bn int64, ver int, ts time.Time) (outbound.UniswapV4BlockWrites, []dexconsumer.ProtocolEventInput) {
	writes := outbound.UniswapV4BlockWrites{
		States:          states,
		Swaps:           acc.swaps,
		LiquidityEvents: acc.liquidity,
		Ticks:           ticks,
		PoolEvents:      acc.poolEvts,
	}
	return writes, dexconsumer.ToProtocolEventInputs(acc.captured, s.chainID, bn, ver, ts)
}

// persistBlock returns the number of state rows actually inserted, which is
// zero on an idempotent ON CONFLICT DO NOTHING replay.
func (s *UniswapV4Service) persistBlock(ctx context.Context, writes outbound.UniswapV4BlockWrites, capturedIns []dexconsumer.ProtocolEventInput, bn int64) (int64, error) {
	return dexconsumer.PersistBlock(ctx, s.txMgr, s.eventWriter, func(ctx context.Context, tx pgx.Tx) (int64, error) {
		rows, err := s.repo.SaveBlock(ctx, tx, writes)
		if err != nil {
			return 0, fmt.Errorf("persisting uniswap v4 block %d: %w", bn, err)
		}
		return rows, nil
	}, capturedIns, bn)
}

// markSnapshotted records the tracker and baselineSeen bookkeeping AFTER a
// successful persist, so a failed block leaves both pools still due and their
// baselines unenumerated for the next (redelivered) attempt.
func (s *UniswapV4Service) markSnapshotted(dueSet []RegisteredPool, baselined []int64, bn int64, ver int) {
	ids := make([]int64, len(dueSet))
	for i, pool := range dueSet {
		ids[i] = pool.ID
	}
	s.tracker.MarkSnapshotted(ids, bn, ver)
	for _, id := range baselined {
		s.baselineSeen[id] = true
	}
}
