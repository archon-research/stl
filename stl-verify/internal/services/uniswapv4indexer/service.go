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
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/tickbitmap"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

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

// sqsutil.RunLoop processes one SQS message at a time, so no field is
// synchronised.
type UniswapV4Service struct {
	poolsByID       map[common.Hash]RegisteredPool
	poolsByRow      map[int64]RegisteredPool
	pools           []RegisteredPool // ordered for deterministic iteration
	poolManager     common.Address
	positionManager RegisteredPositionManager
	multicaller     outbound.Multicaller
	repo            outbound.UniswapV4Repository
	eventWriter     *dexconsumer.ProtocolEventWriter
	txMgr           outbound.TxManager
	chainID         int64
	logger          *slog.Logger
	telemetry       *dextelemetry.Telemetry

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

// V4 state is piecewise-constant between touches: every field snapshotted here
// changes only through a PoolManager log keyed by the pool's own PoolId.
const noPeriodicSweep = 0

func NewUniswapV4Service(ctx context.Context, deps UniswapV4ServiceDeps) (*UniswapV4Service, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}
	if err := ValidatePoolKeys(deps.Pools); err != nil {
		return nil, err
	}
	// Fail the boot with the reason: a broken routing table would otherwise fail
	// every receipt of every block and wedge the queue.
	if _, err := eventsByID(); err != nil {
		return nil, err
	}
	if _, err := positionManagerTransferEvent(); err != nil {
		return nil, err
	}
	poolManager, err := PoolManagerFor(deps.Pools)
	if err != nil {
		return nil, err
	}
	positionManager, err := PositionManagerFor(deps.Pools)
	if err != nil {
		return nil, err
	}
	if positionManager.Address == poolManager {
		return nil, fmt.Errorf("the registry gives the PoolManager and the PositionManager the same address (%s): every PoolManager log would decode as an NFT transfer", poolManager)
	}
	everSnapshotted, err := deps.Repo.PoolIDsEverSnapshotted(ctx, deps.ChainID)
	if err != nil {
		return nil, fmt.Errorf("reading which uniswap v4 pools have ever been indexed on chain %d: %w", deps.ChainID, err)
	}
	baselineSeen := seenSet(everSnapshotted)
	svc := &UniswapV4Service{
		poolsByID:       indexPoolsByHash(deps.Pools),
		poolsByRow:      indexPoolsByRowID(deps.Pools),
		pools:           deps.Pools,
		poolManager:     poolManager,
		positionManager: positionManager,
		multicaller:     deps.Multicaller,
		repo:            deps.Repo,
		eventWriter:     deps.EventWriter,
		txMgr:           deps.TxManager,
		chainID:         deps.ChainID,
		logger:          deps.Logger,
		telemetry:       deps.Telemetry,
		tracker:         dexconsumer.NewSnapshotTracker(noPeriodicSweep),
		baselineSeen:    baselineSeen,
		neverIndexed:    neverIndexedPools(deps.Pools, baselineSeen),
	}
	svc.reportNeverIndexed(ctx)
	svc.reportExcludedFromSnapshots()
	return svc, nil
}

func seenSet(ids []int64) map[int64]bool {
	seen := make(map[int64]bool, len(ids))
	for _, id := range ids {
		seen[id] = true
	}
	return seen
}

// Registry-excluded pools produce no state or tick rows by design; counting them
// would leave the never-indexed alert permanently firing.
func neverIndexedPools(pools []RegisteredPool, indexed map[int64]bool) map[int64]bool {
	never := make(map[int64]bool)
	for _, pool := range SnapshottablePools(pools) {
		if !indexed[pool.ID] {
			never[pool.ID] = true
		}
	}
	return never
}

// The gauge carries no per-pool label, so the log names the pools.
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

// No metric names these pools: the never-indexed gauge skips them, and their
// touches aggregate away under snapshot_supported "false".
func (s *UniswapV4Service) reportExcludedFromSnapshots() {
	var ids []int64
	var hashes []string
	for _, pool := range s.pools {
		if !pool.SnapshotSupported {
			ids = append(ids, pool.ID)
			hashes = append(hashes, pool.PoolIDHash.Hex())
		}
	}
	if len(ids) == 0 {
		return
	}
	s.logger.Info("uniswap-v4 pools excluded from snapshots by the registry",
		"chainId", s.chainID, "count", len(ids), "poolRowIds", ids, "poolIds", hashes)
}

// PoolManagerFor returns the one PoolManager address the registry shares; a
// mixed registry is two deployments, which the log filter silently mis-handles.
func PoolManagerFor(pools []RegisteredPool) (common.Address, error) {
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

// PositionManagerFor returns the one ERC-721 PositionManager the registry
// shares, by PoolManagerFor's one-deployment rule. A registry that lost it would
// hand back address(0), which the log filter matches.
func PositionManagerFor(pools []RegisteredPool) (RegisteredPositionManager, error) {
	first := pools[0]
	for _, pool := range pools[1:] {
		if pool.PositionManager != first.PositionManager {
			return RegisteredPositionManager{}, fmt.Errorf("pools %d and %d have different PositionManager addresses (%s, %s): one worker serves one deployment", first.ID, pool.ID, first.PositionManager, pool.PositionManager)
		}
		if pool.PositionManagerID != first.PositionManagerID {
			return RegisteredPositionManager{}, fmt.Errorf("pools %d and %d have different uniswap_v4_position_manager rows (%d, %d): one worker serves one deployment", first.ID, pool.ID, first.PositionManagerID, pool.PositionManagerID)
		}
	}
	if first.PositionManager == (common.Address{}) || first.PositionManagerID <= 0 {
		return RegisteredPositionManager{}, fmt.Errorf("pool %d carries no PositionManager registry row: address(0) would match every log", first.ID)
	}
	return RegisteredPositionManager{ID: first.PositionManagerID, Address: first.PositionManager}, nil
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

func indexPoolsByRowID(pools []RegisteredPool) map[int64]RegisteredPool {
	byRow := make(map[int64]RegisteredPool, len(pools))
	for _, p := range pools {
		byRow[p.ID] = p
	}
	return byRow
}

// The error counter is recorded once, here, so no inner error path can skip it.
func (s *UniswapV4Service) BlockHandler() dexconsumer.BlockHandler {
	return func(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error {
		if err := s.handleBlock(ctx, event, receipts); err != nil {
			s.telemetry.RecordError(ctx, "blockHandler", err)
			return err
		}
		return nil
	}
}

// handleBlock decodes every receipt and snapshots the due pools BEFORE opening
// the transaction, then persists the block in one. A non-nil error leaves the
// block for SQS redelivery, and all per-block state is local, so a replay is clean.
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

	snaps, err := s.snapshotDueSet(ctx, dueSet, acc, coords)
	if err != nil {
		return err
	}

	if !acc.hasEvents() && snaps.isEmpty() {
		return nil
	}

	writes, capturedIns := s.buildBlockWrites(acc, snaps, coords)

	stateRows, err := s.persistBlock(ctx, writes, capturedIns, coords.number)
	if err != nil {
		return err
	}

	s.markSnapshotted(dueSet, snaps.baselined, coords.number, coords.version)
	s.markIndexed(ctx, dueSet)
	s.recordBlockMetrics(ctx, acc, writes, stateRows)
	return nil
}

// recordBlockMetrics runs only after a successful commit. Attempted is what
// VectorUniswapV4IndexerNotWritingState keys on; the tick/position counts come
// from the write set, so they over-count the rows the writer drops as unchanged.
func (s *UniswapV4Service) recordBlockMetrics(ctx context.Context, acc blockAccumulators, writes outbound.UniswapV4BlockWrites, stateRows outbound.StateRowCounts) {
	s.recordPoolsTouched(ctx, acc.touchedIDs)
	s.telemetry.RecordStateRowsAttempted(ctx, int(stateRows.Attempted))
	s.telemetry.RecordStateRows(ctx, int(stateRows.Persisted))
	s.telemetry.RecordTickRows(ctx, len(writes.Ticks))
	s.telemetry.RecordPositionRows(ctx, len(writes.Positions))
	s.telemetry.RecordNFTTransferRows(ctx, len(writes.NFTTransfers))
}

// Only the snapshot_supported half reaches the due set, so only it may gate
// NotWritingState; NoPoolsTouched wants every touch and aggregates the label away.
func (s *UniswapV4Service) recordPoolsTouched(ctx context.Context, touched map[int64]bool) {
	supported := 0
	for id := range touched {
		if s.poolsByRow[id].SnapshotSupported {
			supported++
		}
	}
	s.telemetry.RecordPoolsTouched(ctx, supported, attribute.String(snapshotSupportedKey, "true"))
	s.telemetry.RecordPoolsTouched(ctx, len(touched)-supported, attribute.String(snapshotSupportedKey, "false"))
}

// Literal "true"/"false" rather than attribute.Bool: the alert selects on the value.
const snapshotSupportedKey = "snapshot_supported"

// A due pool is indexed either way: the commit appended its state row, or an
// ON CONFLICT DO NOTHING replay found it already there.
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

// The tracker lives only in memory, so a restart would leave the orphaned fork's
// (N, v0) rows canonical-latest; a reorg redelivery therefore re-snapshots every
// pool that already has a state row at this height, excluded ones included.
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

// PoolIds are natural keys, so a registry version appended after boot resolves to
// the same boot-time pool. One this process does not know at all was registered
// after it loaded the registry — a restart reloads it — or is absent from it.
func (s *UniswapV4Service) withRegisteredPools(due []RegisteredPool, poolIDs []common.Hash, bn int64) ([]RegisteredPool, error) {
	present := make(map[common.Hash]bool, len(due))
	for _, pool := range due {
		present[pool.PoolIDHash] = true
	}
	for _, id := range poolIDs {
		if present[id] {
			continue
		}
		pool, known := s.poolsByID[id]
		if !known {
			return nil, fmt.Errorf("pool %s has uniswap_v4_pool_state rows at block %d but this process does not know it: registered after boot (restart to reload) or absent from the registry", id, bn)
		}
		if pool.DeployBlock > bn {
			return nil, fmt.Errorf("pool %d has uniswap_v4_pool_state rows at block %d but is registered as deployed at block %d: registry bug", pool.ID, bn, pool.DeployBlock)
		}
		present[id] = true
		due = append(due, pool)
	}
	slices.SortFunc(due, func(a, b RegisteredPool) int { return cmp.Compare(a.ID, b.ID) })
	return due, nil
}

type blockCoords struct {
	hash    common.Hash
	number  int64
	version int
	ts      time.Time
}

type blockAccumulators struct {
	swaps        []*entity.UniswapV4Swap
	liquidity    []*entity.UniswapV4LiquidityEvent
	poolEvts     []*entity.UniswapV4PoolEvent
	nftTransfers []*entity.UniswapV4PositionNFTTransfer
	captured     []dexconsumer.CapturedLog
	touchedIDs   map[int64]bool
	liqByPool    map[int64][]*entity.UniswapV4LiquidityEvent
}

// Must count NFT transfers: they touch no pool, so a block whose only V4
// activity is a posm transfer would otherwise be dropped before the write.
func (acc blockAccumulators) hasEvents() bool {
	return len(acc.swaps) > 0 || len(acc.liquidity) > 0 || len(acc.poolEvts) > 0 ||
		len(acc.nftTransfers) > 0 || len(acc.captured) > 0
}

func (s *UniswapV4Service) decodeBlockEvents(ctx context.Context, receipts []shared.TransactionReceipt, bn int64, ver int, ts time.Time) (blockAccumulators, error) {
	acc := blockAccumulators{
		touchedIDs: make(map[int64]bool),
		liqByPool:  make(map[int64][]*entity.UniswapV4LiquidityEvent),
	}
	for _, receipt := range receipts {
		if err := ctx.Err(); err != nil {
			return blockAccumulators{}, err
		}
		decoded, touched, err := DecodeEvents(receipt, s.poolsByID, s.poolManager, s.positionManager, bn, ver, ts)
		if err != nil {
			return blockAccumulators{}, fmt.Errorf("decoding PoolManager events at block %d: %w", bn, err)
		}
		acc.swaps = append(acc.swaps, decoded.Swaps...)
		acc.liquidity = append(acc.liquidity, decoded.LiquidityEvents...)
		acc.poolEvts = append(acc.poolEvts, decoded.PoolEvents...)
		acc.nftTransfers = append(acc.nftTransfers, decoded.NFTTransfers...)
		acc.captured = append(acc.captured, decoded.Captured...)
		for _, e := range decoded.LiquidityEvents {
			acc.liqByPool[e.PoolID] = append(acc.liqByPool[e.PoolID], e)
		}
		maps.Copy(acc.touchedIDs, touched)
	}
	return acc, nil
}

// blockSnapshots is one block's hash-pinned read output. baselined is returned
// so the caller marks baselineSeen only after a successful persist.
type blockSnapshots struct {
	states    []*entity.UniswapV4PoolState
	ticks     []*entity.UniswapV4Tick
	positions []*entity.UniswapV4Position
	baselined []int64
}

// isEmpty omits baselined: snapshotPool emits a state row for every due pool, so
// a non-empty baselined implies a non-empty states.
func (snaps blockSnapshots) isEmpty() bool {
	return len(snaps.states) == 0 && len(snaps.ticks) == 0 && len(snaps.positions) == 0
}

func (snaps *blockSnapshots) appendPool(other blockSnapshots) {
	snaps.states = append(snaps.states, other.states...)
	snaps.ticks = append(snaps.ticks, other.ticks...)
	snaps.positions = append(snaps.positions, other.positions...)
	snaps.baselined = append(snaps.baselined, other.baselined...)
}

// snapshotDueSet reads the due pools at coords.hash, so no read can answer from
// a post-reorg fork. It must run BEFORE the DB transaction opens, or archive-RPC
// latency pins a pgx connection (pool exhaustion is a stall cause).
func (s *UniswapV4Service) snapshotDueSet(ctx context.Context, dueSet []RegisteredPool, acc blockAccumulators, coords blockCoords) (blockSnapshots, error) {
	var snaps blockSnapshots

	for _, pool := range dueSet {
		poolSnaps, err := s.snapshotPool(ctx, pool, coords, acc.liqByPool[pool.ID])
		if err != nil {
			return blockSnapshots{}, err
		}
		snaps.appendPool(poolSnaps)
	}
	return snaps, nil
}

func (s *UniswapV4Service) snapshotPool(ctx context.Context, pool RegisteredPool, coords blockCoords, liqEvents []*entity.UniswapV4LiquidityEvent) (blockSnapshots, error) {
	state, err := SnapshotState(ctx, s.multicaller, pool, coords.hash, coords.number, coords.version, coords.ts)
	if err != nil {
		return blockSnapshots{}, fmt.Errorf("snapshotting pool %s block %d: %w", pool.PoolIDHash, coords.number, err)
	}

	ticks, isFirstSeen, err := s.snapshotPoolTicks(ctx, pool, coords, liqEvents)
	if err != nil {
		return blockSnapshots{}, err
	}

	positions, err := s.snapshotPoolPositions(ctx, pool, coords, liqEvents)
	if err != nil {
		return blockSnapshots{}, err
	}

	snaps := blockSnapshots{states: []*entity.UniswapV4PoolState{state}, ticks: ticks, positions: positions}
	if isFirstSeen {
		snaps.baselined = []int64{pool.ID}
	}
	return snaps, nil
}

// snapshotPoolPositions has no baseline-enumeration counterpart to BaselineTicks:
// V4 cannot list a pool's positions, so one is discovered only from a log — hence
// the prior-version re-read, which reads a fork-orphaned position back as zeroed.
func (s *UniswapV4Service) snapshotPoolPositions(ctx context.Context, pool RegisteredPool, coords blockCoords, liqEvents []*entity.UniswapV4LiquidityEvent) ([]*entity.UniswapV4Position, error) {
	keys := TouchedPositions(liqEvents)

	if coords.version > 0 {
		prior, err := s.repo.PositionsForPoolAtBlock(ctx, pool.ID, coords.number)
		if err != nil {
			return nil, fmt.Errorf("reading prior-version positions for pool %s block %d: %w", pool.PoolIDHash, coords.number, err)
		}
		keys = MergePositionKeys(keys, prior)
	}

	return ReadPositions(ctx, s.multicaller, pool, keys, coords.hash, coords.number, coords.version, coords.ts)
}

// A tick initialized only on an orphaned fork is invisible to the bitmap scan, so
// a reorg redelivery also re-reads every tick that has a row at this height; a
// now-cleared one reads back zeroed, superseding its stale (N, v0) row.
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
		prior, err := s.repo.TicksForPoolAtBlock(ctx, s.chainID, pool.ID, coords.number)
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
func (s *UniswapV4Service) buildBlockWrites(acc blockAccumulators, snaps blockSnapshots, coords blockCoords) (outbound.UniswapV4BlockWrites, []dexconsumer.ProtocolEventInput) {
	writes := outbound.UniswapV4BlockWrites{
		States:          snaps.states,
		Swaps:           acc.swaps,
		LiquidityEvents: acc.liquidity,
		Ticks:           snaps.ticks,
		PoolEvents:      acc.poolEvts,
		Positions:       snaps.positions,
		NFTTransfers:    acc.nftTransfers,
	}
	return writes, dexconsumer.ToProtocolEventInputs(acc.captured, s.chainID, coords.number, coords.version, coords.ts)
}

// PersistBlock carries only the persisted count back, so attempted rides the
// closure.
func (s *UniswapV4Service) persistBlock(ctx context.Context, writes outbound.UniswapV4BlockWrites, capturedIns []dexconsumer.ProtocolEventInput, bn int64) (outbound.StateRowCounts, error) {
	var attempted int64
	persisted, err := dexconsumer.PersistBlock(ctx, s.txMgr, s.eventWriter, func(ctx context.Context, tx pgx.Tx) (int64, error) {
		rows, err := s.repo.SaveBlock(ctx, tx, writes)
		if err != nil {
			return 0, fmt.Errorf("persisting uniswap v4 block %d: %w", bn, err)
		}
		attempted = rows.Attempted
		return rows.Persisted, nil
	}, capturedIns, bn)
	if err != nil {
		return outbound.StateRowCounts{}, err
	}
	return outbound.StateRowCounts{Attempted: attempted, Persisted: persisted}, nil
}

// Called only after a successful persist: a failed block must leave its pools due
// and their baselines unenumerated.
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
