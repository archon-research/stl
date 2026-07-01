package curveindexer

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
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

// CurveServiceDeps groups the CurveService's constructor arguments.
// No doc comments on self-evident fields: the field names explain themselves.
// Telemetry is optional (nil = no-op).
type CurveServiceDeps struct {
	Pools           []RegisteredPool
	Handlers        map[PoolKind]PoolClassHandler
	Multicaller     outbound.Multicaller
	Repo            outbound.CurveRepository
	EventWriter     *dexconsumer.ProtocolEventWriter
	TxManager       outbound.TxManager
	HeartbeatBlocks int64
	ChainID         int64
	Logger          *slog.Logger
	Telemetry       *dextelemetry.Telemetry
}

// snapshotKey records the (blockNumber, version) of the last persisted snapshot
// for a pool, so heartbeat logic correctly detects reorgs where bn == lastBn but
// version changed.
type snapshotKey struct {
	bn  int64
	ver int
}

// CurveService drives per-block event decoding and transactional persistence for
// the Curve indexer.
//
// Single-goroutine contract: sqsutil.RunLoop processes one SQS message at a
// time, so no synchronisation is required on any CurveService field. All
// per-block work happens in local variables inside BlockHandler; the only
// cross-block state is the pool registry and lastSnapshot (heartbeat tracking).
type CurveService struct {
	// poolsByWatchedAddr maps every address whose logs route to a pool -> that
	// pool. A pool is reachable by its own address and, for pre-NG pools, by its
	// separate LP-token contract (so LP Transfer/Approval reach the owning pool).
	poolsByWatchedAddr map[common.Address]RegisteredPool
	pools              []RegisteredPool // ordered for deterministic iteration
	handlers           map[PoolKind]PoolClassHandler
	multicaller        outbound.Multicaller
	repo               outbound.CurveRepository
	eventWriter        *dexconsumer.ProtocolEventWriter
	txMgr              outbound.TxManager
	heartbeatBlocks    int64
	chainID            int64
	logger             *slog.Logger
	telemetry          *dextelemetry.Telemetry

	lastSnapshot map[int64]snapshotKey // pool.ID -> last snapshotted (block, version)
}

// validate checks that every required dependency is present, so NewCurveService
// reads as its build steps rather than a wall of nil guards.
func (d CurveServiceDeps) validate() error {
	switch {
	case d.Multicaller == nil:
		return fmt.Errorf("multicaller is required")
	case d.Repo == nil:
		return fmt.Errorf("repo is required")
	case d.EventWriter == nil:
		return fmt.Errorf("eventWriter is required")
	case d.TxManager == nil:
		return fmt.Errorf("txManager is required")
	case d.Handlers == nil:
		return fmt.Errorf("handlers is required")
	case d.Logger == nil:
		return fmt.Errorf("logger is required")
	}
	return nil
}

// NewCurveService validates deps and builds a CurveService. Every registered
// pool's Kind must have a corresponding handler entry.
func NewCurveService(deps CurveServiceDeps) (*CurveService, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}

	for _, p := range deps.Pools {
		h, ok := deps.Handlers[p.Kind]
		if !ok {
			return nil, fmt.Errorf("pool %s (id=%d) has kind %q but no handler registered for it", p.Address, p.ID, p.Kind)
		}
		// Warm the handler's per-coin-count caches now, while construction is still
		// single-threaded, so the per-block decode path performs no lazy cache writes.
		h.Warm(p.NCoins)
	}

	watched, err := indexPoolsByWatchedAddress(deps.Pools)
	if err != nil {
		return nil, err
	}

	return &CurveService{
		poolsByWatchedAddr: watched,
		pools:              deps.Pools,
		handlers:           deps.Handlers,
		multicaller:        deps.Multicaller,
		repo:               deps.Repo,
		eventWriter:        deps.EventWriter,
		txMgr:              deps.TxManager,
		heartbeatBlocks:    deps.HeartbeatBlocks,
		chainID:            deps.ChainID,
		logger:             deps.Logger,
		telemetry:          deps.Telemetry,
		lastSnapshot:       make(map[int64]snapshotKey),
	}, nil
}

// blockAccumulators holds every event decoded from a single block's receipts.
type blockAccumulators struct {
	swaps     []SwapRecord
	liquidity []LiquidityRecord
	paramEvts []ParameterEventRecord
	lpEvts    []LpTokenEventRecord
	captured  []CapturedEvent
	touched   map[int64]RegisteredPool
}

// BlockHandler returns the dexconsumer.BlockHandler for this coordinator. It
// decodes every receipt in the block into local accumulators, snapshots the
// touched and heartbeat-due pools (via multicall, before opening the
// transaction), and persists swaps, liquidity events, captured logs, and pool
// state in one transaction. Returning a non-nil error leaves the block for SQS
// redelivery; nil is returned only after a successful commit. All per-block
// state is local, so a redelivery reprocesses from scratch with no carryover.
func (c *CurveService) BlockHandler() dexconsumer.BlockHandler {
	return func(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error {
		bn := event.BlockNumber
		ver := event.Version
		ts := time.Unix(event.BlockTimestamp, 0).UTC()

		acc, err := c.decodeBlockEvents(ctx, receipts, bn, ver, ts)
		if err != nil {
			return err
		}

		snapshotSet := c.buildSnapshotSet(bn, ver, acc.touched)

		// Read pool state via multicall BEFORE opening the transaction so archive-RPC
		// latency never pins a pgx connection (connection-pool exhaustion is a stall cause).
		snapshots, err := c.snapshotPools(ctx, snapshotSet, bn, ver, ts)
		if err != nil {
			return err
		}

		// Quiet block: nothing decoded and no snapshot due. Skip the empty transaction.
		if len(acc.swaps) == 0 && len(acc.liquidity) == 0 && len(acc.paramEvts) == 0 &&
			len(acc.lpEvts) == 0 && len(acc.captured) == 0 && len(snapshots) == 0 {
			return nil
		}

		// Build DB inputs before opening the transaction so conversion errors fail
		// fast without touching the connection pool.
		writes, capturedIns, err := c.buildBlockWrites(acc, snapshots, bn, ver, ts)
		if err != nil {
			return err
		}

		stateRows, err := c.persistBlock(ctx, writes, capturedIns, bn)
		if err != nil {
			return err
		}

		for _, pool := range snapshotSet {
			c.lastSnapshot[pool.ID] = snapshotKey{bn: bn, ver: ver}
		}
		c.telemetry.RecordStateRows(ctx, int(stateRows))
		return nil
	}
}

// decodeBlockEvents iterates every receipt, identifies pools touched by each
// receipt's logs, calls each pool's handler, and accumulates the decoded events.
// ctx is checked at the start of each receipt to honour cancellation without
// silently acking a partially-decoded block.
func (c *CurveService) decodeBlockEvents(ctx context.Context, receipts []shared.TransactionReceipt, bn int64, ver int, ts time.Time) (blockAccumulators, error) {
	acc := blockAccumulators{
		touched: make(map[int64]RegisteredPool),
	}
	for _, receipt := range receipts {
		// Bail early (with an error, never a silent ack) if the handler-timeout
		// budget or a shutdown cancelled ctx, rather than decoding the rest.
		if err := ctx.Err(); err != nil {
			return blockAccumulators{}, err
		}
		pools, err := c.poolsTouchedByReceipt(receipt, bn)
		if err != nil {
			return blockAccumulators{}, err
		}
		for _, pool := range pools {
			decoded, err := c.handlers[pool.Kind].DecodeEvents(receipt, pool, c.chainID, bn, ver, ts)
			if err != nil {
				c.telemetry.RecordError(ctx, "decodeEvents", err)
				return blockAccumulators{}, fmt.Errorf("decoding events for pool %s block %d: %w", pool.Address, bn, err)
			}
			acc.swaps = append(acc.swaps, decoded.Swaps...)
			acc.liquidity = append(acc.liquidity, decoded.Liquidity...)
			acc.paramEvts = append(acc.paramEvts, decoded.ParameterEvents...)
			acc.lpEvts = append(acc.lpEvts, decoded.LpTokenEvents...)
			acc.captured = append(acc.captured, decoded.Captured...)
			acc.touched[pool.ID] = pool
		}
	}
	return acc, nil
}

// snapshotPools calls each pool's SnapshotState via multicall and validates the
// result. It must run BEFORE the DB transaction opens (see BlockHandler doc).
func (c *CurveService) snapshotPools(ctx context.Context, snapshotSet []RegisteredPool, bn int64, ver int, ts time.Time) ([]StateSnapshot, error) {
	snapshots := make([]StateSnapshot, 0, len(snapshotSet))
	for _, pool := range snapshotSet {
		snap, err := c.handlers[pool.Kind].SnapshotState(ctx, c.multicaller, pool, bn, ver, ts)
		if err != nil {
			c.telemetry.RecordError(ctx, "snapshotState", err)
			return nil, fmt.Errorf("snapshotting pool %s block %d: %w", pool.Address, bn, err)
		}
		if err := snap.Validate(); err != nil {
			c.telemetry.RecordError(ctx, "snapshotState", err)
			return nil, fmt.Errorf("invalid snapshot for pool %s block %d: %w", pool.Address, bn, err)
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, nil
}

// buildBlockWrites converts decoded accumulators and snapshots into the typed
// input structs that the repo and event writer expect. Conversion errors are
// returned before the transaction opens so they fail fast without touching the
// connection pool.
func (c *CurveService) buildBlockWrites(acc blockAccumulators, snapshots []StateSnapshot, bn int64, ver int, ts time.Time) (outbound.BlockWrites, []dexconsumer.ProtocolEventInput, error) {
	swapIns := make([]outbound.SwapInput, 0, len(acc.swaps))
	for _, s := range acc.swaps {
		swapIns = append(swapIns, toSwapInput(s, bn, ver, ts))
	}

	liqIns := make([]outbound.LiquidityInput, 0, len(acc.liquidity))
	for _, l := range acc.liquidity {
		liqIns = append(liqIns, toLiquidityInput(l, bn, ver, ts))
	}

	split := splitSnapshots(snapshots)

	paramIns, err := collectParameterEvents(acc.paramEvts, bn, ver, ts)
	if err != nil {
		return outbound.BlockWrites{}, nil, err
	}
	lpIns, err := collectLpTokenEvents(acc.lpEvts, bn, ver, ts)
	if err != nil {
		return outbound.BlockWrites{}, nil, err
	}

	capturedIns := make([]dexconsumer.ProtocolEventInput, 0, len(acc.captured))
	for _, cap := range acc.captured {
		capturedIns = append(capturedIns, dexconsumer.ProtocolEventInput{
			ContractAddress: cap.Address,
			ChainID:         c.chainID,
			BlockNumber:     bn,
			BlockVersion:    ver,
			BlockTimestamp:  ts,
			TxHash:          cap.TxHash,
			LogIndex:        cap.LogIndex,
			EventName:       cap.EventName,
			Payload:         cap.Payload,
		})
	}

	writes := outbound.BlockWrites{
		Swaps:             swapIns,
		Liquidity:         liqIns,
		StableStates:      split.stableStates,
		CryptoStates:      split.cryptoStates,
		StableswapConfigs: split.stableConfigs,
		CryptoswapConfigs: split.cryptoConfigs,
		ParameterEvents:   paramIns,
		LpTokenEvents:     lpIns,
	}
	return writes, capturedIns, nil
}

// persistBlock saves the block writes and captured events in a single DB
// transaction. SaveBlock and SaveBatch share one pgx.Tx so both commit or both
// roll back together. Returns the number of state rows actually inserted (may be
// zero on an idempotent ON CONFLICT DO NOTHING replay).
func (c *CurveService) persistBlock(ctx context.Context, writes outbound.BlockWrites, capturedIns []dexconsumer.ProtocolEventInput, bn int64) (int64, error) {
	var stateRows int64
	err := c.txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		var txErr error
		stateRows, txErr = c.repo.SaveBlock(ctx, tx, writes)
		if txErr != nil {
			c.telemetry.RecordError(ctx, "persistBlock", txErr)
			return fmt.Errorf("persisting curve block %d: %w", bn, txErr)
		}
		if txErr := c.eventWriter.SaveBatch(ctx, tx, capturedIns); txErr != nil {
			c.telemetry.RecordError(ctx, "persistBlock", txErr)
			return fmt.Errorf("persisting captured events block %d: %w", bn, txErr)
		}
		return nil
	})
	return stateRows, err
}

// indexPoolsByWatchedAddress builds the address -> pool index. Each pool is
// reachable by its own address and, when set, by its separate LP-token contract
// address (pre-NG pools). A watched address must map to exactly one pool; a
// collision (two pools sharing an address) is a registry bug we fail on at
// construction rather than silently dropping events for one of them.
func indexPoolsByWatchedAddress(pools []RegisteredPool) (map[common.Address]RegisteredPool, error) {
	byAddr := make(map[common.Address]RegisteredPool, len(pools))
	add := func(addr common.Address, pool RegisteredPool) error {
		if existing, ok := byAddr[addr]; ok && existing.ID != pool.ID {
			return fmt.Errorf("address %s maps to both pool %d and pool %d", addr, existing.ID, pool.ID)
		}
		byAddr[addr] = pool
		return nil
	}
	for _, p := range pools {
		if err := add(p.Address, p); err != nil {
			return nil, err
		}
		if p.LpTokenAddress != nil {
			if err := add(*p.LpTokenAddress, p); err != nil {
				return nil, err
			}
		}
	}
	return byAddr, nil
}

// poolsTouchedByReceipt returns the deduplicated pools whose watched address (own
// or LP-token contract) appears in the receipt's logs, in deterministic pool-ID
// order. A pool reachable by both its own address and its LP-token address in the
// same receipt is returned once so DecodeEvents (which scans all logs) runs once.
func (c *CurveService) poolsTouchedByReceipt(receipt shared.TransactionReceipt, bn int64) ([]RegisteredPool, error) {
	byID := make(map[int64]RegisteredPool)
	for _, log := range receipt.Logs {
		if !common.IsHexAddress(log.Address) {
			return nil, fmt.Errorf("invalid log address %q in block %d", log.Address, bn)
		}
		if pool, ok := c.poolsByWatchedAddr[common.HexToAddress(log.Address)]; ok {
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

// buildSnapshotSet returns the sorted (by pool.ID ASC) union of touched pools
// and heartbeat-due pools. Consistent ordering is required by the advisory-lock
// convention in the DB trigger.
func (c *CurveService) buildSnapshotSet(bn int64, ver int, touched map[int64]RegisteredPool) []RegisteredPool {
	byID := make(map[int64]RegisteredPool)
	maps.Copy(byID, touched)
	if c.heartbeatBlocks > 0 {
		for _, pool := range c.pools {
			last, seen := c.lastSnapshot[pool.ID]
			if !seen || bn-last.bn >= c.heartbeatBlocks || (bn == last.bn && ver != last.ver) {
				byID[pool.ID] = pool
			}
		}
	}
	result := make([]RegisteredPool, 0, len(byID))
	for _, pool := range byID {
		result = append(result, pool)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].ID < result[j].ID
	})
	return result
}

// ---------------------------------------------------------------------------
// Mapping helpers: domain records -> repo input structs
// ---------------------------------------------------------------------------

// splitSnapshots groups class-tagged snapshots into the per-class state and
// config slices the repo's BlockWrites expects.
type splitSnapshotResult struct {
	stableStates  []*entity.CurveStableswapState
	cryptoStates  []*entity.CurveCryptoswapState
	stableConfigs []*entity.CurveStableswapConfig
	cryptoConfigs []*entity.CurveCryptoswapConfig
}

func splitSnapshots(snapshots []StateSnapshot) splitSnapshotResult {
	var r splitSnapshotResult
	for _, snap := range snapshots {
		switch {
		case snap.Stableswap != nil:
			r.stableStates = append(r.stableStates, snap.Stableswap)
		case snap.Cryptoswap != nil:
			r.cryptoStates = append(r.cryptoStates, snap.Cryptoswap)
		}
		if snap.StableswapConfig != nil {
			r.stableConfigs = append(r.stableConfigs, snap.StableswapConfig)
		}
		if snap.CryptoswapConfig != nil {
			r.cryptoConfigs = append(r.cryptoConfigs, snap.CryptoswapConfig)
		}
	}
	return r
}

func collectParameterEvents(recs []ParameterEventRecord, bn int64, ver int, ts time.Time) ([]*entity.CurveParameterEvent, error) {
	out := make([]*entity.CurveParameterEvent, 0, len(recs))
	for _, p := range recs {
		rec, err := entity.NewCurveParameterEvent(entity.CurveParameterEventParams{
			CurvePoolID:  p.Pool.ID,
			BlockNumber:  bn,
			BlockVersion: ver,
			Timestamp:    ts,
			TxHash:       p.TxHash,
			LogIndex:     int(p.LogIndex),
			EventName:    p.EventName,
			Params:       p.Params,
		})
		if err != nil {
			return nil, fmt.Errorf("building parameter event for pool %d block %d: %w", p.Pool.ID, bn, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

func collectLpTokenEvents(recs []LpTokenEventRecord, bn int64, ver int, ts time.Time) ([]*entity.CurveLpTokenEvent, error) {
	out := make([]*entity.CurveLpTokenEvent, 0, len(recs))
	for _, l := range recs {
		rec, err := entity.NewCurveLpTokenEvent(entity.CurveLpTokenEventParams{
			CurvePoolID:  l.Pool.ID,
			BlockNumber:  bn,
			BlockVersion: ver,
			Timestamp:    ts,
			TxHash:       l.TxHash,
			LogIndex:     int(l.LogIndex),
			EventName:    l.EventName,
			From:         l.From,
			To:           l.To,
			Value:        l.Value,
		})
		if err != nil {
			return nil, fmt.Errorf("building lp token event for pool %d block %d: %w", l.Pool.ID, bn, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

func toSwapInput(s SwapRecord, bn int64, ver int, ts time.Time) outbound.SwapInput {
	return outbound.SwapInput{
		CurvePoolID:    s.Pool.ID,
		BlockNumber:    bn,
		BlockVersion:   ver,
		BlockTimestamp: ts,
		LogIndex:       int(s.LogIndex),
		TxHash:         s.TxHash,
		Buyer:          s.Buyer,
		SoldID:         s.SoldID,
		BoughtID:       s.BoughtID,
		TokensSold:     s.TokensSold,
		TokensBought:   s.TokensBought,
		Fee:            s.Fee,
		IsUnderlying:   s.IsUnderlying,
	}
}

func toLiquidityInput(l LiquidityRecord, bn int64, ver int, ts time.Time) outbound.LiquidityInput {
	return outbound.LiquidityInput{
		CurvePoolID:    l.Pool.ID,
		BlockNumber:    bn,
		BlockVersion:   ver,
		BlockTimestamp: ts,
		LogIndex:       int(l.LogIndex),
		TxHash:         l.TxHash,
		Provider:       l.Provider,
		Kind:           string(l.Kind),
		TokenAmounts:   l.TokenAmounts,
		CoinIndex:      l.CoinIndex,
		Fees:           l.Fees,
		Invariant:      l.Invariant,
		TokenSupply:    l.TokenSupply,
	}
}
