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

	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// CoordinatorDeps groups the Coordinator's constructor arguments.
// No doc comments on self-evident fields: the field names explain themselves.
// Telemetry is optional (nil = no-op).
type CoordinatorDeps struct {
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

// Coordinator drives per-block event decoding and transactional persistence for
// the Curve indexer.
//
// Single-goroutine contract: sqsutil.RunLoop processes one SQS message at a
// time, so no synchronisation is required on any Coordinator field. All
// per-block work happens in local variables inside BlockHandler; the only
// cross-block state is the pool registry and lastSnapshot (heartbeat tracking).
type Coordinator struct {
	poolsByAddr     map[common.Address]RegisteredPool
	pools           []RegisteredPool // ordered for deterministic iteration
	handlers        map[PoolKind]PoolClassHandler
	multicaller     outbound.Multicaller
	repo            outbound.CurveRepository
	eventWriter     *dexconsumer.ProtocolEventWriter
	txMgr           outbound.TxManager
	heartbeatBlocks int64
	chainID         int64
	logger          *slog.Logger
	telemetry       *dextelemetry.Telemetry

	lastSnapshot map[int64]snapshotKey // pool.ID -> last snapshotted (block, version)
}

// NewCoordinator validates deps and builds a Coordinator. Every registered
// pool's Kind must have a corresponding handler entry.
func NewCoordinator(deps CoordinatorDeps) (*Coordinator, error) {
	if deps.Multicaller == nil {
		return nil, fmt.Errorf("multicaller is required")
	}
	if deps.Repo == nil {
		return nil, fmt.Errorf("repo is required")
	}
	if deps.EventWriter == nil {
		return nil, fmt.Errorf("eventWriter is required")
	}
	if deps.TxManager == nil {
		return nil, fmt.Errorf("txManager is required")
	}
	if deps.Handlers == nil {
		return nil, fmt.Errorf("handlers is required")
	}
	if deps.Logger == nil {
		return nil, fmt.Errorf("logger is required")
	}

	poolsByAddr := make(map[common.Address]RegisteredPool, len(deps.Pools))
	for _, p := range deps.Pools {
		if _, ok := deps.Handlers[p.Kind]; !ok {
			return nil, fmt.Errorf("pool %s (id=%d) has kind %q but no handler registered for it", p.Address, p.ID, p.Kind)
		}
		poolsByAddr[p.Address] = p
	}

	return &Coordinator{
		poolsByAddr:     poolsByAddr,
		pools:           deps.Pools,
		handlers:        deps.Handlers,
		multicaller:     deps.Multicaller,
		repo:            deps.Repo,
		eventWriter:     deps.EventWriter,
		txMgr:           deps.TxManager,
		heartbeatBlocks: deps.HeartbeatBlocks,
		chainID:         deps.ChainID,
		logger:          deps.Logger,
		telemetry:       deps.Telemetry,
		lastSnapshot:    make(map[int64]snapshotKey),
	}, nil
}

// BlockHandler returns the dexconsumer.BlockHandler for this coordinator. It
// decodes every receipt in the block into local accumulators, snapshots the
// touched and heartbeat-due pools (via multicall, before opening the
// transaction), and persists swaps, liquidity events, captured logs, and pool
// state in one transaction. Returning a non-nil error leaves the block for SQS
// redelivery; nil is returned only after a successful commit. All per-block
// state is local, so a redelivery reprocesses from scratch with no carryover.
func (c *Coordinator) BlockHandler() dexconsumer.BlockHandler {
	return func(ctx context.Context, event outbound.BlockEvent, receipts []shared.TransactionReceipt) error {
		bn := event.BlockNumber
		ver := event.Version
		ts := time.Unix(event.BlockTimestamp, 0).UTC()

		var (
			swaps     []SwapRecord
			liquidity []LiquidityRecord
			captured  []CapturedEvent
		)
		touched := make(map[int64]RegisteredPool)

		for _, receipt := range receipts {
			// Bail early (with an error, never a silent ack) if the handler-timeout
			// budget or a shutdown cancelled ctx, rather than decoding the rest.
			if err := ctx.Err(); err != nil {
				return err
			}
			presentAddrs := make(map[common.Address]struct{}, len(receipt.Logs))
			for _, log := range receipt.Logs {
				presentAddrs[common.HexToAddress(log.Address)] = struct{}{}
			}
			for addr, pool := range c.poolsByAddr {
				if _, ok := presentAddrs[addr]; !ok {
					continue
				}
				decoded, err := c.handlers[pool.Kind].DecodeEvents(receipt, pool, c.chainID, bn, ver, ts)
				if err != nil {
					return fmt.Errorf("decoding events for pool %s block %d: %w", pool.Address, bn, err)
				}
				swaps = append(swaps, decoded.Swaps...)
				liquidity = append(liquidity, decoded.Liquidity...)
				captured = append(captured, decoded.Captured...)
				touched[pool.ID] = pool
			}
		}

		snapshotSet := c.buildSnapshotSet(bn, ver, touched)

		// Read pool state via multicall BEFORE opening the transaction so archive-RPC
		// latency never pins a pgx connection (connection-pool exhaustion is a stall cause).
		snapshots := make([]StateSnapshot, 0, len(snapshotSet))
		for _, pool := range snapshotSet {
			snap, err := c.handlers[pool.Kind].SnapshotState(ctx, c.multicaller, pool, bn, ver, ts)
			if err != nil {
				return fmt.Errorf("snapshotting pool %s block %d: %w", pool.Address, bn, err)
			}
			if err := snap.Validate(); err != nil {
				return fmt.Errorf("invalid snapshot for pool %s block %d: %w", pool.Address, bn, err)
			}
			snapshots = append(snapshots, snap)
		}

		// Quiet block: nothing decoded and no snapshot due. Skip the empty transaction.
		if len(swaps) == 0 && len(liquidity) == 0 && len(captured) == 0 && len(snapshots) == 0 {
			return nil
		}

		var stateRows int64
		err := c.txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
			for _, s := range swaps {
				if err := c.repo.SaveSwap(ctx, tx, toSwapInput(s, bn, ver, ts)); err != nil {
					return fmt.Errorf("saving swap block %d log %d: %w", bn, s.LogIndex, err)
				}
			}
			for _, l := range liquidity {
				if err := c.repo.SaveLiquidityEvent(ctx, tx, toLiquidityInput(l, bn, ver, ts)); err != nil {
					return fmt.Errorf("saving liquidity event block %d log %d: %w", bn, l.LogIndex, err)
				}
			}
			for _, cap := range captured {
				in := dexconsumer.ProtocolEventInput{
					ContractAddress: cap.Pool.Address,
					ChainID:         c.chainID,
					BlockNumber:     bn,
					BlockVersion:    ver,
					BlockTimestamp:  ts,
					TxHash:          cap.TxHash,
					LogIndex:        cap.LogIndex,
					EventName:       cap.EventName,
					Payload:         cap.Payload,
				}
				if err := c.eventWriter.Save(ctx, tx, in); err != nil {
					return fmt.Errorf("saving captured event block %d log %d: %w", bn, cap.LogIndex, err)
				}
			}
			for _, snap := range snapshots {
				switch {
				case snap.Stableswap != nil:
					n, err := c.repo.SaveStableswapState(ctx, tx, snap.Stableswap)
					if err != nil {
						return fmt.Errorf("saving stableswap state pool %s block %d: %w", snap.Pool.Address, bn, err)
					}
					stateRows += n
				case snap.Cryptoswap != nil:
					n, err := c.repo.SaveCryptoswapState(ctx, tx, snap.Cryptoswap)
					if err != nil {
						return fmt.Errorf("saving cryptoswap state pool %s block %d: %w", snap.Pool.Address, bn, err)
					}
					stateRows += n
				}
			}
			return nil
		})
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

// buildSnapshotSet returns the sorted (by pool.ID ASC) union of touched pools
// and heartbeat-due pools. Consistent ordering is required by the advisory-lock
// convention in the DB trigger.
func (c *Coordinator) buildSnapshotSet(bn int64, ver int, touched map[int64]RegisteredPool) []RegisteredPool {
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
