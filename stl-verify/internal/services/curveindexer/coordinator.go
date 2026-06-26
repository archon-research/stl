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

// blockKey identifies a (blockNumber, version) pair for the per-block buffer.
type blockKey struct {
	bn  int64
	ver int
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
// time, so no synchronisation is required on any Coordinator field.
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

	// per-block buffer
	curKey    blockKey
	swaps     []SwapRecord
	liquidity []LiquidityRecord
	captured  []CapturedEvent
	touched   map[int64]RegisteredPool
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
		touched:         make(map[int64]RegisteredPool),
	}, nil
}

// ReceiptHandler returns the dexconsumer.ReceiptHandler for this coordinator.
func (c *Coordinator) ReceiptHandler() dexconsumer.ReceiptHandler {
	return func(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, version int, ts time.Time) error {
		key := blockKey{bn: blockNumber, ver: version}
		if key != c.curKey {
			c.swaps = nil
			c.liquidity = nil
			c.captured = nil
			c.touched = make(map[int64]RegisteredPool)
			c.curKey = key
		}

		// Collect pool addresses present in this receipt's logs.
		presentAddrs := make(map[common.Address]struct{}, len(receipt.Logs))
		for _, log := range receipt.Logs {
			presentAddrs[common.HexToAddress(log.Address)] = struct{}{}
		}

		for addr, pool := range c.poolsByAddr {
			if _, touched := presentAddrs[addr]; !touched {
				continue
			}
			decoded, err := c.handlers[pool.Kind].DecodeEvents(receipt, pool, chainID, blockNumber, version, ts)
			if err != nil {
				return fmt.Errorf("decoding events for pool %s block %d: %w", pool.Address, blockNumber, err)
			}
			c.swaps = append(c.swaps, decoded.Swaps...)
			c.liquidity = append(c.liquidity, decoded.Liquidity...)
			c.captured = append(c.captured, decoded.Captured...)
			c.touched[pool.ID] = pool
		}
		return nil
	}
}

// Finalizer returns the dexconsumer.BlockFinalizer for this coordinator.
func (c *Coordinator) Finalizer() dexconsumer.BlockFinalizer {
	return func(ctx context.Context, event outbound.BlockEvent) error {
		bn := event.BlockNumber
		ver := event.Version
		ts := time.Unix(event.BlockTimestamp, 0).UTC()

		// Always clear the buffer on return so a failed finalize followed by SQS
		// redelivery re-accumulates from empty rather than doubling.
		defer func() {
			c.swaps = nil
			c.liquidity = nil
			c.captured = nil
			c.touched = make(map[int64]RegisteredPool)
		}()

		snapshotSet := c.buildSnapshotSet(bn, ver)

		err := c.txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
			for _, s := range c.swaps {
				if err := c.repo.SaveSwap(ctx, tx, toSwapInput(s, c.chainID, bn, ver, ts)); err != nil {
					return fmt.Errorf("saving swap block %d log %d: %w", bn, s.LogIndex, err)
				}
			}
			for _, l := range c.liquidity {
				if err := c.repo.SaveLiquidityEvent(ctx, tx, toLiquidityInput(l, bn, ver, ts)); err != nil {
					return fmt.Errorf("saving liquidity event block %d log %d: %w", bn, l.LogIndex, err)
				}
			}
			for _, cap := range c.captured {
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
			for _, pool := range snapshotSet {
				snap, err := c.handlers[pool.Kind].SnapshotState(ctx, c.multicaller, pool, bn, ver, ts)
				if err != nil {
					return fmt.Errorf("snapshotting pool %s block %d: %w", pool.Address, bn, err)
				}
				if err := snap.Validate(); err != nil {
					return fmt.Errorf("invalid snapshot for pool %s block %d: %w", pool.Address, bn, err)
				}
				switch {
				case snap.Stableswap != nil:
					if err := c.repo.SaveStableswapState(ctx, tx, snap.Stableswap); err != nil {
						return fmt.Errorf("saving stableswap state pool %s block %d: %w", pool.Address, bn, err)
					}
				case snap.Cryptoswap != nil:
					if err := c.repo.SaveCryptoswapState(ctx, tx, snap.Cryptoswap); err != nil {
						return fmt.Errorf("saving cryptoswap state pool %s block %d: %w", pool.Address, bn, err)
					}
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
		c.telemetry.RecordStateRows(ctx, len(snapshotSet))
		return nil
	}
}

// buildSnapshotSet returns the sorted (by pool.ID ASC) union of touched pools
// and heartbeat-due pools. Consistent ordering is required by the advisory-lock
// convention in the DB trigger.
func (c *Coordinator) buildSnapshotSet(bn int64, ver int) []RegisteredPool {
	byID := make(map[int64]RegisteredPool)
	maps.Copy(byID, c.touched)
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

func toSwapInput(s SwapRecord, chainID, bn int64, ver int, ts time.Time) outbound.SwapInput {
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
