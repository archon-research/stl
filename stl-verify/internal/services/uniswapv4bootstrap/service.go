// Package uniswapv4bootstrap discovers every historical Uniswap V4 LP position
// of the registered pools and snapshots each one at a single finality-safe
// block.
//
// It exists because the live indexer can only ever learn a position from a
// ModifyLiquidity log: v4-core exposes no enumeration, so a position minted
// before the indexer went live and never touched since is invisible to it
// forever. This one-shot run closes that hole by replaying the PoolManager's
// whole ModifyLiquidity history for those pools, then reading each discovered
// key's authoritative state through the same StateView getter, hash pinning and
// append-on-change write path the live indexer uses.
//
// A run keeps no progress state and is idempotent: the write path appends only
// where the stored value differs, so re-running writes nothing new.
package uniswapv4bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
)

// pinnedBlockVersion is the block_version every backfilled row carries. The pin
// sits past finality, so the height has exactly one canonical block and the
// live indexer's reorg versioning has nothing to disagree with.
const pinnedBlockVersion = 0

// Deps groups the Service's constructor arguments. Every field is required.
type Deps struct {
	// Pools is the full registry as LoadPools returned it; the run scans the
	// snapshot-supported subset, matching the live indexer's snapshot gate.
	Pools       []uniswapv4indexer.RegisteredPool
	LogScan     outbound.LogScanClient
	Multicaller outbound.Multicaller
	Repo        outbound.UniswapV4Repository
	TxManager   outbound.TxManager
	Logger      *slog.Logger
	Config      Config
}

// Service is one bootstrap run's collaborators, resolved once at construction.
type Service struct {
	pools       []uniswapv4indexer.RegisteredPool
	poolsByHash map[common.Hash]uniswapv4indexer.RegisteredPool
	poolManager common.Address
	topic0      common.Hash
	logScan     outbound.LogScanClient
	multicaller outbound.Multicaller
	repo        outbound.UniswapV4Repository
	txMgr       outbound.TxManager
	logger      *slog.Logger
	cfg         Config
}

// Summary is what a finished run reports, so the operator can tell a genuinely
// empty history from a scan that silently covered nothing.
type Summary struct {
	PinnedBlock     int64
	PinnedHash      common.Hash
	PinnedTimestamp time.Time
	FromBlock       int64
	Pools           int
	Scan            scanStats
	Keys            int
	KeysByPool      map[int64]int
	PositionsRead   int
	Batches         int
}

// New validates deps and resolves the log filter's fixed parts. It refuses a
// registry the live indexer would also refuse — PoolIds that disagree with
// their keys, or more than one PoolManager deployment — plus one with no
// snapshot-supported pool at all, which would scan the chain and write nothing.
func New(deps Deps) (*Service, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}
	cfg := deps.Config.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := uniswapv4indexer.ValidatePoolKeys(deps.Pools); err != nil {
		return nil, err
	}
	poolManager, err := uniswapv4indexer.PoolManagerFor(deps.Pools)
	if err != nil {
		return nil, err
	}

	pools := snapshottablePoolsByID(deps.Pools)
	if len(pools) == 0 {
		return nil, fmt.Errorf("no registered uniswap v4 pool on chain %d is snapshot_supported: nothing to bootstrap", cfg.ChainID)
	}
	topic0, err := uniswapv4indexer.ModifyLiquidityTopic0()
	if err != nil {
		return nil, err
	}

	return &Service{
		pools:       pools,
		poolsByHash: poolsByHash(pools),
		poolManager: poolManager,
		topic0:      topic0,
		logScan:     deps.LogScan,
		multicaller: deps.Multicaller,
		repo:        deps.Repo,
		txMgr:       deps.TxManager,
		logger:      deps.Logger,
		cfg:         cfg,
	}, nil
}

func (d Deps) validate() error {
	switch {
	case len(d.Pools) == 0:
		return fmt.Errorf("at least one pool is required")
	case d.LogScan == nil:
		return fmt.Errorf("log scan client is required")
	case d.Multicaller == nil:
		return fmt.Errorf("multicaller is required")
	case d.Repo == nil:
		return fmt.Errorf("repo is required")
	case d.TxManager == nil:
		return fmt.Errorf("txManager is required")
	case d.Logger == nil:
		return fmt.Errorf("logger is required")
	}
	return nil
}

// snapshottablePoolsByID returns the snapshot-supported pools in ascending
// surrogate-id order, so a run's reads, writes and logs are deterministic.
func snapshottablePoolsByID(all []uniswapv4indexer.RegisteredPool) []uniswapv4indexer.RegisteredPool {
	pools := uniswapv4indexer.SnapshottablePools(all)
	slices.SortFunc(pools, func(a, b uniswapv4indexer.RegisteredPool) int {
		return int(a.ID - b.ID)
	})
	return pools
}

func poolsByHash(pools []uniswapv4indexer.RegisteredPool) map[common.Hash]uniswapv4indexer.RegisteredPool {
	byHash := make(map[common.Hash]uniswapv4indexer.RegisteredPool, len(pools))
	for _, pool := range pools {
		byHash[pool.PoolIDHash] = pool
	}
	return byHash
}

// Run executes the whole bootstrap: pin a finality-safe block, replay the
// ModifyLiquidity history up to it, prove the pin is still canonical, then read
// and persist every discovered position at that block. Any failure stops the
// run; already-committed batches stay, and re-running redoes the work
// idempotently.
func (s *Service) Run(ctx context.Context) (Summary, error) {
	pin, err := pinBlock(ctx, s.logScan, s.cfg.FinalityDepth, s.cfg.PinBlock)
	if err != nil {
		return Summary{}, err
	}
	from, err := s.scanStart(pin)
	if err != nil {
		return Summary{}, err
	}
	s.logStart(pin, from)

	keysByPool, stats, err := s.discoverPositionKeys(ctx, from, pin.number)
	if err != nil {
		return Summary{}, err
	}

	// Before any write: a pin that moved invalidates every key just discovered
	// and every read about to be issued, so the cheap check goes first.
	if err := assertPinStable(ctx, s.logScan, pin); err != nil {
		return Summary{}, err
	}

	summary := s.newSummary(pin, from, stats, keysByPool)
	if err := s.snapshotAndPersist(ctx, keysByPool, pin, &summary); err != nil {
		return Summary{}, err
	}

	s.logger.Info("uniswap-v4 position bootstrap complete",
		"chainId", s.cfg.ChainID, "pinnedBlock", summary.PinnedBlock, "pools", summary.Pools,
		"keys", summary.Keys, "positionsRead", summary.PositionsRead, "batches", summary.Batches,
		"scanWindows", summary.Scan.windows, "scanNarrowings", summary.Scan.narrowings, "scanLogs", summary.Scan.logs)
	return summary, nil
}

func (s *Service) logStart(pin pinnedBlock, from int64) {
	poolIDs := make([]string, len(s.pools))
	for i, pool := range s.pools {
		poolIDs[i] = pool.PoolIDHash.Hex()
	}
	s.logger.Info("starting uniswap-v4 position bootstrap",
		"chainId", s.cfg.ChainID, "poolManager", s.poolManager, "pools", len(s.pools), "poolIds", poolIDs,
		"fromBlock", from, "pinnedBlock", pin.number, "pinnedHash", pin.hash, "pinnedTimestamp", pin.ts,
		"initialWindow", s.cfg.InitialWindow, "positionBatch", s.cfg.PositionBatch)
}

// scanStart resolves the first block to scan: the override when set, otherwise
// the lowest deploy height among the scanned pools, below which no position of
// theirs can exist.
func (s *Service) scanStart(pin pinnedBlock) (int64, error) {
	from := s.cfg.FromBlock
	if from == 0 {
		from = s.pools[0].DeployBlock
		for _, pool := range s.pools[1:] {
			from = min(from, pool.DeployBlock)
		}
	}
	if from > pin.number {
		return 0, fmt.Errorf("scan start %d is above the pinned block %d: nothing to scan", from, pin.number)
	}
	return from, nil
}

func (s *Service) newSummary(pin pinnedBlock, from int64, stats scanStats, keysByPool map[int64][]entity.UniswapV4PositionKey) Summary {
	summary := Summary{
		PinnedBlock:     pin.number,
		PinnedHash:      pin.hash,
		PinnedTimestamp: pin.ts,
		FromBlock:       from,
		Pools:           len(s.pools),
		Scan:            stats,
		KeysByPool:      make(map[int64]int, len(keysByPool)),
	}
	for poolID, keys := range keysByPool {
		summary.KeysByPool[poolID] = len(keys)
		summary.Keys += len(keys)
	}
	return summary
}

// discoverPositionKeys replays [from, to]'s ModifyLiquidity logs and returns
// the deduplicated, Compare-sorted position keys per pool.
func (s *Service) discoverPositionKeys(ctx context.Context, from, to int64) (map[int64][]entity.UniswapV4PositionKey, scanStats, error) {
	scanner := &logWindowScanner{
		client: s.logScan,
		filter: s.baseFilter(),
		policy: windowPolicy{initial: s.cfg.InitialWindow, min: s.cfg.MinWindow, max: s.cfg.MaxWindow},
		logger: s.logger,
	}

	found := make(map[int64][]entity.UniswapV4PositionKey)
	stats, err := scanner.scan(ctx, from, to, func(w logWindow) error {
		windowKeys, err := uniswapv4indexer.PositionKeysFromLogs(toSharedLogs(w.logs), s.poolsByHash, s.poolManager)
		if err != nil {
			return err
		}
		for poolID, keys := range windowKeys {
			found[poolID] = append(found[poolID], keys...)
		}
		return nil
	})
	if err != nil {
		return nil, stats, err
	}

	for poolID, keys := range found {
		found[poolID] = uniswapv4indexer.MergePositionKeys(keys, nil)
	}
	s.logger.Info("uniswap-v4 position discovery complete",
		"chainId", s.cfg.ChainID, "fromBlock", from, "toBlock", to,
		"windows", stats.windows, "narrowings", stats.narrowings, "logs", stats.logs, "poolsWithKeys", len(found))
	return found, stats, nil
}

// baseFilter is the log filter minus its range: the singleton PoolManager, the
// ModifyLiquidity signature, and the registered pools' on-chain ids as the
// topic1 OR-set. Narrowing to those ids at the node is what keeps the scan off
// the thousands of untracked pools the singleton also emits for.
func (s *Service) baseFilter() outbound.LogFilter {
	poolIDs := make([]common.Hash, len(s.pools))
	for i, pool := range s.pools {
		poolIDs[i] = pool.PoolIDHash
	}
	return outbound.LogFilter{
		Address: s.poolManager,
		Topic0:  s.topic0,
		Topic1:  poolIDs,
	}
}

// toSharedLogs re-shapes the port's wire logs into the decoder's log type. The
// hex strings are copied verbatim: validating them is the decoder's job, and
// normalising anything here would defeat its guard.
func toSharedLogs(logs []outbound.FilteredLog) []shared.Log {
	out := make([]shared.Log, len(logs))
	for i, l := range logs {
		out[i] = shared.Log{
			Address:          l.Address,
			Topics:           l.Topics,
			Data:             l.Data,
			BlockHash:        l.BlockHash,
			BlockNumber:      l.BlockNumber,
			TransactionHash:  l.TransactionHash,
			TransactionIndex: l.TransactionIndex,
			LogIndex:         l.LogIndex,
			Removed:          l.Removed,
		}
	}
	return out
}

// snapshotAndPersist reads and writes each pool's positions in bounded batches,
// pool by pool in ascending id order.
func (s *Service) snapshotAndPersist(
	ctx context.Context,
	keysByPool map[int64][]entity.UniswapV4PositionKey,
	pin pinnedBlock,
	summary *Summary,
) error {
	for _, pool := range s.pools {
		keys := keysByPool[pool.ID]
		if len(keys) == 0 {
			s.logger.Info("uniswap-v4 pool has no historical positions",
				"chainId", s.cfg.ChainID, "poolRowId", pool.ID, "poolId", pool.PoolIDHash.Hex())
			continue
		}
		if err := s.snapshotPool(ctx, pool, keys, pin, summary); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) snapshotPool(
	ctx context.Context,
	pool uniswapv4indexer.RegisteredPool,
	keys []entity.UniswapV4PositionKey,
	pin pinnedBlock,
	summary *Summary,
) error {
	batch := 0
	for chunk := range slices.Chunk(keys, s.cfg.PositionBatch) {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch++

		rows, err := uniswapv4indexer.ReadPositions(ctx, s.multicaller, pool, chunk, pin.hash, pin.number, pinnedBlockVersion, pin.ts)
		if err != nil {
			return fmt.Errorf("reading positions batch %d of pool %s at block %d: %w", batch, pool.PoolIDHash, pin.number, err)
		}
		if err := s.persist(ctx, rows); err != nil {
			return fmt.Errorf("persisting positions batch %d of pool %s at block %d: %w", batch, pool.PoolIDHash, pin.number, err)
		}

		summary.PositionsRead += len(rows)
		summary.Batches++
		s.logger.Info("persisted uniswap-v4 position batch",
			"chainId", s.cfg.ChainID, "poolRowId", pool.ID, "poolId", pool.PoolIDHash.Hex(),
			"batch", batch, "positions", len(rows), "poolKeys", len(keys),
			"poolPositionsDone", summary.PositionsRead, "pinnedBlock", pin.number)
	}
	return nil
}

// persist writes one batch through the repository's append-on-change path,
// which is what makes a rerun a no-op: it appends only where the stored value
// for the slot differs.
func (s *Service) persist(ctx context.Context, rows []*entity.UniswapV4Position) error {
	if len(rows) == 0 {
		return nil
	}
	return s.txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		_, err := s.repo.SaveBlock(ctx, tx, outbound.UniswapV4BlockWrites{Positions: rows})
		return err
	})
}
