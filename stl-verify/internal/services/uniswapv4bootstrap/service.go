// Package uniswapv4bootstrap backfills the LP positions the live indexer can
// never see: v4-core exposes no position enumeration, so one minted before the
// indexer went live and never touched since emits no log it could learn from.
package uniswapv4bootstrap

import (
	"cmp"
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

// Past finality the height has one canonical block, so the live indexer's
// reorg versioning has nothing to disagree with.
const pinnedBlockVersion = 0

type Deps struct {
	Pools       []uniswapv4indexer.RegisteredPool
	LogScan     outbound.LogScanClient
	Multicaller outbound.Multicaller
	Repo        outbound.UniswapV4Repository
	TxManager   outbound.TxManager
	Logger      *slog.Logger
	Config      Config
}

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

type Summary struct {
	PinnedBlock      int64
	PinnedHash       common.Hash
	PinnedTimestamp  time.Time
	FromBlock        int64
	Pools            int
	ScanWindows      int
	ScanNarrowings   int
	ScanLogs         int
	Keys             int
	KeysByPool       map[int64]int
	PositionsRead    int
	PositionsWritten int64
	Batches          int
}

func (s *Summary) recordScan(stats scanStats) {
	s.ScanWindows = stats.windows
	s.ScanNarrowings = stats.narrowings
	s.ScanLogs = stats.logs
}

func (s *Summary) recordKeys(keysByPool map[int64][]entity.UniswapV4PositionKey) {
	s.KeysByPool = make(map[int64]int, len(keysByPool))
	for poolID, keys := range keysByPool {
		s.KeysByPool[poolID] = len(keys)
		s.Keys += len(keys)
	}
}

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

func snapshottablePoolsByID(all []uniswapv4indexer.RegisteredPool) []uniswapv4indexer.RegisteredPool {
	pools := uniswapv4indexer.SnapshottablePools(all)
	slices.SortFunc(pools, func(a, b uniswapv4indexer.RegisteredPool) int {
		return cmp.Compare(a.ID, b.ID)
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

func (s *Service) Run(ctx context.Context) (Summary, error) {
	pin, err := pinBlock(ctx, s.logScan, s.cfg.FinalityDepth, s.cfg.PinBlock)
	if err != nil {
		return Summary{}, err
	}
	summary := Summary{
		PinnedBlock:     pin.number,
		PinnedHash:      pin.hash,
		PinnedTimestamp: pin.ts,
		Pools:           len(s.pools),
	}

	from, err := s.scanStart(pin)
	if err != nil {
		return summary, err
	}
	summary.FromBlock = from
	s.logStart(pin, from)

	keysByPool, stats, err := s.discoverPositionKeys(ctx, from, pin.number)
	summary.recordScan(stats)
	if err != nil {
		return summary, err
	}
	summary.recordKeys(keysByPool)

	// A pin that moved invalidates every key just discovered, so this precedes any write.
	if err := assertPinStable(ctx, s.logScan, pin); err != nil {
		return summary, err
	}
	if err := s.snapshotAndPersist(ctx, keysByPool, pin, &summary); err != nil {
		return summary, err
	}

	s.logComplete(summary)
	return summary, nil
}

func (s *Service) logComplete(summary Summary) {
	s.logger.Info("uniswap-v4 position bootstrap complete",
		"chainId", s.cfg.ChainID, "pinnedBlock", summary.PinnedBlock, "pools", summary.Pools,
		"keys", summary.Keys, "positionsRead", summary.PositionsRead,
		"positionsWritten", summary.PositionsWritten, "batches", summary.Batches,
		"scanWindows", summary.ScanWindows, "scanNarrowings", summary.ScanNarrowings, "scanLogs", summary.ScanLogs)
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

// Topic1 narrows at the node to the registered pools: the singleton PoolManager
// also emits ModifyLiquidity for the thousands of pools nothing here tracks.
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

// The hex strings are copied verbatim: normalising any of them here would
// defeat the decoder's validation guard.
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
	batch, poolPositionsDone := 0, 0
	for chunk := range slices.Chunk(keys, s.cfg.PositionBatch) {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch++

		rows, err := uniswapv4indexer.ReadPositions(ctx, s.multicaller, pool, chunk, pin.hash, pin.number, pinnedBlockVersion, pin.ts)
		if err != nil {
			return fmt.Errorf("reading positions batch %d of pool %s at block %d: %w", batch, pool.PoolIDHash, pin.number, err)
		}
		written, err := s.persist(ctx, rows)
		if err != nil {
			return fmt.Errorf("persisting positions batch %d of pool %s at block %d: %w", batch, pool.PoolIDHash, pin.number, err)
		}

		poolPositionsDone += len(rows)
		summary.PositionsRead += len(rows)
		summary.PositionsWritten += written
		summary.Batches++
		s.logger.Info("persisted uniswap-v4 position batch",
			"chainId", s.cfg.ChainID, "poolRowId", pool.ID, "poolId", pool.PoolIDHash.Hex(),
			"batch", batch, "positions", len(rows), "positionsWritten", written, "poolKeys", len(keys),
			"poolPositionsDone", poolPositionsDone, "pinnedBlock", pin.number)
	}
	return nil
}

func (s *Service) persist(ctx context.Context, rows []*entity.UniswapV4Position) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	var written int64
	err := s.txMgr.WithTransaction(ctx, func(tx pgx.Tx) error {
		var saveErr error
		written, saveErr = s.repo.SavePositions(ctx, tx, rows)
		return saveErr
	})
	if err != nil {
		return 0, err
	}
	return written, nil
}
