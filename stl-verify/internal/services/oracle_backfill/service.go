// Package oracle_backfill provides a historical backfill service for onchain oracle prices.
// It fetches oracle prices for a block range using a worker pool and stores price changes
// in the database.
package oracle_backfill

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// BlockHeaderFetcher retrieves block headers from an Ethereum node.
// Satisfied by *ethclient.Client.
type BlockHeaderFetcher interface {
	HeaderByNumber(ctx context.Context, number *big.Int) (*ethtypes.Header, error)
}

// MulticallFactory creates a new Multicaller instance.
// Each backfill worker needs its own multicall client for concurrent use.
type MulticallFactory func() (outbound.Multicaller, error)

// Config holds configuration for the backfill service.
type Config struct {
	Concurrency  int
	BatchSize    int
	OracleSource string
	Logger       *slog.Logger
}

func configDefaults() Config {
	return Config{
		Concurrency:  100,
		BatchSize:    1000,
		OracleSource: "sparklend",
		Logger:       slog.Default(),
	}
}

// Service orchestrates parallel oracle price backfilling.
type Service struct {
	config         Config
	headerFetcher  BlockHeaderFetcher
	newMulticaller MulticallFactory
	repo           outbound.OnchainPriceRepository

	providerABI *abi.ABI
	oracleABI   *abi.ABI

	// tokenAddressMap is the token_id → on-chain address mapping.
	tokenAddressMap map[int64]common.Address

	logger *slog.Logger
}

// NewService creates a new oracle backfill service.
func NewService(
	config Config,
	headerFetcher BlockHeaderFetcher,
	newMulticaller MulticallFactory,
	repo outbound.OnchainPriceRepository,
	tokenAddresses map[int64]common.Address,
) (*Service, error) {
	if headerFetcher == nil {
		return nil, fmt.Errorf("headerFetcher cannot be nil")
	}
	if newMulticaller == nil {
		return nil, fmt.Errorf("newMulticaller cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if len(tokenAddresses) == 0 {
		return nil, fmt.Errorf("tokenAddresses cannot be empty")
	}

	defaults := configDefaults()
	if config.Concurrency <= 0 {
		config.Concurrency = defaults.Concurrency
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.OracleSource == "" {
		config.OracleSource = defaults.OracleSource
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	providerABI, err := abis.GetPoolAddressProviderABI()
	if err != nil {
		return nil, fmt.Errorf("loading PoolAddressProvider ABI: %w", err)
	}

	oracleABI, err := abis.GetSparkLendOracleABI()
	if err != nil {
		return nil, fmt.Errorf("loading SparkLend Oracle ABI: %w", err)
	}

	return &Service{
		config:          config,
		headerFetcher:   headerFetcher,
		newMulticaller:  newMulticaller,
		repo:            repo,
		providerABI:     providerABI,
		oracleABI:       oracleABI,
		tokenAddressMap: tokenAddresses,
		logger:          config.Logger.With("component", "oracle-backfill"),
	}, nil
}

// Run executes the backfill for the given block range.
func (s *Service) Run(ctx context.Context, fromBlock, toBlock int64) error {
	source, err := s.repo.GetOracleSource(ctx, s.config.OracleSource)
	if err != nil {
		return fmt.Errorf("getting oracle source: %w", err)
	}

	assets, err := s.repo.GetEnabledAssets(ctx, source.ID)
	if err != nil {
		return fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return fmt.Errorf("no enabled assets for oracle source %s", source.Name)
	}

	providerAddr := common.BytesToAddress(source.PoolAddressProvider)

	// Build ordered token address and ID lists
	tokenAddrs := make([]common.Address, len(assets))
	tokenIDs := make([]int64, len(assets))
	for i, asset := range assets {
		addr, ok := s.tokenAddressMap[asset.TokenID]
		if !ok {
			return fmt.Errorf("token address not found for token_id %d", asset.TokenID)
		}
		tokenAddrs[i] = addr
		tokenIDs[i] = asset.TokenID
	}

	// Resume support: skip already-processed blocks
	latestBlock, err := s.repo.GetLatestBlock(ctx, source.ID)
	if err != nil {
		return fmt.Errorf("getting latest block: %w", err)
	}
	if latestBlock > 0 && latestBlock >= fromBlock {
		s.logger.Info("resuming from latest stored block",
			"latestStored", latestBlock,
			"requestedFrom", fromBlock)
		fromBlock = latestBlock + 1
	}

	totalBlocks := toBlock - fromBlock + 1
	if totalBlocks <= 0 {
		s.logger.Info("no blocks to process", "from", fromBlock, "to", toBlock)
		return nil
	}

	s.logger.Info("starting backfill",
		"from", fromBlock,
		"to", toBlock,
		"blocks", totalBlocks,
		"concurrency", s.config.Concurrency,
		"assets", len(assets))

	// Progress tracking
	var stats backfillStats
	stats.startTime = time.Now()
	stopProgress := s.startProgressReporter(ctx, &stats, totalBlocks)
	defer stopProgress()

	// Child context: cancelled if batchWriter fails so workers stop promptly.
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	// Batch collector channel: workers send prices here for batch DB inserts
	priceCh := make(chan []*entity.OnchainTokenPrice, s.config.Concurrency*2)

	// Start batch writer goroutine
	writerDone := make(chan error, 1)
	go func() {
		err := s.batchWriter(workerCtx, priceCh, &stats)
		if err != nil {
			workerCancel() // signal workers to stop
		}
		writerDone <- err
	}()

	// Split block range among workers (contiguous sub-ranges for change detection)
	workerCount := s.config.Concurrency
	if int64(workerCount) > totalBlocks {
		workerCount = int(totalBlocks)
	}
	blocksPerWorker := totalBlocks / int64(workerCount)

	var wg sync.WaitGroup
	for i := range workerCount {
		workerFrom := fromBlock + int64(i)*blocksPerWorker
		workerTo := workerFrom + blocksPerWorker - 1
		if i == workerCount-1 {
			workerTo = toBlock // last worker takes remaining blocks
		}

		wg.Add(1)
		go func(wFrom, wTo int64) {
			defer wg.Done()
			s.worker(workerCtx, wFrom, wTo, source, providerAddr, tokenAddrs, tokenIDs, priceCh, &stats)
		}(workerFrom, workerTo)
	}

	wg.Wait()
	close(priceCh)

	// Wait for batch writer to finish
	if err := <-writerDone; err != nil {
		return fmt.Errorf("batch writer: %w", err)
	}

	s.logger.Info("backfill complete",
		"blocks", stats.blocksProcessed.Load(),
		"pricesStored", stats.pricesStored.Load(),
		"errors", stats.blocksFailed.Load(),
		"duration", time.Since(stats.startTime))

	return nil
}

type backfillStats struct {
	blocksProcessed atomic.Int64
	blocksFailed    atomic.Int64
	pricesStored    atomic.Int64
	startTime       time.Time
}

func (s *Service) worker(
	ctx context.Context,
	fromBlock, toBlock int64,
	source *entity.OracleSource,
	providerAddr common.Address,
	tokenAddrs []common.Address,
	tokenIDs []int64,
	priceCh chan<- []*entity.OnchainTokenPrice,
	stats *backfillStats,
) {
	mc, err := s.newMulticaller()
	if err != nil {
		s.logger.Error("failed to create multicall client", "error", err)
		return
	}

	oracleSourceID := int16(source.ID)
	oracleAddr := common.Address{} // will be set on first block

	// Per-worker price cache for change detection within this contiguous range.
	// First block in range initializes from "all new" (or DB if needed).
	prevPrices := make(map[int64]float64)

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		if ctx.Err() != nil {
			return
		}

		prices, newOracleAddr, err := s.processBlock(ctx, mc, providerAddr, oracleAddr, tokenAddrs, tokenIDs, oracleSourceID, blockNum)
		if err != nil {
			s.logger.Error("failed to process block",
				"block", blockNum,
				"error", err)
			stats.blocksFailed.Add(1)
			continue
		}

		oracleAddr = newOracleAddr

		// Change detection: only keep prices that differ from previous block
		var changed []*entity.OnchainTokenPrice
		for _, p := range prices {
			if prev, ok := prevPrices[p.TokenID]; ok && prev == p.PriceUSD {
				continue
			}
			changed = append(changed, p)
			prevPrices[p.TokenID] = p.PriceUSD
		}

		stats.blocksProcessed.Add(1)

		if len(changed) > 0 {
			select {
			case priceCh <- changed:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (s *Service) processBlock(
	ctx context.Context,
	mc outbound.Multicaller,
	providerAddr, cachedOracleAddr common.Address,
	tokenAddrs []common.Address,
	tokenIDs []int64,
	oracleSourceID int16,
	blockNum int64,
) ([]*entity.OnchainTokenPrice, common.Address, error) {
	// Fetch oracle prices via multicall
	result, err := blockchain.FetchOraclePrices(
		ctx, mc, s.providerABI, s.oracleABI,
		providerAddr, cachedOracleAddr,
		tokenAddrs, blockNum,
	)
	if err != nil {
		return nil, cachedOracleAddr, fmt.Errorf("fetching oracle prices: %w", err)
	}

	if len(result.Prices) != len(tokenIDs) {
		return nil, result.OracleAddress, fmt.Errorf("price count mismatch: expected %d, got %d", len(tokenIDs), len(result.Prices))
	}

	// Get block timestamp
	header, err := s.headerFetcher.HeaderByNumber(ctx, new(big.Int).SetInt64(blockNum))
	if err != nil {
		return nil, result.OracleAddress, fmt.Errorf("getting block header: %w", err)
	}
	blockTimestamp := time.Unix(int64(header.Time), 0).UTC()
	oracleAddrBytes := result.OracleAddress.Bytes()

	prices := make([]*entity.OnchainTokenPrice, 0, len(tokenIDs))
	for i, rawPrice := range result.Prices {
		priceUSD := blockchain.ConvertOraclePriceToUSD(rawPrice)

		p, err := entity.NewOnchainTokenPrice(
			tokenIDs[i],
			oracleSourceID,
			blockNum,
			0, // block_version = 0 for historical backfill
			blockTimestamp,
			oracleAddrBytes,
			priceUSD,
		)
		if err != nil {
			s.logger.Error("invalid price entity", "tokenID", tokenIDs[i], "error", err)
			continue
		}
		prices = append(prices, p)
	}

	return prices, result.OracleAddress, nil
}

func (s *Service) batchWriter(ctx context.Context, priceCh <-chan []*entity.OnchainTokenPrice, stats *backfillStats) error {
	batch := make([]*entity.OnchainTokenPrice, 0, s.config.BatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.repo.UpsertPrices(ctx, batch); err != nil {
			return fmt.Errorf("upserting batch: %w", err)
		}
		stats.pricesStored.Add(int64(len(batch)))
		batch = batch[:0]
		return nil
	}

	for prices := range priceCh {
		batch = append(batch, prices...)
		if len(batch) >= s.config.BatchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	return flush()
}

func (s *Service) startProgressReporter(ctx context.Context, stats *backfillStats, totalBlocks int64) func() {
	ticker := time.NewTicker(2 * time.Second)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				processed := stats.blocksProcessed.Load()
				failed := stats.blocksFailed.Load()
				stored := stats.pricesStored.Load()
				elapsed := time.Since(stats.startTime)
				blocksPerSec := float64(processed) / elapsed.Seconds()

				remaining := totalBlocks - processed - failed
				var eta time.Duration
				if blocksPerSec > 0 {
					eta = time.Duration(float64(remaining)/blocksPerSec) * time.Second
				}

				s.logger.Info("progress",
					"processed", processed,
					"failed", failed,
					"stored", stored,
					"total", totalBlocks,
					"blocks/s", fmt.Sprintf("%.0f", blocksPerSec),
					"eta", eta.Round(time.Second))
			case <-ctx.Done():
				return
			}
		}
	}()

	return func() {
		close(done)
		ticker.Stop()
	}
}
