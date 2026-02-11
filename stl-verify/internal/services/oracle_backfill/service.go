// Package oracle_backfill provides a historical backfill service for onchain oracle prices.
// It fetches oracle prices for a block range using a worker pool and stores price changes
// in the database. All oracles are loaded from the DB — no hardcoded oracle configuration.
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
	Concurrency int
	BatchSize   int
	Logger      *slog.Logger
}

func configDefaults() Config {
	return Config{
		Concurrency: 100,
		BatchSize:   1000,
		Logger:      slog.Default(),
	}
}

// oracleWorkUnit holds everything needed to fetch prices for one oracle.
type oracleWorkUnit struct {
	oracle     *entity.Oracle
	oracleAddr common.Address
	tokenAddrs []common.Address
	tokenIDs   []int64
	validFrom  int64 // earliest block to query (0 = no lower bound)
	validTo    int64 // latest block to query (0 = no upper bound)
}

// oracleBlockRange represents the valid block range for an oracle across all protocols.
type oracleBlockRange struct {
	validFrom int64
	validTo   int64 // 0 = no upper bound (still active)
}

// Service orchestrates parallel oracle price backfilling.
type Service struct {
	config         Config
	headerFetcher  BlockHeaderFetcher
	newMulticaller MulticallFactory
	repo           outbound.OnchainPriceRepository

	oracleABI *abi.ABI

	logger *slog.Logger
}

// NewService creates a new oracle backfill service.
func NewService(
	config Config,
	headerFetcher BlockHeaderFetcher,
	newMulticaller MulticallFactory,
	repo outbound.OnchainPriceRepository,
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

	defaults := configDefaults()
	if config.Concurrency <= 0 {
		config.Concurrency = defaults.Concurrency
	}
	if config.BatchSize <= 0 {
		config.BatchSize = defaults.BatchSize
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		return nil, fmt.Errorf("loading Oracle ABI: %w", err)
	}

	return &Service{
		config:         config,
		headerFetcher:  headerFetcher,
		newMulticaller: newMulticaller,
		repo:           repo,
		oracleABI:      oracleABI,
		logger:         config.Logger.With("component", "oracle-backfill"),
	}, nil
}

// Run executes the backfill for the given block range across all enabled oracles.
func (s *Service) Run(ctx context.Context, fromBlock, toBlock int64) error {
	workUnits, err := s.buildOracleWorkUnits(ctx)
	if err != nil {
		return err
	}
	if len(workUnits) == 0 {
		s.logger.Info("no oracles with enabled assets found")
		return nil
	}

	s.logger.Info("loaded oracles for backfill", "count", len(workUnits))

	for _, wu := range workUnits {
		if err := s.runForOracle(ctx, wu, fromBlock, toBlock); err != nil {
			return fmt.Errorf("backfilling oracle %s: %w", wu.oracle.Name, err)
		}
	}

	return nil
}

// buildOracleWorkUnits loads all enabled oracles from DB, deduplicates by oracle_id,
// and builds the per-oracle data structures needed for price fetching.
func (s *Service) buildOracleWorkUnits(ctx context.Context) ([]*oracleWorkUnit, error) {
	// Load all enabled oracles (generic + protocol-bound)
	allOracles, err := s.repo.GetAllEnabledOracles(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting enabled oracles: %w", err)
	}

	// Load all protocol-oracle bindings to compute valid block ranges.
	bindings, err := s.repo.GetAllProtocolOracleBindings(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting protocol oracle bindings: %w", err)
	}
	blockRanges := computeOracleBlockRanges(bindings)

	// Deduplicate by oracle ID (a protocol oracle may also exist as a generic oracle)
	seen := make(map[int64]bool)
	var workUnits []*oracleWorkUnit

	for _, oracle := range allOracles {
		if seen[oracle.ID] {
			continue
		}
		seen[oracle.ID] = true

		wu, err := s.buildWorkUnit(ctx, oracle)
		if err != nil {
			s.logger.Warn("skipping oracle", "name", oracle.Name, "error", err)
			continue
		}
		if wu == nil {
			continue
		}

		// Set valid block ranges from deployment block and protocol bindings.
		wu.validFrom = oracle.DeploymentBlock
		if br, ok := blockRanges[oracle.ID]; ok {
			if br.validFrom > wu.validFrom {
				wu.validFrom = br.validFrom
			}
			wu.validTo = br.validTo
		}

		workUnits = append(workUnits, wu)
	}

	return workUnits, nil
}

func (s *Service) buildWorkUnit(ctx context.Context, oracle *entity.Oracle) (*oracleWorkUnit, error) {
	assets, err := s.repo.GetEnabledAssets(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting enabled assets: %w", err)
	}
	if len(assets) == 0 {
		return nil, nil
	}

	tokenAddrBytes, err := s.repo.GetTokenAddresses(ctx, oracle.ID)
	if err != nil {
		return nil, fmt.Errorf("getting token addresses: %w", err)
	}

	tokenAddrs := make([]common.Address, len(assets))
	tokenIDs := make([]int64, len(assets))
	for i, asset := range assets {
		addrBytes, ok := tokenAddrBytes[asset.TokenID]
		if !ok {
			return nil, fmt.Errorf("token address not found for token_id %d", asset.TokenID)
		}
		tokenAddrs[i] = common.BytesToAddress(addrBytes)
		tokenIDs[i] = asset.TokenID
	}

	return &oracleWorkUnit{
		oracle:     oracle,
		oracleAddr: common.Address(oracle.Address),
		tokenAddrs: tokenAddrs,
		tokenIDs:   tokenIDs,
	}, nil
}

func (s *Service) runForOracle(ctx context.Context, wu *oracleWorkUnit, fromBlock, toBlock int64) error {
	// Clamp the requested block range to this oracle's valid range.
	var ok bool
	fromBlock, toBlock, ok = clampBlockRange(fromBlock, toBlock, wu.validFrom, wu.validTo)
	if !ok {
		s.logger.Info("no blocks to process after clamping to oracle valid range",
			"oracle", wu.oracle.Name,
			"validFrom", wu.validFrom,
			"validTo", wu.validTo)
		return nil
	}

	// Resume support: if the latest stored block falls within the requested
	// range, skip ahead to avoid re-processing. If it's outside the range
	// (e.g. backfilling an earlier period), run the full requested range.
	// Duplicate blocks are safe thanks to ON CONFLICT DO NOTHING.
	latestBlock, err := s.repo.GetLatestBlock(ctx, wu.oracle.ID)
	if err != nil {
		return fmt.Errorf("getting latest block: %w", err)
	}
	if latestBlock > 0 && latestBlock >= fromBlock && latestBlock < toBlock {
		s.logger.Info("resuming from latest stored block",
			"oracle", wu.oracle.Name,
			"latestStored", latestBlock,
			"requestedFrom", fromBlock)
		fromBlock = latestBlock + 1
	}

	totalBlocks := toBlock - fromBlock + 1
	if totalBlocks <= 0 {
		s.logger.Info("no blocks to process", "oracle", wu.oracle.Name, "from", fromBlock, "to", toBlock)
		return nil
	}

	s.logger.Info("starting backfill",
		"oracle", wu.oracle.Name,
		"from", fromBlock,
		"to", toBlock,
		"blocks", totalBlocks,
		"concurrency", s.config.Concurrency,
		"assets", len(wu.tokenIDs))

	// Progress tracking
	var stats backfillStats
	stats.startTime = time.Now()
	stopProgress := s.startProgressReporter(ctx, &stats, totalBlocks, wu.oracle.Name)
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
	remainder := totalBlocks % int64(workerCount)

	var wg sync.WaitGroup
	cursor := fromBlock
	for i := range workerCount {
		workerFrom := cursor
		workerTo := workerFrom + blocksPerWorker - 1
		if int64(i) < remainder {
			workerTo++ // distribute remainder evenly across first N workers
		}
		cursor = workerTo + 1

		wg.Add(1)
		go func(wFrom, wTo int64) {
			defer wg.Done()
			s.worker(workerCtx, wFrom, wTo, wu, priceCh, &stats)
		}(workerFrom, workerTo)
	}

	wg.Wait()
	close(priceCh)

	// Wait for batch writer to finish
	if err := <-writerDone; err != nil {
		return fmt.Errorf("batch writer: %w", err)
	}

	s.logger.Info("backfill complete",
		"oracle", wu.oracle.Name,
		"blocks", stats.blocksProcessed.Load(),
		"pricesStored", stats.pricesStored.Load(),
		"errors", stats.blocksFailed.Load(),
		"duration", time.Since(stats.startTime))

	return nil
}

// computeOracleBlockRanges groups protocol-oracle bindings by protocol, then
// determines each oracle's valid block range as the union across all protocols.
//
// For each protocol, bindings are sorted by from_block. An oracle is "superseded"
// when a newer binding exists for the same protocol; its valid-to for that protocol
// is the next binding's from_block - 1. If the oracle is the latest binding for
// any protocol, it has no upper bound (validTo = 0).
//
// Across protocols, the oracle's range is the widest (union):
//   - validFrom = min(from_block) across all protocol bindings
//   - validTo = 0 if active in any protocol, else max(superseded_block)
func computeOracleBlockRanges(bindings []*entity.ProtocolOracle) map[int64]*oracleBlockRange {
	// Group bindings by protocol (already ordered by from_block from DB).
	byProtocol := make(map[int64][]*entity.ProtocolOracle)
	for _, b := range bindings {
		byProtocol[b.ProtocolID] = append(byProtocol[b.ProtocolID], b)
	}

	// Per-oracle: track min validFrom and whether it's still active somewhere.
	type rangeAccum struct {
		minFrom     int64
		maxTo       int64
		stillActive bool
	}
	accum := make(map[int64]*rangeAccum)

	for _, protocolBindings := range byProtocol {
		for i, b := range protocolBindings {
			a, ok := accum[b.OracleID]
			if !ok {
				a = &rangeAccum{minFrom: b.FromBlock}
				accum[b.OracleID] = a
			}
			if b.FromBlock < a.minFrom {
				a.minFrom = b.FromBlock
			}

			isLast := i == len(protocolBindings)-1
			if isLast {
				// Oracle is still active in this protocol.
				a.stillActive = true
			} else {
				// Superseded: next binding's from_block - 1.
				supersededAt := protocolBindings[i+1].FromBlock - 1
				if supersededAt > a.maxTo {
					a.maxTo = supersededAt
				}
			}
		}
	}

	result := make(map[int64]*oracleBlockRange, len(accum))
	for oracleID, a := range accum {
		r := &oracleBlockRange{validFrom: a.minFrom}
		if !a.stillActive {
			r.validTo = a.maxTo
		}
		result[oracleID] = r
	}
	return result
}

// clampBlockRange restricts the requested [from, to] range to the oracle's valid
// [validFrom, validTo] range. Returns the clamped from/to and whether any blocks
// remain (ok=true means from <= to after clamping).
func clampBlockRange(from, to, validFrom, validTo int64) (int64, int64, bool) {
	if validFrom > 0 && from < validFrom {
		from = validFrom
	}
	if validTo > 0 && to > validTo {
		to = validTo
	}
	return from, to, from <= to
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
	wu *oracleWorkUnit,
	priceCh chan<- []*entity.OnchainTokenPrice,
	stats *backfillStats,
) {
	mc, err := s.newMulticaller()
	if err != nil {
		s.logger.Error("failed to create multicall client", "error", err)
		return
	}

	oracleID := int16(wu.oracle.ID)
	priceDecimals := wu.oracle.PriceDecimals
	if priceDecimals == 0 {
		priceDecimals = 8
	}

	// Per-worker price cache for change detection within this contiguous range.
	prevPrices := make(map[int64]float64)

	for blockNum := fromBlock; blockNum <= toBlock; blockNum++ {
		if ctx.Err() != nil {
			return
		}

		prices, err := s.processBlock(ctx, mc, wu.oracleAddr, wu.tokenAddrs, wu.tokenIDs, oracleID, priceDecimals, blockNum)
		if err != nil {
			s.logger.Error("failed to process block",
				"block", blockNum,
				"error", err)
			stats.blocksFailed.Add(1)
			continue
		}

		// Change detection: only keep prices that differ from previous block
		// If there is a duplicate price on the worker block boundary,
		// it will be sent in both batches but that's acceptable for simplicity
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
	oracleAddr common.Address,
	tokenAddrs []common.Address,
	tokenIDs []int64,
	oracleID int16,
	priceDecimals int,
	blockNum int64,
) ([]*entity.OnchainTokenPrice, error) {
	// Fetch oracle prices via individual multicall calls (each with AllowFailure: true).
	// Tokens without price sources at historical blocks return Success: false and are skipped.
	results, err := blockchain.FetchOraclePricesIndividual(
		ctx, mc, s.oracleABI,
		oracleAddr,
		tokenAddrs, blockNum,
	)
	if err != nil {
		return nil, fmt.Errorf("fetching oracle prices: %w", err)
	}

	// Fast path: if no token succeeded, skip the header fetch entirely.
	// This avoids a wasted RPC call for blocks where the oracle is deployed but not yet configured.
	hasSuccess := false
	for _, r := range results {
		if r.Success {
			hasSuccess = true
			break
		}
	}
	if !hasSuccess {
		return nil, nil
	}

	// Get block timestamp
	header, err := s.headerFetcher.HeaderByNumber(ctx, new(big.Int).SetInt64(blockNum))
	if err != nil {
		return nil, fmt.Errorf("getting block header: %w", err)
	}
	blockTimestamp := time.Unix(int64(header.Time), 0).UTC()

	prices := make([]*entity.OnchainTokenPrice, 0, len(tokenIDs))
	for i, result := range results {
		if !result.Success {
			continue // token didn't have a price source at this block
		}
		priceUSD := blockchain.ConvertOraclePriceToUSD(result.Price, priceDecimals)

		p, err := entity.NewOnchainTokenPrice(
			tokenIDs[i],
			oracleID,
			blockNum,
			0, // block_version = 0 for historical backfill
			blockTimestamp,
			priceUSD,
		)
		if err != nil {
			s.logger.Error("invalid price entity", "tokenID", tokenIDs[i], "error", err)
			continue
		}
		prices = append(prices, p)
	}

	return prices, nil
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

func (s *Service) startProgressReporter(ctx context.Context, stats *backfillStats, totalBlocks int64, oracleName string) func() {
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
				pct := float64(processed+failed) / float64(totalBlocks) * 100

				s.logger.Info("progress",
					"oracle", oracleName,
					"pct", fmt.Sprintf("%.1f%%", pct),
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
