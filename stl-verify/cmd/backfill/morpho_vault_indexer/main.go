// Backfill discovers MetaMorpho vaults by scanning historical Ethereum receipt
// files stored in S3 for Morpho Blue events. Candidate addresses (caller/onBehalf)
// are collected, then probed on-chain via multicall (MORPHO() must return the
// Morpho Blue singleton). Confirmed vaults are stored in the morpho_vault table.
//
// Usage:
//
//	go run ./cmd/backfill/morpho_vault_indexer \
//	  -from 18883124 -to 24600000 \
//	  -bucket stl-sentinelstaging-ethereum-raw-89d540d0 \
//	  -db "$DATABASE_URL" \
//	  -rpc-url "$RPC_URL" \
//	  -goroutines 64
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	s3adapter "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx, os.Args[1:]); err != nil {
		slog.Error("fatal error", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg, err := parseConfig(args)
	if err != nil {
		return err
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: env.ParseLogLevel(slog.LevelInfo),
	}))
	slog.SetDefault(logger)

	logger.Info("starting morpho vault backfill",
		"from", cfg.from,
		"to", cfg.to,
		"bucket", cfg.bucket,
		"chainID", cfg.chainID,
		"goroutines", cfg.goroutines)

	// AWS + S3
	awsRegion := env.Get("AWS_REGION", "eu-west-1")
	awsOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(awsRegion),
	}
	if accessKeyID := os.Getenv("AWS_ACCESS_KEY_ID"); accessKeyID != "" {
		secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
		awsOpts = append(awsOpts, awsconfig.WithCredentialsProvider(aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretKey,
				Source:          "StaticCredentials",
			}, nil
		})))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsOpts...)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}
	s3HTTPClient := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          cfg.goroutines + 64,
			MaxIdleConnsPerHost:   cfg.goroutines + 64,
			MaxConnsPerHost:       cfg.goroutines + 64,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	s3Reader := s3adapter.NewReaderWithHTTPClient(awsCfg, s3HTTPClient, logger)

	// PostgreSQL
	pool, err := postgres.OpenPool(ctx, postgres.DefaultDBConfig(cfg.dbURL))
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer pool.Close()
	logger.Info("PostgreSQL connected")

	// Ethereum RPC
	httpClient := &http.Client{
		Timeout: 120 * time.Second,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          20,
			MaxIdleConnsPerHost:   10,
			MaxConnsPerHost:       10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
	rpcClient, err := rpc.DialOptions(ctx, cfg.rpcURL, rpc.WithHTTPClient(httpClient))
	if err != nil {
		return fmt.Errorf("connecting to RPC: %w", err)
	}
	defer rpcClient.Close()
	ethClient := ethclient.NewClient(rpcClient)
	logger.Info("Ethereum RPC connected")

	rpcChainID, err := ethClient.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("fetching RPC chain ID: %w", err)
	}
	if rpcChainID.Int64() != cfg.chainID {
		return fmt.Errorf("RPC chain ID mismatch: RPC reports %d, config says %d", rpcChainID.Int64(), cfg.chainID)
	}

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("creating multicall client: %w", err)
	}

	// Shared vault prober (handles MetaMorpho ABI internally)
	sharedProber, err := morpho_indexer.NewVaultProber()
	if err != nil {
		return fmt.Errorf("creating vault prober: %w", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return fmt.Errorf("loading ERC20 ABI: %w", err)
	}

	// Event extractor (thread-safe, read-only after init)
	extractor, err := morpho_indexer.NewEventExtractor()
	if err != nil {
		return fmt.Errorf("creating event extractor: %w", err)
	}

	// Phase 1: Scan receipts for candidate addresses
	candidates, err := scanBlockRange(ctx, logger, s3Reader, extractor, cfg.bucket, cfg.from, cfg.to, cfg.goroutines)
	if err != nil {
		return fmt.Errorf("scanning block range: %w", err)
	}
	logger.Info("scan complete", "uniqueCandidates", len(candidates))

	if len(candidates) == 0 {
		logger.Info("no candidates found, nothing to probe")
		return nil
	}

	// Phase 2: Probe candidates on-chain to confirm vaults
	prober := &vaultProber{
		multicaller:  multicaller,
		sharedProber: sharedProber,
		erc20ABI:     erc20ABI,
		logger:       logger,
	}

	vaults, err := prober.probeAllCandidates(ctx, candidates, cfg.to, cfg.probeBatch)
	if err != nil {
		return fmt.Errorf("probing candidates: %w", err)
	}
	logger.Info("probing complete", "confirmedVaults", len(vaults))

	if len(vaults) == 0 {
		logger.Info("no vaults confirmed")
		return nil
	}

	// Phase 3: Persist confirmed vaults
	deployBlock, err := morpho_indexer.MorphoBlueDeployBlock(cfg.chainID)
	if err != nil {
		return fmt.Errorf("getting deploy block: %w", err)
	}
	err = persistVaults(ctx, pool, logger, vaults, cfg.chainID, deployBlock)
	if err != nil {
		return fmt.Errorf("persisting vaults: %w", err)
	}

	logger.Info("backfill complete", "vaultsPersisted", len(vaults))
	return nil
}

// candidateEntry represents a candidate address and the earliest block it was seen.
type candidateEntry struct {
	address    common.Address
	firstBlock int64
}

// progress holds atomic counters shared between workers and the progress reporter.
type progress struct {
	partitionsDone     atomic.Int64
	blocksProcessed    atomic.Int64
	uniqueCandidates   atomic.Int64
	totalPartitions    int64
	totalBlocks        atomic.Int64
	listDurationMs     atomic.Int64
	listCount          atomic.Int64
	downloadDurationMs atomic.Int64
	downloadCount      atomic.Int64
	extractDurationMs  atomic.Int64
}

// blockWork represents a single block receipt file to download and process.
type blockWork struct {
	key         string
	blockNumber int64
}

// scanBlockRange discovers receipt keys across partitions, then fans out
// individual block downloads across a shared worker pool.
func scanBlockRange(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	extractor *morpho_indexer.EventExtractor,
	bucket string,
	from, to int64,
	numWorkers int,
) (map[common.Address]int64, error) {
	ctx, cancelScan := context.WithCancel(ctx)
	defer cancelScan()

	partitions := partitionsForRange(from, to)
	prog := &progress{totalPartitions: int64(len(partitions))}

	// Start progress reporter early so listing phase is visible.
	candidateCh := make(chan candidateEntry, 1024)
	candidates := make(map[common.Address]int64)
	var collectorDone sync.WaitGroup
	collectorDone.Add(1)
	go collectCandidates(candidateCh, candidates, prog, &collectorDone)

	stopProgress := make(chan struct{})
	go reportProgress(ctx, logger, prog, stopProgress)

	// Phase 1: List all partitions concurrently to collect block keys.
	logger.Info("listing partitions", "count", len(partitions))
	blockKeys, err := listAllBlockKeys(ctx, logger, s3Reader, bucket, partitions, from, to, numWorkers, prog)
	if err != nil {
		close(candidateCh)
		close(stopProgress)
		collectorDone.Wait()
		return nil, fmt.Errorf("listing block keys: %w", err)
	}
	prog.totalBlocks.Store(int64(len(blockKeys)))
	logger.Info("listing complete", "totalBlocks", len(blockKeys))

	if len(blockKeys) == 0 {
		close(candidateCh)
		close(stopProgress)
		collectorDone.Wait()
		return nil, nil
	}

	// Phase 2: Download and process blocks via shared worker pool.

	workCh := make(chan blockWork, numWorkers*2)
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go downloadWorker(ctx, logger, s3Reader, extractor, bucket, workCh, candidateCh, prog, &firstErr, cancelScan, &wg)
	}

	// Feed block keys into the work channel.
	feedBlocks(ctx, blockKeys, workCh, &firstErr)
	close(workCh)

	wg.Wait()
	close(candidateCh)
	close(stopProgress)
	collectorDone.Wait()

	if v := firstErr.Load(); v != nil {
		return nil, v.(error)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return candidates, nil
}

// listAllBlockKeys lists receipt keys from all partitions concurrently and
// returns a flat list of block work items sorted by block number.
func listAllBlockKeys(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	bucket string,
	partitions []string,
	from, to int64,
	numWorkers int,
	prog *progress,
) ([]blockWork, error) {
	// Use fewer workers for listing since each call is heavier.
	listWorkers := min(numWorkers, 64)

	partCh := make(chan string, listWorkers)
	type listResult struct {
		keys []blockWork
		err  error
	}
	resultCh := make(chan listResult, listWorkers)

	var wg sync.WaitGroup
	for range listWorkers {
		wg.Go(func() {
			for part := range partCh {
				if ctx.Err() != nil {
					return
				}
				listStart := time.Now()
				receiptKeys, err := listHighestVersionReceipts(ctx, s3Reader, bucket, part)
				prog.listDurationMs.Add(time.Since(listStart).Milliseconds())
				prog.listCount.Add(1)
				if err != nil {
					resultCh <- listResult{err: fmt.Errorf("listing partition %s: %w", part, err)}
					return
				}

				var keys []blockWork
				for _, key := range receiptKeys {
					parsed, ok := s3key.Parse(key)
					if !ok {
						continue
					}
					if parsed.BlockNumber >= from && parsed.BlockNumber <= to {
						keys = append(keys, blockWork{key: key, blockNumber: parsed.BlockNumber})
					}
				}

				logBlockGapsFromKeys(logger, part, keys, from, to)
				prog.partitionsDone.Add(1)
				resultCh <- listResult{keys: keys}
			}
		})
	}

	// Feed partitions.
	go func() {
		defer close(partCh)
		for _, part := range partitions {
			select {
			case partCh <- part:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Collect results.
	go func() {
		wg.Wait()
		close(resultCh)
	}()

	var allKeys []blockWork
	for res := range resultCh {
		if res.err != nil {
			return nil, res.err
		}
		allKeys = append(allKeys, res.keys...)
	}

	sort.Slice(allKeys, func(i, j int) bool {
		return allKeys[i].blockNumber < allKeys[j].blockNumber
	})

	return allKeys, nil
}

// logBlockGapsFromKeys is like logBlockGaps but works with blockWork slices.
func logBlockGapsFromKeys(logger *slog.Logger, partitionPrefix string, keys []blockWork, from, to int64) {
	if len(keys) == 0 {
		return
	}
	blockNums := make([]int64, len(keys))
	for i, k := range keys {
		blockNums[i] = k.blockNumber
	}
	logBlockGaps(logger, partitionPrefix, blockNums, from, to)
}

// downloadWorker pulls block work items from workCh, downloads and processes each one.
func downloadWorker(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	extractor *morpho_indexer.EventExtractor,
	bucket string,
	workCh <-chan blockWork,
	candidateCh chan<- candidateEntry,
	prog *progress,
	firstErr *atomic.Value,
	cancelScan context.CancelFunc,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	morphoBlueAddr := morpho_indexer.MorphoBlueAddress
	for work := range workCh {
		if ctx.Err() != nil {
			return
		}

		dlStart := time.Now()
		receipts, err := downloadReceipts(ctx, s3Reader, bucket, work.key)
		if err != nil {
			firstErr.CompareAndSwap(nil, fmt.Errorf("downloading %s: %w", work.key, err))
			cancelScan()
			return
		}
		prog.downloadDurationMs.Add(time.Since(dlStart).Milliseconds())
		prog.downloadCount.Add(1)

		extractStart := time.Now()
		extractCandidatesFromReceipts(logger, receipts, extractor, morphoBlueAddr, work.blockNumber, candidateCh)
		prog.extractDurationMs.Add(time.Since(extractStart).Milliseconds())
		prog.blocksProcessed.Add(1)
	}
}

// feedBlocks sends block work items into workCh, stopping early on
// context cancellation or if a worker has recorded an error.
func feedBlocks(ctx context.Context, blockKeys []blockWork, workCh chan<- blockWork, firstErr *atomic.Value) {
	for _, bk := range blockKeys {
		if v := firstErr.Load(); v != nil {
			return
		}
		select {
		case workCh <- bk:
		case <-ctx.Done():
			return
		}
	}
}

// collectCandidates reads candidate entries from candidateCh and merges them,
// keeping the earliest first-seen block for each address.
func collectCandidates(candidateCh <-chan candidateEntry, candidates map[common.Address]int64, prog *progress, done *sync.WaitGroup) {
	defer done.Done()
	for entry := range candidateCh {
		if existing, ok := candidates[entry.address]; !ok || entry.firstBlock < existing {
			if !ok {
				prog.uniqueCandidates.Add(1)
			}
			candidates[entry.address] = entry.firstBlock
		}
	}
}

// reportProgress logs scan stats every 10 seconds until stopCh is closed.
func reportProgress(ctx context.Context, logger *slog.Logger, prog *progress, stopCh <-chan struct{}) {
	startTime := time.Now()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopCh:
			return
		case <-ticker.C:
			blocks := prog.blocksProcessed.Load()
			totalBlocks := prog.totalBlocks.Load()
			elapsed := time.Since(startTime)
			blocksPerSec := float64(blocks) / elapsed.Seconds()
			var eta time.Duration
			if blocks > 0 && totalBlocks > 0 {
				remaining := totalBlocks - blocks
				eta = time.Duration(float64(elapsed) / float64(blocks) * float64(remaining))
			}
			listCount := prog.listCount.Load()
			dlCount := prog.downloadCount.Load()
			var avgListMs, avgDlMs, avgExtractMs float64
			if listCount > 0 {
				avgListMs = float64(prog.listDurationMs.Load()) / float64(listCount)
			}
			if dlCount > 0 {
				avgDlMs = float64(prog.downloadDurationMs.Load()) / float64(dlCount)
				avgExtractMs = float64(prog.extractDurationMs.Load()) / float64(dlCount)
			}
			logger.Info("progress",
				"partitionsListed", fmt.Sprintf("%d/%d", prog.partitionsDone.Load(), prog.totalPartitions),
				"blocksProcessed", fmt.Sprintf("%d/%d", blocks, totalBlocks),
				"uniqueCandidates", prog.uniqueCandidates.Load(),
				"blocksPerSec", fmt.Sprintf("%.0f", blocksPerSec),
				"elapsed", elapsed.Round(time.Second),
				"eta", eta.Round(time.Second),
				"avgListMs", fmt.Sprintf("%.0f", avgListMs),
				"avgDownloadMs", fmt.Sprintf("%.0f", avgDlMs),
				"avgExtractMs", fmt.Sprintf("%.0f", avgExtractMs))
		}
	}
}

// logBlockGaps warns about missing blocks within a partition's expected range.
func logBlockGaps(logger *slog.Logger, partitionPrefix string, blockNums []int64, from, to int64) {
	if len(blockNums) == 0 {
		return
	}

	// Determine the expected contiguous range within this partition.
	// Partition covers [partStart, partStart+999]. Clamp to [from, to].
	partStart := blockNums[0] - (blockNums[0] % partition.BlockRangeSize)
	partEnd := partStart + partition.BlockRangeSize - 1
	if partStart < from {
		partStart = from
	}
	if partEnd > to {
		partEnd = to
	}

	present := make(map[int64]bool, len(blockNums))
	for _, bn := range blockNums {
		present[bn] = true
	}

	var gapStart int64
	inGap := false
	for bn := partStart; bn <= partEnd; bn++ {
		if !present[bn] {
			if !inGap {
				gapStart = bn
				inGap = true
			}
		} else if inGap {
			logger.Warn("missing blocks in S3",
				"partition", partitionPrefix,
				"gapStart", gapStart,
				"gapEnd", bn-1,
				"missingCount", bn-gapStart)
			inGap = false
		}
	}
	if inGap {
		logger.Warn("missing blocks in S3",
			"partition", partitionPrefix,
			"gapStart", gapStart,
			"gapEnd", partEnd,
			"missingCount", partEnd-gapStart+1)
	}
}

// partitionsForRange returns the ordered list of distinct partition prefixes
// that cover the given block range.
func partitionsForRange(from, to int64) []string {
	seen := make(map[string]bool)
	var parts []string
	for block := from; block <= to; block += partition.BlockRangeSize {
		p := partition.GetPartition(block)
		if !seen[p] {
			seen[p] = true
			parts = append(parts, p)
		}
	}
	lastPart := partition.GetPartition(to)
	if !seen[lastPart] {
		parts = append(parts, lastPart)
	}
	sort.Strings(parts)
	return parts
}

// listHighestVersionReceipts lists all receipt files in a partition and
// returns the S3 key with the highest version for each block number.
func listHighestVersionReceipts(
	ctx context.Context,
	s3Reader outbound.S3Reader,
	bucket, partitionPrefix string,
) ([]string, error) {
	keys, err := s3Reader.ListPrefix(ctx, bucket, partitionPrefix+"/")
	if err != nil {
		return nil, err
	}

	type entry struct {
		version int
		key     string
	}
	best := make(map[int64]entry)

	for _, key := range keys {
		parsed, ok := s3key.Parse(key)
		if !ok || parsed.DataType != s3key.Receipts {
			continue
		}
		if cur, exists := best[parsed.BlockNumber]; !exists || parsed.Version > cur.version {
			best[parsed.BlockNumber] = entry{version: parsed.Version, key: key}
		}
	}

	blockNums := make([]int64, 0, len(best))
	for bn := range best {
		blockNums = append(blockNums, bn)
	}
	slices.Sort(blockNums)

	result := make([]string, 0, len(blockNums))
	for _, bn := range blockNums {
		result = append(result, best[bn].key)
	}
	return result, nil
}

// downloadReceipts streams and parses a receipt file from S3.
func downloadReceipts(
	ctx context.Context,
	s3Reader outbound.S3Reader,
	bucket, key string,
) ([]shared.TransactionReceipt, error) {
	reader, err := s3Reader.StreamFile(ctx, bucket, key)
	if err != nil {
		return nil, fmt.Errorf("streaming %s: %w", key, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", key, err)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(data, &receipts); err != nil {
		return nil, fmt.Errorf("unmarshalling %s: %w", key, err)
	}
	return receipts, nil
}

// extractCandidatesFromReceipts scans receipts for Morpho Blue events and sends
// candidate vault addresses (caller/onBehalf) to candidateCh.
func extractCandidatesFromReceipts(
	logger *slog.Logger,
	receipts []shared.TransactionReceipt,
	extractor *morpho_indexer.EventExtractor,
	morphoBlueAddr common.Address,
	blockNumber int64,
	candidateCh chan<- candidateEntry,
) {
	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			if !strings.EqualFold(log.Address, morphoBlueAddr.Hex()) {
				continue
			}
			if !extractor.IsMorphoBlueEvent(log) {
				continue
			}

			event, err := extractor.ExtractMorphoBlueEvent(log)
			if err != nil {
				logger.Warn("failed to extract Morpho Blue event",
					"block", blockNumber,
					"txHash", log.TransactionHash,
					"error", err)
				continue
			}

			for _, addr := range candidateAddresses(event) {
				if addr == (common.Address{}) {
					continue
				}
				candidateCh <- candidateEntry{address: addr, firstBlock: blockNumber}
			}
		}
	}
}

// candidateAddresses returns addresses from a Morpho Blue event that could
// be MetaMorpho vaults — the caller and onBehalf fields.
func candidateAddresses(event morpho_indexer.MorphoBlueEvent) []common.Address {
	switch e := event.(type) {
	case *morpho_indexer.SupplyEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *morpho_indexer.WithdrawEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *morpho_indexer.BorrowEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *morpho_indexer.RepayEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *morpho_indexer.SupplyCollateralEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *morpho_indexer.WithdrawCollateralEvent:
		return []common.Address{e.Caller, e.OnBehalf}
	case *morpho_indexer.LiquidateEvent:
		return []common.Address{e.Caller, e.Borrower}
	default:
		return nil
	}
}

// persistVaults stores confirmed vaults in the database, creating the protocol
// and asset token entries as needed.
func persistVaults(
	ctx context.Context,
	pool *pgxpool.Pool,
	logger *slog.Logger,
	vaults []confirmedVault,
	chainID int64,
	deployBlock int64,
) error {
	txManager, err := postgres.NewTxManager(pool, logger)
	if err != nil {
		return fmt.Errorf("creating tx manager: %w", err)
	}

	morphoRepo, err := postgres.NewMorphoRepository(pool, logger)
	if err != nil {
		return fmt.Errorf("creating morpho repository: %w", err)
	}

	protocolRepo, err := postgres.NewProtocolRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating protocol repository: %w", err)
	}

	tokenRepo, err := postgres.NewTokenRepository(pool, logger, 0)
	if err != nil {
		return fmt.Errorf("creating token repository: %w", err)
	}

	for _, v := range vaults {
		if err := txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
			protocolID, err := protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, morpho_indexer.MorphoBlueAddress, "Morpho Blue", "lending", deployBlock)
			if err != nil {
				return fmt.Errorf("getting protocol: %w", err)
			}

			tokenID, err := tokenRepo.GetOrCreateToken(ctx, tx, chainID, v.Asset, v.AssetSymbol, int(v.Decimals), v.FirstBlock)
			if err != nil {
				return fmt.Errorf("getting asset token: %w", err)
			}

			vault, err := entity.NewMorphoVault(chainID, protocolID, v.Address.Bytes(), v.Name, v.Symbol, tokenID, v.Version, v.FirstBlock)
			if err != nil {
				return fmt.Errorf("creating vault entity: %w", err)
			}

			_, err = morphoRepo.GetOrCreateVault(ctx, tx, vault)
			if err != nil {
				return fmt.Errorf("persisting vault: %w", err)
			}

			return nil
		}); err != nil {
			return fmt.Errorf("persisting vault %s: %w", v.Address.Hex(), err)
		}

		logger.Info("persisted vault",
			"address", v.Address.Hex(),
			"name", v.Name)
	}

	return nil
}
