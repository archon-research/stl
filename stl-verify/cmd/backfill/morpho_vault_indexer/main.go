// Backfill discovers MetaMorpho vaults by scanning historical Ethereum receipt
// files stored in S3 for Morpho Blue events. Candidate addresses (caller/onBehalf)
// are collected, then probed on-chain via multicall (MORPHO() must return the
// Morpho Blue singleton). Confirmed vaults are stored in the morpho_vault table.
//
// Usage:
//
//	go run ./cmd/backfill/morpho_user_indexer \
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
	"math/big"
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

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/ethereum/go-ethereum/accounts/abi"
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
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("loading AWS config: %w", err)
	}
	s3Reader := s3adapter.NewReader(awsCfg, logger)

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
	ethClient := ethclient.NewClient(rpcClient)
	logger.Info("Ethereum RPC connected", "url", cfg.rpcURL)

	multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return fmt.Errorf("creating multicall client: %w", err)
	}

	// ABIs for vault probing
	metaMorphoABI, err := abis.GetMetaMorphoReadABI()
	if err != nil {
		return fmt.Errorf("loading MetaMorpho ABI: %w", err)
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
		multicaller:   multicaller,
		metaMorphoABI: metaMorphoABI,
		erc20ABI:      erc20ABI,
		logger:        logger,
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
	err = persistVaults(ctx, pool, logger, vaults, cfg.chainID, cfg.from)
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
	partitionsDone  atomic.Int64
	blocksProcessed atomic.Int64
	totalPartitions int64
}

// scanBlockRange fans out partition processing across goroutines and merges results.
// Returns a map of candidate address → earliest block seen.
func scanBlockRange(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	extractor *morpho_indexer.EventExtractor,
	bucket string,
	from, to int64,
	numWorkers int,
) (map[common.Address]int64, error) {
	partitions := partitionsForRange(from, to)
	prog := &progress{totalPartitions: int64(len(partitions))}
	logger.Info("partitions to process", "count", prog.totalPartitions)

	partCh := make(chan string, numWorkers)
	candidateCh := make(chan candidateEntry, 1024)
	var firstErr atomic.Value

	candidates := make(map[common.Address]int64)
	var collectorDone sync.WaitGroup
	collectorDone.Add(1)
	go collectCandidates(candidateCh, candidates, &collectorDone)

	stopProgress := make(chan struct{})
	go reportProgress(ctx, logger, prog, candidates, stopProgress)

	var wg sync.WaitGroup
	for range numWorkers {
		wg.Add(1)
		go scanPartitions(ctx, logger, s3Reader, extractor, bucket, from, to, partCh, candidateCh, prog, &firstErr, &wg)
	}

	feedPartitions(ctx, partitions, partCh, &firstErr)

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

// collectCandidates reads candidate entries from candidateCh and merges them,
// keeping the earliest first-seen block for each address.
func collectCandidates(candidateCh <-chan candidateEntry, candidates map[common.Address]int64, done *sync.WaitGroup) {
	defer done.Done()
	for entry := range candidateCh {
		if existing, ok := candidates[entry.address]; !ok || entry.firstBlock < existing {
			candidates[entry.address] = entry.firstBlock
		}
	}
}

// reportProgress logs scan stats every 10 seconds until stopCh is closed.
func reportProgress(ctx context.Context, logger *slog.Logger, prog *progress, candidates map[common.Address]int64, stopCh <-chan struct{}) {
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
			done := prog.partitionsDone.Load()
			blocks := prog.blocksProcessed.Load()
			elapsed := time.Since(startTime)
			blocksPerSec := float64(blocks) / elapsed.Seconds()
			remaining := prog.totalPartitions - done
			var eta time.Duration
			if done > 0 {
				eta = time.Duration(float64(elapsed) / float64(done) * float64(remaining))
			}
			logger.Info("progress",
				"partitions", fmt.Sprintf("%d/%d", done, prog.totalPartitions),
				"blocksProcessed", blocks,
				"uniqueCandidates", len(candidates),
				"blocksPerSec", fmt.Sprintf("%.0f", blocksPerSec),
				"elapsed", elapsed.Round(time.Second),
				"eta", eta.Round(time.Second))
		}
	}
}

// scanPartitions reads partition prefixes from partCh, processes each one,
// and sends discovered candidate addresses to candidateCh.
func scanPartitions(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	extractor *morpho_indexer.EventExtractor,
	bucket string,
	from, to int64,
	partCh <-chan string,
	candidateCh chan<- candidateEntry,
	prog *progress,
	firstErr *atomic.Value,
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for part := range partCh {
		if ctx.Err() != nil {
			return
		}
		blocks, err := processPartition(ctx, logger, s3Reader, extractor, bucket, part, from, to, candidateCh)
		if err != nil {
			firstErr.CompareAndSwap(nil, err)
			logger.Error("partition failed", "partition", part, "error", err)
			return
		}
		prog.blocksProcessed.Add(int64(blocks))
		prog.partitionsDone.Add(1)
	}
}

// feedPartitions sends partition prefixes into partCh, stopping early on
// context cancellation or if a worker has recorded an error.
func feedPartitions(ctx context.Context, partitions []string, partCh chan<- string, firstErr *atomic.Value) {
	for _, part := range partitions {
		if ctx.Err() != nil {
			break
		}
		if v := firstErr.Load(); v != nil {
			break
		}
		partCh <- part
	}
	close(partCh)
}

// processPartition lists receipts for a single partition, downloads them,
// and sends candidate addresses to candidateCh.
// Returns the number of blocks processed.
func processPartition(
	ctx context.Context,
	logger *slog.Logger,
	s3Reader outbound.S3Reader,
	extractor *morpho_indexer.EventExtractor,
	bucket, partitionPrefix string,
	from, to int64,
	candidateCh chan<- candidateEntry,
) (int, error) {
	morphoBlueAddr := morpho_indexer.MorphoBlueAddress

	receiptKeys, err := listHighestVersionReceipts(ctx, s3Reader, bucket, partitionPrefix)
	if err != nil {
		return 0, fmt.Errorf("listing receipts for partition %s: %w", partitionPrefix, err)
	}

	// Collect in-range block numbers and check for gaps.
	var inRangeBlocks []int64
	for _, key := range receiptKeys {
		parsed, ok := s3key.Parse(key)
		if !ok {
			continue
		}
		if parsed.BlockNumber >= from && parsed.BlockNumber <= to {
			inRangeBlocks = append(inRangeBlocks, parsed.BlockNumber)
		}
	}
	logBlockGaps(logger, partitionPrefix, inRangeBlocks, from, to)

	blocks := 0
	for _, key := range receiptKeys {
		if ctx.Err() != nil {
			return blocks, ctx.Err()
		}

		parsed, ok := s3key.Parse(key)
		if !ok {
			continue
		}
		if parsed.BlockNumber < from || parsed.BlockNumber > to {
			continue
		}

		receipts, err := downloadReceipts(ctx, s3Reader, bucket, key)
		if err != nil {
			return blocks, fmt.Errorf("downloading %s: %w", key, err)
		}

		extractCandidatesFromReceipts(receipts, extractor, morphoBlueAddr, parsed.BlockNumber, candidateCh)
		blocks++
	}

	return blocks, nil
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

// ---------------------------------------------------------------------------
// Phase 2: On-chain vault probing
// ---------------------------------------------------------------------------

// confirmedVault holds metadata for a confirmed MetaMorpho vault.
type confirmedVault struct {
	Address    common.Address
	Name       string
	Symbol     string
	Asset      common.Address
	Decimals   uint8
	Version    entity.MorphoVaultVersion
	FirstBlock int64
}

// vaultProber probes candidate addresses on-chain to determine if they are MetaMorpho vaults.
type vaultProber struct {
	multicaller   outbound.Multicaller
	metaMorphoABI *abi.ABI
	erc20ABI      *abi.ABI
	logger        *slog.Logger
}

// probeAllCandidates checks each candidate address by calling MORPHO() and asset()
// via multicall. Returns confirmed vaults with their metadata.
func (p *vaultProber) probeAllCandidates(
	ctx context.Context,
	candidates map[common.Address]int64,
	probeBlock int64,
	batchSize int,
) ([]confirmedVault, error) {
	addrs := make([]common.Address, 0, len(candidates))
	for addr := range candidates {
		addrs = append(addrs, addr)
	}

	blockNum := new(big.Int).SetInt64(probeBlock)
	var confirmed []confirmedVault

	for i := 0; i < len(addrs); i += batchSize {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		end := min(i+batchSize, len(addrs))
		batch := addrs[i:end]

		vaults, err := p.probeBatch(ctx, batch, candidates, blockNum)
		if err != nil {
			return nil, fmt.Errorf("probing batch %d-%d: %w", i, end, err)
		}
		confirmed = append(confirmed, vaults...)

		p.logger.Info("probe progress",
			"probed", end,
			"total", len(addrs),
			"confirmedSoFar", len(confirmed))
	}

	return confirmed, nil
}

// probeBatch probes a batch of candidate addresses. For each address, it calls
// MORPHO() and asset() in a single multicall. Confirmed vaults get their metadata
// fetched in a follow-up multicall.
func (p *vaultProber) probeBatch(
	ctx context.Context,
	batch []common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	morphoData, err := p.metaMorphoABI.Pack("MORPHO")
	if err != nil {
		return nil, fmt.Errorf("packing MORPHO call: %w", err)
	}
	assetData, err := p.metaMorphoABI.Pack("asset")
	if err != nil {
		return nil, fmt.Errorf("packing asset call: %w", err)
	}

	// Build multicall: 2 calls per candidate (MORPHO, asset)
	calls := make([]outbound.Call, 0, len(batch)*2)
	for _, addr := range batch {
		calls = append(calls,
			outbound.Call{Target: addr, AllowFailure: true, CallData: morphoData},
			outbound.Call{Target: addr, AllowFailure: true, CallData: assetData},
		)
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall probe: %w", err)
	}

	// Identify confirmed vaults from results
	var vaultAddrs []common.Address
	vaultAssets := make(map[common.Address]common.Address)

	for i, addr := range batch {
		morphoResult := results[i*2]
		assetResult := results[i*2+1]

		if !morphoResult.Success || len(morphoResult.ReturnData) == 0 {
			continue
		}
		morphoUnpacked, err := p.metaMorphoABI.Unpack("MORPHO", morphoResult.ReturnData)
		if err != nil || len(morphoUnpacked) == 0 {
			continue
		}
		morphoAddr, ok := morphoUnpacked[0].(common.Address)
		if !ok || morphoAddr != morpho_indexer.MorphoBlueAddress {
			continue
		}

		if !assetResult.Success || len(assetResult.ReturnData) == 0 {
			continue
		}
		assetUnpacked, err := p.metaMorphoABI.Unpack("asset", assetResult.ReturnData)
		if err != nil || len(assetUnpacked) == 0 {
			continue
		}
		asset, ok := assetUnpacked[0].(common.Address)
		if !ok || asset == (common.Address{}) {
			continue
		}

		vaultAddrs = append(vaultAddrs, addr)
		vaultAssets[addr] = asset
	}

	if len(vaultAddrs) == 0 {
		return nil, nil
	}

	// Fetch metadata for confirmed vaults
	return p.fetchVaultMetadata(ctx, vaultAddrs, vaultAssets, firstBlocks, blockNum)
}

// fetchVaultMetadata fetches name, symbol, decimals, and version for confirmed vaults.
func (p *vaultProber) fetchVaultMetadata(
	ctx context.Context,
	vaultAddrs []common.Address,
	vaultAssets map[common.Address]common.Address,
	firstBlocks map[common.Address]int64,
	blockNum *big.Int,
) ([]confirmedVault, error) {
	nameData, err := p.metaMorphoABI.Pack("name")
	if err != nil {
		return nil, fmt.Errorf("packing name: %w", err)
	}
	symbolData, err := p.metaMorphoABI.Pack("symbol")
	if err != nil {
		return nil, fmt.Errorf("packing symbol: %w", err)
	}
	skimData, err := p.metaMorphoABI.Pack("skimRecipient")
	if err != nil {
		return nil, fmt.Errorf("packing skimRecipient: %w", err)
	}
	decimalsData, err := p.erc20ABI.Pack("decimals")
	if err != nil {
		return nil, fmt.Errorf("packing decimals: %w", err)
	}

	// 4 calls per vault: name, symbol, skimRecipient (version detect), asset decimals
	calls := make([]outbound.Call, 0, len(vaultAddrs)*4)
	for _, addr := range vaultAddrs {
		calls = append(calls,
			outbound.Call{Target: addr, AllowFailure: true, CallData: nameData},
			outbound.Call{Target: addr, AllowFailure: true, CallData: symbolData},
			outbound.Call{Target: addr, AllowFailure: true, CallData: skimData},
			outbound.Call{Target: vaultAssets[addr], AllowFailure: true, CallData: decimalsData},
		)
	}

	results, err := p.multicaller.Execute(ctx, calls, blockNum)
	if err != nil {
		return nil, fmt.Errorf("multicall metadata: %w", err)
	}

	var vaults []confirmedVault
	for i, addr := range vaultAddrs {
		nameResult := results[i*4]
		symbolResult := results[i*4+1]
		skimResult := results[i*4+2]
		decimalsResult := results[i*4+3]

		v := confirmedVault{
			Address:    addr,
			Asset:      vaultAssets[addr],
			FirstBlock: firstBlocks[addr],
			Version:    entity.MorphoVaultV1,
		}

		// Version detection: skimRecipient() only exists on V2
		if skimResult.Success && len(skimResult.ReturnData) > 0 {
			if _, err := p.metaMorphoABI.Unpack("skimRecipient", skimResult.ReturnData); err == nil {
				v.Version = entity.MorphoVaultV2
			}
		}

		if nameResult.Success && len(nameResult.ReturnData) > 0 {
			if unpacked, err := p.metaMorphoABI.Unpack("name", nameResult.ReturnData); err == nil && len(unpacked) > 0 {
				if name, ok := unpacked[0].(string); ok {
					v.Name = name
				}
			}
		}

		if symbolResult.Success && len(symbolResult.ReturnData) > 0 {
			if unpacked, err := p.metaMorphoABI.Unpack("symbol", symbolResult.ReturnData); err == nil && len(unpacked) > 0 {
				if sym, ok := unpacked[0].(string); ok {
					v.Symbol = sym
				}
			}
		}

		if decimalsResult.Success && len(decimalsResult.ReturnData) > 0 {
			if unpacked, err := p.erc20ABI.Unpack("decimals", decimalsResult.ReturnData); err == nil && len(unpacked) > 0 {
				if dec, ok := unpacked[0].(uint8); ok {
					v.Decimals = dec
				}
			}
		}

		p.logger.Info("confirmed vault",
			"address", addr.Hex(),
			"name", v.Name,
			"symbol", v.Symbol,
			"asset", v.Asset.Hex(),
			"version", v.Version,
			"firstBlock", v.FirstBlock)

		vaults = append(vaults, v)
	}

	return vaults, nil
}

// ---------------------------------------------------------------------------
// Phase 3: Persist confirmed vaults
// ---------------------------------------------------------------------------

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

			tokenID, err := tokenRepo.GetOrCreateToken(ctx, tx, chainID, v.Asset, "", int(v.Decimals), v.FirstBlock)
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
