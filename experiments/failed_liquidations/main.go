package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/common"
	erigonlog "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	execState "github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

const (
	// SparkLend Pool contract address on Ethereum mainnet
	DefaultSparkLendPool = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"

	// Default Erigon data directory
	DefaultDataDir = "/mnt/data"

	// SparkLend deployment block on Ethereum mainnet
	DefaultStartBlock = 17203646

	// LiquidationCall function selector: liquidationCall(address,address,address,uint256,bool)
	LiquidationCallSelector = "00a718a9"
)

// LiquidationCall represents a liquidation call transaction
type LiquidationCall struct {
	BlockNumber       uint64 `json:"block_number"`
	BlockTimestamp    uint64 `json:"block_timestamp"`
	TransactionHash   string `json:"transaction_hash"`
	TxIndex           int    `json:"tx_index"`
	FromAddress       string `json:"from_address"`
	ToAddress         string `json:"to_address"`
	CollateralAsset   string `json:"collateral_asset"`
	DebtAsset         string `json:"debt_asset"`
	User              string `json:"user"`
	DebtToCover       string `json:"debt_to_cover"`
	ReceiveAToken     bool   `json:"receive_atoken"`
	GasLimit          uint64 `json:"gas_limit"`
	Value             string `json:"value"`
	CallDepth         int    `json:"call_depth"`
	Failed            bool   `json:"failed"`
	RevertReason      string `json:"revert_reason,omitempty"`
	InternalCallIndex int    `json:"internal_call_index"`
}

// LiquidationTracer captures liquidation calls during EVM execution
type LiquidationTracer struct {
	sparkLendContract common.Address
	liquidations      []*LiquidationCall
	currentTx         types.Transaction
	currentBlock      *types.Block
	txIndex           int
	callDepth         int
	internalCallIdx   int
	currentFrom       accounts.Address
}

func NewLiquidationTracer(sparkLendContract common.Address) *LiquidationTracer {
	return &LiquidationTracer{
		sparkLendContract: sparkLendContract,
		liquidations:      make([]*LiquidationCall, 0),
	}
}

func (t *LiquidationTracer) SetTransaction(tx types.Transaction, block *types.Block, txIndex int) {
	t.currentTx = tx
	t.currentBlock = block
	t.txIndex = txIndex
	t.callDepth = 0
	t.internalCallIdx = 0
}

func (t *LiquidationTracer) Tracer() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter: t.OnEnter,
		OnExit:  t.OnExit,
	}
}

func (t *LiquidationTracer) OnTxStart(env *tracing.VMContext, tx types.Transaction, from accounts.Address) {
	t.currentFrom = from
	t.callDepth = 0
	t.internalCallIdx = 0
}

func (t *LiquidationTracer) OnEnter(depth int, typ byte, from accounts.Address, to accounts.Address, precompile bool, input []byte, gas uint64, value uint256.Int, code []byte) {
	t.callDepth = depth

	// Check if this is a call to SparkLend
	toAddr := to.Value()
	if toAddr != t.sparkLendContract {
		return
	}

	// Check if it's a liquidationCall (selector = 00a718a9)
	if len(input) < 4 {
		return
	}

	selector := hex.EncodeToString(input[:4])
	if selector != LiquidationCallSelector {
		return
	}

	// This is a liquidationCall to SparkLend!
	args := decodeLiquidationCall(input)

	var txHash string
	if t.currentTx != nil {
		txHash = t.currentTx.Hash().Hex()
	}

	var blockTimestamp uint64
	var gasLimit uint64
	var txValue string
	var txToAddr string

	if t.currentBlock != nil {
		blockTimestamp = t.currentBlock.Header().Time
	}
	if t.currentTx != nil {
		gasLimit = t.currentTx.GetGasLimit()
		txValue = t.currentTx.GetValue().String()
		if t.currentTx.GetTo() != nil {
			txToAddr = t.currentTx.GetTo().Hex()
		}
	}

	liqCall := &LiquidationCall{
		BlockNumber:       t.currentBlock.NumberU64(),
		BlockTimestamp:    blockTimestamp,
		TransactionHash:   txHash,
		TxIndex:           t.txIndex,
		FromAddress:       from.Value().Hex(),
		ToAddress:         txToAddr,
		CollateralAsset:   args.CollateralAsset,
		DebtAsset:         args.DebtAsset,
		User:              args.User,
		DebtToCover:       args.DebtToCover,
		ReceiveAToken:     args.ReceiveAToken,
		GasLimit:          gasLimit,
		Value:             txValue,
		CallDepth:         depth,
		Failed:            false, // Will be set in OnExit if reverted
		InternalCallIndex: t.internalCallIdx,
	}

	t.liquidations = append(t.liquidations, liqCall)
	t.internalCallIdx++
}

func (t *LiquidationTracer) OnExit(depth int, output []byte, gasUsed uint64, err error, reverted bool) {
	// If a liquidation call reverted, mark it
	if reverted || err != nil {
		// Find the most recent liquidation at this depth and mark it as failed
		for i := len(t.liquidations) - 1; i >= 0; i-- {
			if t.liquidations[i].CallDepth == depth && !t.liquidations[i].Failed {
				t.liquidations[i].Failed = true
				if err != nil {
					t.liquidations[i].RevertReason = err.Error()
				} else if reverted {
					t.liquidations[i].RevertReason = "reverted"
				}
				break
			}
		}
	}
}

func (t *LiquidationTracer) GetLiquidations() []*LiquidationCall {
	return t.liquidations
}

func (t *LiquidationTracer) Reset() {
	t.liquidations = make([]*LiquidationCall, 0)
}

// ScanConfig holds configuration for the liquidation scanner
type ScanConfig struct {
	DataDir          string
	SparkLendAddress string
	StartBlock       uint64
	EndBlock         uint64 // 0 for latest
	FailedOnly       bool
	ScanAll          bool // Use calldata heuristics
	TraceAll         bool // Trace ALL transactions (default: false, uses TracesToIdx for efficiency)
	NumWorkers       int
	OutputFile       string // If set, stream results to this CSV file as they're found
	AppendMode       bool   // If true, append to existing file instead of overwriting
}

// ScanResult holds the results of a liquidation scan
type ScanResult struct {
	Liquidations    []*LiquidationCall
	CandidateTxs    int
	BlocksProcessed int
	StartBlock      uint64
	EndBlock        uint64
}

// blockTxInfo holds information about a candidate transaction
type blockTxInfo struct {
	blockNum uint64
	txIndex  int
	txNum    uint64
}

// blockJob represents a block to be processed by a worker
type blockJob struct {
	blockNum  uint64
	txIndices []int
}

// StreamingCSVWriter writes liquidation results to a CSV file as they arrive
type StreamingCSVWriter struct {
	file       *os.File
	writer     *csv.Writer
	mu         sync.Mutex
	count      int
	headerDone bool
}

// NewStreamingCSVWriter creates a new streaming CSV writer
func NewStreamingCSVWriter(filename string, appendMode bool) (*StreamingCSVWriter, error) {
	var file *os.File
	var err error

	if appendMode {
		// Check if file exists and has content
		if info, statErr := os.Stat(filename); statErr == nil && info.Size() > 0 {
			file, err = os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return nil, err
			}
			return &StreamingCSVWriter{
				file:       file,
				writer:     csv.NewWriter(file),
				headerDone: true, // Header already exists
			}, nil
		}
	}

	// Create new file
	file, err = os.Create(filename)
	if err != nil {
		return nil, err
	}

	w := &StreamingCSVWriter{
		file:   file,
		writer: csv.NewWriter(file),
	}

	// Write header immediately
	header := []string{
		"block_number",
		"block_timestamp",
		"transaction_hash",
		"tx_index",
		"from_address",
		"to_address",
		"collateral_asset",
		"debt_asset",
		"user",
		"debt_to_cover",
		"receive_atoken",
		"gas_limit",
		"value",
		"call_depth",
		"failed",
		"revert_reason",
		"internal_call_index",
	}
	if err := w.writer.Write(header); err != nil {
		file.Close()
		return nil, err
	}
	w.writer.Flush()
	w.headerDone = true

	return w, nil
}

// Write writes liquidation records to the CSV file (thread-safe)
func (w *StreamingCSVWriter) Write(liquidations []*LiquidationCall) error {
	if len(liquidations) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	for _, r := range liquidations {
		row := []string{
			fmt.Sprintf("%d", r.BlockNumber),
			fmt.Sprintf("%d", r.BlockTimestamp),
			r.TransactionHash,
			fmt.Sprintf("%d", r.TxIndex),
			r.FromAddress,
			r.ToAddress,
			r.CollateralAsset,
			r.DebtAsset,
			r.User,
			r.DebtToCover,
			fmt.Sprintf("%t", r.ReceiveAToken),
			fmt.Sprintf("%d", r.GasLimit),
			r.Value,
			fmt.Sprintf("%d", r.CallDepth),
			fmt.Sprintf("%t", r.Failed),
			r.RevertReason,
			fmt.Sprintf("%d", r.InternalCallIndex),
		}
		if err := w.writer.Write(row); err != nil {
			return err
		}
		w.count++
	}

	// Flush after each batch to ensure data is written to disk
	w.writer.Flush()
	return w.writer.Error()
}

// Count returns the number of records written
func (w *StreamingCSVWriter) Count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

// Close closes the CSV writer and underlying file
func (w *StreamingCSVWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writer.Flush()
	return w.file.Close()
}

// processBlocksParallel processes blocks in parallel using a worker pool
func processBlocksParallel(
	ctx context.Context,
	blockToTxs map[uint64][]int,
	historyDb *temporal.DB,
	blockReader *freezeblocks.BlockReader,
	txNumReader rawdbv3.TxNumsReader,
	chainConfig *chain.Config,
	sparkLendContract common.Address,
	logger erigonlog.Logger,
	totalTxs int,
	failedOnly bool,
	numWorkers int,
	csvWriter *StreamingCSVWriter, // nil for no streaming output
) []*LiquidationCall {
	// Create job channel from map
	jobs := make(chan blockJob, len(blockToTxs))
	go func() {
		for blockNum, txIndices := range blockToTxs {
			select {
			case <-ctx.Done():
				close(jobs)
				return
			case jobs <- blockJob{blockNum, txIndices}:
			}
		}
		close(jobs)
	}()

	return processBlocksFromChannel(ctx, jobs, historyDb, blockReader, txNumReader, chainConfig, sparkLendContract, logger, totalTxs, failedOnly, numWorkers, csvWriter)
}

// processBlocksFromChannel processes blocks from a channel in parallel
// This allows streaming/pipelining - jobs can be sent while processing continues
func processBlocksFromChannel(
	ctx context.Context,
	jobs <-chan blockJob,
	historyDb *temporal.DB,
	blockReader *freezeblocks.BlockReader,
	txNumReader rawdbv3.TxNumsReader,
	chainConfig *chain.Config,
	sparkLendContract common.Address,
	logger erigonlog.Logger,
	totalTxs int, // 0 if unknown (streaming mode)
	failedOnly bool,
	numWorkers int,
	csvWriter *StreamingCSVWriter,
) []*LiquidationCall {
	results := make(chan []*LiquidationCall, 1000)

	var processedCount int64
	startTime := time.Now()

	// Start progress reporter
	progressDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		var lastProcessed int64 = -1 // Track last reported value
		for {
			select {
			case <-progressDone:
				return
			case <-ticker.C:
				processed := atomic.LoadInt64(&processedCount)
				// Only log if there's been progress since last report
				if processed == lastProcessed {
					continue
				}
				lastProcessed = processed
				elapsed := time.Since(startTime)
				txPerSec := float64(processed) / elapsed.Seconds()
				if totalTxs > 0 {
					log.Printf("Progress: %d/%d transactions (%.1f%%), %.1f tx/sec",
						processed, totalTxs, float64(processed)/float64(totalTxs)*100, txPerSec)
				} else {
					log.Printf("Progress: %d transactions processed, %.1f tx/sec",
						processed, txPerSec)
				}
			}
		}
	}()

	// Start workers
	var wg sync.WaitGroup
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own DB transaction and tracer
			historyTx, err := historyDb.BeginTemporalRo(ctx)
			if err != nil {
				log.Printf("Worker %d: failed to begin transaction: %v", workerID, err)
				return
			}
			defer historyTx.Rollback()

			tracer := NewLiquidationTracer(sparkLendContract)
			vmConfig := vm.Config{Tracer: tracer.Tracer()}

			for job := range jobs {
				select {
				case <-ctx.Done():
					return
				default:
				}

				liquidations := processBlock(ctx, job.blockNum, job.txIndices, blockReader, historyTx, txNumReader, chainConfig, tracer, vmConfig, logger, failedOnly)
				atomic.AddInt64(&processedCount, int64(len(job.txIndices)))
				results <- liquidations
			}
		}(w)
	}

	// Wait for all workers to finish, then close results
	go func() {
		wg.Wait()
		// Print final progress
		processed := atomic.LoadInt64(&processedCount)
		elapsed := time.Since(startTime)
		txPerSec := float64(processed) / elapsed.Seconds()
		if totalTxs > 0 {
			log.Printf("Progress: %d/%d transactions (100.0%%), %.1f tx/sec",
				processed, totalTxs, txPerSec)
		} else {
			log.Printf("Progress: %d transactions total, %.1f tx/sec",
				processed, txPerSec)
		}
		close(results)
		close(progressDone)
	}()

	// Collect all results and stream to CSV if writer provided
	var allLiquidations []*LiquidationCall
	for liquidations := range results {
		allLiquidations = append(allLiquidations, liquidations...)
		// Stream to CSV immediately
		if csvWriter != nil && len(liquidations) > 0 {
			if err := csvWriter.Write(liquidations); err != nil {
				log.Printf("Warning: failed to write to CSV: %v", err)
			}
		}
	}

	return allLiquidations
}

// processBlock processes a single block and returns found liquidations
// Optimized: Single-pass execution instead of resetting state for each target
func processBlock(
	ctx context.Context,
	blockNum uint64,
	txIndices []int,
	blockReader *freezeblocks.BlockReader,
	historyTx kv.TemporalTx,
	txNumReader rawdbv3.TxNumsReader,
	chainConfig *chain.Config,
	tracer *LiquidationTracer,
	vmConfig vm.Config,
	logger erigonlog.Logger,
	failedOnly bool,
) []*LiquidationCall {
	var liquidations []*LiquidationCall

	// Get block
	block, err := blockReader.BlockByNumber(ctx, historyTx, blockNum)
	if err != nil || block == nil {
		return liquidations
	}

	// Create historical state reader for this block
	dbstate, err := rpchelper.CreateHistoryStateReader(historyTx, blockNum, 0, txNumReader)
	if err != nil {
		return liquidations
	}

	header := block.Header()
	engine := ethash.NewFullFaker()

	// Create a getHeader function that can read historical headers
	getHeader := func(hash common.Hash, number uint64) (*types.Header, error) {
		h, err := blockReader.Header(ctx, historyTx, hash, number)
		return h, err
	}

	blockContext := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, getHeader), engine, accounts.NilAddress, chainConfig)

	// Sort txIndices to process in order
	sort.Ints(txIndices)

	// Build a set of target indices for O(1) lookup
	targetSet := make(map[int]bool, len(txIndices))
	for _, idx := range txIndices {
		targetSet[idx] = true
	}

	// Find the maximum target index - we only need to execute up to this point
	maxTargetIdx := txIndices[len(txIndices)-1]

	txns := block.Transactions()
	if maxTargetIdx >= len(txns) {
		maxTargetIdx = len(txns) - 1
	}

	// Initialize state ONCE at block start
	intraBlockState := execState.New(dbstate)
	protocol.InitializeBlockExecution(engine, nil, header, chainConfig, intraBlockState, nil, logger, nil)
	gp := new(protocol.GasPool).AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))

	// Single pass: execute transactions up to maxTargetIdx, tracing targets as we reach them
	for i := 0; i <= maxTargetIdx; i++ {
		txn := txns[i]
		intraBlockState.SetTxContext(blockNum, i)

		isTarget := targetSet[i]

		// Optimization: Skip tracing for simple ETH transfers (no input data = can't call SparkLend)
		// Also skip contract creations (no to address)
		isSimpleTransfer := len(txn.GetData()) == 0 || txn.GetTo() == nil
		shouldTrace := isTarget && !isSimpleTransfer

		// Only use tracer for transactions that could potentially call SparkLend
		var currentVmConfig vm.Config
		if shouldTrace {
			tracer.SetTransaction(txn, block, i)
			currentVmConfig = vmConfig
		} else {
			currentVmConfig = vm.Config{}
		}

		msg, _ := txn.AsMessage(*types.MakeSigner(chainConfig, blockNum, header.Time), header.BaseFee, blockContext.Rules(chainConfig))
		txContext := protocol.NewEVMTxContext(msg)

		evm := vm.NewEVM(blockContext, txContext, intraBlockState, chainConfig, currentVmConfig)

		if shouldTrace && tracer.Tracer().OnTxStart != nil {
			tracer.OnTxStart(nil, txn, msg.From())
		}

		_, err := protocol.ApplyMessage(evm, msg, gp, true, false, engine)
		if err != nil {
			// Transaction failed at execution level
		}

		// Finalize state changes
		intraBlockState.FinalizeTx(blockContext.Rules(chainConfig), execState.NewNoopWriter())

		// Collect liquidations from this transaction if it was traced
		if shouldTrace {
			for _, liq := range tracer.GetLiquidations() {
				if failedOnly && !liq.Failed {
					continue
				}
				liquidations = append(liquidations, liq)
			}
			tracer.Reset()
		}
	}

	return liquidations
}

// ScanLiquidations scans the blockchain for liquidation calls based on the provided configuration.
// This is the main entry point for the scanning logic, usable by both CLI and tests.
func ScanLiquidations(ctx context.Context, config ScanConfig) (ScanResult, error) {
	result := ScanResult{
		StartBlock: config.StartBlock,
	}

	// Setup directories
	dirs := datadir.New(config.DataDir)

	chainDataPath := filepath.Join(config.DataDir, "chaindata")
	log.Printf("Opening Erigon database at: %s", chainDataPath)

	// Create a logger for mdbx
	logger := erigonlog.New()

	// Open the MDBX database
	rawDB, err := mdbx.New(kv.Label("chaindata"), logger).
		Path(chainDataPath).
		Readonly(true).
		Open(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to open database: %w", err)
	}
	defer rawDB.Close()

	// Create snapshot configuration for mainnet
	snapCfg := ethconfig.NewSnapCfg(true, true, true, "mainnet")

	// Open block snapshots
	log.Printf("Opening snapshots from: %s", dirs.Snap)
	blockSnapshots := freezeblocks.NewRoSnapshots(snapCfg, dirs.Snap, logger)
	if err := blockSnapshots.OpenFolder(); err != nil {
		return result, fmt.Errorf("failed to open block snapshots: %w", err)
	}
	defer blockSnapshots.Close()

	// Open bor snapshots (for polygon compatibility, can be nil for mainnet)
	borSnapshots := heimdall.NewRoSnapshots(snapCfg, dirs.Snap, logger)
	_ = borSnapshots.OpenFolder()
	defer borSnapshots.Close()

	// Create block reader
	blockReader := freezeblocks.NewBlockReader(blockSnapshots, borSnapshots)

	frozenBlocks := blockReader.FrozenBlocks()
	log.Printf("Frozen blocks available: %d", frozenBlocks)

	// Open the aggregator for historical state
	log.Printf("Opening state aggregator...")
	agg, err := state.New(dirs).SanityOldNaming().Logger(logger).Open(ctx, rawDB)
	if err != nil {
		return result, fmt.Errorf("failed to create aggregator: %w", err)
	}
	defer agg.Close()

	if err := agg.OpenFolder(); err != nil {
		return result, fmt.Errorf("failed to open aggregator folder: %w", err)
	}

	// Create temporal database
	historyDb, err := temporal.New(rawDB, agg)
	if err != nil {
		return result, fmt.Errorf("failed to create temporal database: %w", err)
	}
	defer historyDb.Close()

	// Get chain config
	chainConfig := fromdb.ChainConfig(rawDB)
	log.Printf("Chain: %s", chainConfig.ChainName)

	// Get current block if endBlock is 0
	actualEndBlock := config.EndBlock
	if actualEndBlock == 0 {
		actualEndBlock = frozenBlocks
		log.Printf("Using frozen blocks as end: %d", actualEndBlock)
	}

	// Ensure we don't exceed available blocks
	if actualEndBlock > frozenBlocks {
		log.Printf("Warning: end block %d exceeds frozen blocks %d, using %d", actualEndBlock, frozenBlocks, frozenBlocks)
		actualEndBlock = frozenBlocks
	}

	result.EndBlock = actualEndBlock

	// Parse contract address
	sparkLendContract := common.HexToAddress(config.SparkLendAddress)
	liquidationSelector := common.Hex2Bytes(LiquidationCallSelector)

	historyTx, err := historyDb.BeginTemporalRo(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to begin temporal transaction: %w", err)
	}
	defer historyTx.Rollback()

	txNumReader := blockReader.TxnumReader(ctx)

	var candidateTxs []blockTxInfo

	if config.TraceAll {
		// Trace ALL transactions - most complete but slowest
		log.Printf("Tracing ALL transactions in blocks %d to %d...", config.StartBlock, actualEndBlock)
		log.Printf("This is the most complete method but will be slow for large ranges")

		for blockNum := config.StartBlock; blockNum <= actualEndBlock; blockNum++ {
			if blockNum%1000 == 0 {
				log.Printf("Collecting transactions from block %d...", blockNum)
			}

			block, err := blockReader.BlockByNumber(ctx, historyTx, blockNum)
			if err != nil || block == nil {
				continue
			}

			// Add ALL transactions from this block
			for i := range block.Transactions() {
				baseTxNum, _ := txNumReader.Min(historyTx, blockNum)
				candidateTxs = append(candidateTxs, blockTxInfo{blockNum, i, baseTxNum + uint64(i)})
			}
		}

		log.Printf("Will trace %d transactions total", len(candidateTxs))
	} else if config.ScanAll {
		// Scan ALL transactions in the block range and use calldata heuristics
		log.Printf("Scanning ALL transactions in blocks %d to %d...", config.StartBlock, actualEndBlock)
		log.Printf("Looking for transactions containing SparkLend address or liquidationCall selector in calldata")

		for blockNum := config.StartBlock; blockNum <= actualEndBlock; blockNum++ {
			if blockNum%1000 == 0 {
				log.Printf("Scanning block %d...", blockNum)
			}

			block, err := blockReader.BlockByNumber(ctx, historyTx, blockNum)
			if err != nil || block == nil {
				continue
			}

			for i, txn := range block.Transactions() {
				input := txn.GetData()

				// Check if calldata contains SparkLend address
				containsSparkLend := bytes.Contains(input, sparkLendContract.Bytes())

				// Check if calldata contains liquidationCall selector
				containsSelector := bytes.Contains(input, liquidationSelector)

				// Also check if it directly calls SparkLend
				to := txn.GetTo()
				directCall := to != nil && *to == sparkLendContract

				if containsSparkLend || containsSelector || directCall {
					baseTxNum, _ := txNumReader.Min(historyTx, blockNum)
					candidateTxs = append(candidateTxs, blockTxInfo{blockNum, i, baseTxNum + uint64(i)})
				}
			}
		}

		log.Printf("Found %d candidate transactions with SparkLend/liquidation in calldata", len(candidateTxs))
	} else {
		// Use TracesToIdx to find candidate transactions
		log.Printf("Finding candidate transactions using TracesToIdx...")

		startTxNum, err := txNumReader.Min(historyTx, config.StartBlock)
		if err != nil {
			return result, fmt.Errorf("failed to get start txnum: %w", err)
		}

		endTxNum, err := txNumReader.Max(historyTx, actualEndBlock)
		if err != nil {
			return result, fmt.Errorf("failed to get end txnum: %w", err)
		}

		log.Printf("TxNum range: %d to %d (~%d million txs to scan)", startTxNum, endTxNum, (endTxNum-startTxNum)/1_000_000)

		// Create streaming CSV writer early so we can stream results
		var csvWriter *StreamingCSVWriter
		if config.OutputFile != "" {
			var err error
			csvWriter, err = NewStreamingCSVWriter(config.OutputFile, config.AppendMode)
			if err != nil {
				return ScanResult{}, fmt.Errorf("failed to create output file: %w", err)
			}
			defer csvWriter.Close()
			log.Printf("Streaming results to %s", config.OutputFile)
		}

		// Pipeline: scan index and process blocks concurrently
		// Jobs channel for sending blocks to workers
		jobs := make(chan blockJob, 1000)

		// Start workers immediately - they'll process as jobs arrive
		log.Printf("Starting %d workers for parallel processing...", config.NumWorkers)
		var allLiquidations []*LiquidationCall
		var liquidationsMu sync.Mutex
		processingDone := make(chan struct{})

		go func() {
			results := processBlocksFromChannel(ctx, jobs, historyDb, blockReader, txNumReader, chainConfig, sparkLendContract, logger, 0, config.FailedOnly, config.NumWorkers, csvWriter)
			liquidationsMu.Lock()
			allLiquidations = results
			liquidationsMu.Unlock()
			close(processingDone)
		}()

		// Query TracesToIdx and stream jobs to workers
		timestamps, err := historyTx.IndexRange(kv.TracesToIdx, sparkLendContract.Bytes(), int(startTxNum), int(endTxNum), order.Asc, -1)
		if err != nil {
			close(jobs)
			return result, fmt.Errorf("failed to query TracesToIdx: %w", err)
		}

		// Batch by block to avoid re-reading blocks
		currentBlock := uint64(0)
		currentTxIndices := []int{}
		candidateCount := 0
		blocksFound := 0
		scanStart := time.Now()
		lastLog := time.Now()

		for timestamps.HasNext() {
			select {
			case <-ctx.Done():
				timestamps.Close()
				close(jobs)
				<-processingDone
				return result, ctx.Err()
			default:
			}

			txNum, err := timestamps.Next()
			if err != nil {
				log.Printf("Warning: failed to iterate timestamps: %v", err)
				break
			}

			blockNum, _, err := txNumReader.FindBlockNum(historyTx, txNum)
			if err != nil {
				continue
			}

			baseTxNum, err := txNumReader.Min(historyTx, blockNum)
			if err != nil {
				continue
			}

			txIndex := int(txNum - baseTxNum)
			candidateCount++

			// Batch by block
			if blockNum != currentBlock && currentBlock != 0 {
				// Send previous block's job
				select {
				case <-ctx.Done():
					timestamps.Close()
					close(jobs)
					<-processingDone
					return result, ctx.Err()
				case jobs <- blockJob{currentBlock, currentTxIndices}:
					blocksFound++
				}
				currentTxIndices = []int{}
			}
			currentBlock = blockNum
			currentTxIndices = append(currentTxIndices, txIndex)

			// Log progress every 10 seconds
			if time.Since(lastLog) > 10*time.Second {
				elapsed := time.Since(scanStart)
				// Show block number progress (more meaningful than TxNum position)
				blockProgress := float64(blockNum-config.StartBlock) / float64(actualEndBlock-config.StartBlock) * 100
				log.Printf("Index scan: block %d (%.1f%%), found %d candidates in %d blocks, %.1f candidates/sec",
					blockNum, blockProgress, candidateCount, blocksFound, float64(candidateCount)/elapsed.Seconds())
				lastLog = time.Now()
			}
		}
		timestamps.Close()

		// Send last block if any
		if len(currentTxIndices) > 0 {
			jobs <- blockJob{currentBlock, currentTxIndices}
			blocksFound++
		}
		close(jobs)

		log.Printf("Index scan complete: found %d candidates in %d blocks (took %v)", candidateCount, blocksFound, time.Since(scanStart))

		// Wait for processing to complete
		<-processingDone

		result.CandidateTxs = candidateCount
		result.BlocksProcessed = blocksFound

		// Sort results by block number and tx index
		sort.Slice(allLiquidations, func(i, j int) bool {
			if allLiquidations[i].BlockNumber != allLiquidations[j].BlockNumber {
				return allLiquidations[i].BlockNumber < allLiquidations[j].BlockNumber
			}
			return allLiquidations[i].TxIndex < allLiquidations[j].TxIndex
		})

		result.Liquidations = allLiquidations
		log.Printf("Scan complete. Found %d liquidation calls", len(allLiquidations))

		return result, nil
	}

	if len(candidateTxs) == 0 {
		log.Printf("No candidate transactions found in the specified block range")
		return result, nil
	}

	// Group by block to avoid re-reading blocks
	blockToTxs := make(map[uint64][]int)
	for _, info := range candidateTxs {
		blockToTxs[info.blockNum] = append(blockToTxs[info.blockNum], info.txIndex)
	}

	result.CandidateTxs = len(candidateTxs)
	result.BlocksProcessed = len(blockToTxs)

	log.Printf("Processing %d blocks with %d workers...", len(blockToTxs), config.NumWorkers)

	// Create streaming CSV writer if output file is specified
	var csvWriter *StreamingCSVWriter
	if config.OutputFile != "" {
		var err error
		csvWriter, err = NewStreamingCSVWriter(config.OutputFile, config.AppendMode)
		if err != nil {
			return ScanResult{}, fmt.Errorf("failed to create output file: %w", err)
		}
		defer csvWriter.Close()
		log.Printf("Streaming results to %s", config.OutputFile)
	}

	allLiquidations := processBlocksParallel(ctx, blockToTxs, historyDb, blockReader, txNumReader, chainConfig, sparkLendContract, logger, len(candidateTxs), config.FailedOnly, config.NumWorkers, csvWriter)

	// Sort results by block number and tx index
	sort.Slice(allLiquidations, func(i, j int) bool {
		if allLiquidations[i].BlockNumber != allLiquidations[j].BlockNumber {
			return allLiquidations[i].BlockNumber < allLiquidations[j].BlockNumber
		}
		return allLiquidations[i].TxIndex < allLiquidations[j].TxIndex
	})

	result.Liquidations = allLiquidations
	log.Printf("Scan complete. Found %d liquidation calls", len(allLiquidations))

	return result, nil
}

// getLastProcessedBlock reads the output CSV file and returns the highest block number found
func getLastProcessedBlock(filename string) (uint64, error) {
	file, err := os.Open(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // File doesn't exist, start from beginning
		}
		return 0, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Skip header
	if _, err := reader.Read(); err != nil {
		return 0, nil // Empty file or error reading header
	}

	var maxBlock uint64
	for {
		record, err := reader.Read()
		if err != nil {
			break // EOF or error
		}
		if len(record) < 1 {
			continue
		}
		// First column is block_number
		blockStr := strings.TrimSpace(record[0])
		var block uint64
		if _, err := fmt.Sscanf(blockStr, "%d", &block); err == nil {
			if block > maxBlock {
				maxBlock = block
			}
		}
	}

	return maxBlock, nil
}

func main() {
	// Command line flags
	dataDirFlag := flag.String("data-dir", DefaultDataDir, "Erigon data directory")
	sparkLendAddress := flag.String("contract", DefaultSparkLendPool, "SparkLend Pool contract address")
	startBlock := flag.Uint64("start", DefaultStartBlock, "Start block number")
	endBlock := flag.Uint64("end", 0, "End block number (0 for latest)")
	outputFile := flag.String("output", "liquidation_calls.csv", "Output CSV file")
	jsonOutput := flag.Bool("json", false, "Also output JSON file")
	failedOnly := flag.Bool("failed-only", true, "Only output failed liquidations")
	scanAll := flag.Bool("scan-all", false, "Scan all transactions using calldata heuristics (faster but may miss some)")
	traceAll := flag.Bool("trace-all", false, "Trace ALL transactions (slow; default uses TracesToIdx which is ~50x faster)")
	numWorkers := flag.Int("workers", 1, "Number of parallel workers (default 1; more workers often slower due to DB contention)")
	resume := flag.Bool("resume", false, "Resume from last processed block (reads from output file)")
	flag.Parse()

	// Handle resume: find last block in output file and start from there
	actualStartBlock := *startBlock
	if *resume {
		if lastBlock, err := getLastProcessedBlock(*outputFile); err == nil && lastBlock > 0 {
			actualStartBlock = lastBlock + 1
			log.Printf("Resuming from block %d (last processed: %d)", actualStartBlock, lastBlock)
		} else if err != nil {
			log.Printf("Warning: could not read resume point: %v, starting from %d", err, *startBlock)
		}
	}

	log.Printf("Liquidation Call Scanner (Re-executing transactions with tracer)")
	log.Printf("=================================================================")
	log.Printf("Data Directory: %s", *dataDirFlag)
	log.Printf("Contract: %s", *sparkLendAddress)
	log.Printf("Start Block: %d", actualStartBlock)
	if *endBlock > 0 {
		log.Printf("End Block: %d", *endBlock)
	} else {
		log.Printf("End Block: latest")
	}
	log.Printf("Output: %s", *outputFile)
	log.Printf("Failed Only: %v", *failedOnly)
	log.Printf("Scan All (calldata): %v", *scanAll)
	log.Printf("Trace All: %v (default uses TracesToIdx for ~50x speedup)", *traceAll)
	log.Printf("Workers: %d", *numWorkers)
	log.Printf("=================================================================")

	// Check if we've already completed the range
	if *endBlock > 0 && actualStartBlock > *endBlock {
		log.Printf("Already completed: start block %d > end block %d", actualStartBlock, *endBlock)
		fmt.Println("\nScan complete!")
		return
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// Build config from flags - OutputFile enables streaming writes
	config := ScanConfig{
		DataDir:          *dataDirFlag,
		SparkLendAddress: *sparkLendAddress,
		StartBlock:       actualStartBlock,
		EndBlock:         *endBlock,
		FailedOnly:       *failedOnly,
		ScanAll:          *scanAll,
		TraceAll:         *traceAll,
		NumWorkers:       *numWorkers,
		OutputFile:       *outputFile, // Stream results to CSV as they're found
		AppendMode:       *resume,     // Append to existing file when resuming
	}

	// Run the scan
	result, err := ScanLiquidations(ctx, config)
	if err != nil {
		log.Fatalf("Scan failed: %v", err)
	}

	// CSV is written incrementally during scan via OutputFile
	// Only write JSON at the end if requested
	if len(result.Liquidations) > 0 {
		log.Printf("Wrote %d liquidation calls to %s", len(result.Liquidations), *outputFile)

		if *jsonOutput {
			jsonFile := strings.TrimSuffix(*outputFile, ".csv") + ".json"
			if err := writeJSON(jsonFile, result.Liquidations); err != nil {
				log.Fatalf("Failed to write JSON: %v", err)
			}
			log.Printf("Wrote %d liquidation calls to %s", len(result.Liquidations), jsonFile)
		}
	} else {
		log.Printf("No liquidation calls found")
	}

	fmt.Println("\nScan complete!")
}

// LiquidationArgs holds decoded liquidation call arguments
type LiquidationArgs struct {
	CollateralAsset string
	DebtAsset       string
	User            string
	DebtToCover     string
	ReceiveAToken   bool
}

// decodeLiquidationCall decodes the liquidationCall function arguments
func decodeLiquidationCall(data []byte) LiquidationArgs {
	args := LiquidationArgs{}

	if len(data) < 4+160 { // 4 bytes selector + 5 params * 32 bytes
		return args
	}

	// Remove the function selector
	data = data[4:]

	args.CollateralAsset = common.BytesToAddress(data[0:32]).Hex()
	args.DebtAsset = common.BytesToAddress(data[32:64]).Hex()
	args.User = common.BytesToAddress(data[64:96]).Hex()
	args.DebtToCover = new(big.Int).SetBytes(data[96:128]).String()
	args.ReceiveAToken = new(big.Int).SetBytes(data[128:160]).Cmp(big.NewInt(0)) != 0

	return args
}

// writeJSON writes the results to a JSON file
func writeJSON(filename string, results []*LiquidationCall) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(results)
}

// Ensure imports are used
var _ = rawdbv3.TxNums
