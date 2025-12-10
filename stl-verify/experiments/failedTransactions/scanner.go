package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

const (
	// MaxRetries for RPC calls
	MaxRetries = 3

	// RetryDelay between retries
	RetryDelay = 2 * time.Second
)

// Scanner handles blockchain scanning for failed transactions
type Scanner struct {
	client             *ethclient.Client
	rpcClient          *rpc.Client // Raw RPC client for batch calls and custom methods
	db                 *Database
	sparkLendContract  common.Address
	startBlock         uint64
	endBlock           uint64
	concurrency        int
	checkpointInterval uint64
	useTracing         bool // Use debug_traceTransaction for 100% accuracy
	skipRevertReason   bool // Skip fetching revert reason (faster)
	batchSize          int  // Number of blocks to fetch in a batch
}

// BlockResult holds the result of processing a block
type BlockResult struct {
	BlockNum  uint64
	FailedTxs []*FailedTransaction
	Err       error
}

// BatchBlockResult holds the result of processing a batch of blocks
type BatchBlockResult struct {
	Results []BlockResult
}

// ReceiptJSON represents a transaction receipt in JSON-RPC format
type ReceiptJSON struct {
	TransactionHash string `json:"transactionHash"`
	Status          string `json:"status"`
	GasUsed         string `json:"gasUsed"`
}

// NewScanner creates a new Scanner instance
func NewScanner(client *ethclient.Client, rpcClient *rpc.Client, db *Database, sparkLendAddress string, startBlock, endBlock uint64, concurrency int, checkpointInterval uint64, useTracing bool, skipRevertReason bool, batchSize int) *Scanner {
	if concurrency < 1 {
		concurrency = 1
	}
	if checkpointInterval < 1 {
		checkpointInterval = 1000
	}
	if batchSize < 1 {
		batchSize = 10
	}
	return &Scanner{
		client:             client,
		rpcClient:          rpcClient,
		db:                 db,
		sparkLendContract:  common.HexToAddress(sparkLendAddress),
		startBlock:         startBlock,
		endBlock:           endBlock,
		concurrency:        concurrency,
		checkpointInterval: checkpointInterval,
		useTracing:         useTracing,
		skipRevertReason:   skipRevertReason,
		batchSize:          batchSize,
	}
}

// Start begins the scanning process
func (s *Scanner) Start(ctx context.Context) error {
	// Check for existing sync state to resume
	syncState, err := s.db.GetSyncState()
	if err != nil {
		return fmt.Errorf("failed to get sync state: %w", err)
	}

	startBlock := s.startBlock
	if syncState != nil && syncState.LastProcessedBlock >= startBlock {
		startBlock = syncState.LastProcessedBlock + 1
		log.Printf("Resuming from block %d (last checkpoint)", startBlock)
	} else {
		log.Printf("Starting fresh scan from block %d", startBlock)
	}

	// Get current block if endBlock is 0
	endBlock := s.endBlock
	if endBlock == 0 {
		header, err := s.client.HeaderByNumber(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to get latest block: %w", err)
		}
		endBlock = header.Number.Uint64()
		log.Printf("Scanning up to latest block %d", endBlock)
	}

	log.Printf("Scanning blocks %d to %d for failed SparkLend transactions", startBlock, endBlock)
	log.Printf("Target contract: %s", s.sparkLendContract.Hex())
	log.Printf("Using %d concurrent workers with batch size %d", s.concurrency, s.batchSize)

	// Use atomic counter for thread-safe counting
	var failedTxCount int64
	var processedBlocks int64
	lastCheckpoint := startBlock
	totalBlocks := endBlock - startBlock + 1

	// Create channels for work distribution - now sending batches of block numbers
	batchChan := make(chan []uint64, s.concurrency*2)
	resultChan := make(chan BlockResult, s.concurrency*s.batchSize*2)

	// Create a WaitGroup for workers
	var wg sync.WaitGroup

	// Start worker goroutines - each processes a batch of blocks
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for blockNums := range batchChan {
				// Batch fetch all blocks
				blocks, err := s.getBatchBlocks(ctx, blockNums)
				if err != nil {
					log.Printf("Warning: batch block fetch failed: %v, falling back to individual fetches", err)
					// Fallback to individual processing
					for _, blockNum := range blockNums {
						var failedTxs []*FailedTransaction
						var err error
						if s.useTracing {
							failedTxs, err = s.processBlockWithTracing(ctx, blockNum)
						} else {
							failedTxs, err = s.processBlockConcurrent(ctx, blockNum)
						}
						resultChan <- BlockResult{
							BlockNum:  blockNum,
							FailedTxs: failedTxs,
							Err:       err,
						}
					}
					continue
				}

				// Process each block in the batch
				for _, blockNum := range blockNums {
					block, ok := blocks[blockNum]
					if !ok || block == nil {
						resultChan <- BlockResult{
							BlockNum: blockNum,
							Err:      fmt.Errorf("block %d not found in batch response", blockNum),
						}
						continue
					}

					var failedTxs []*FailedTransaction
					var err error
					if s.useTracing {
						failedTxs, err = s.processBlockWithTracingPreloaded(ctx, block)
					} else {
						failedTxs, err = s.processBlockConcurrentPreloaded(ctx, block)
					}
					resultChan <- BlockResult{
						BlockNum:  blockNum,
						FailedTxs: failedTxs,
						Err:       err,
					}
				}
			}
		}(i)
	}

	// Start a goroutine to close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Start a goroutine to feed block batches to workers
	go func() {
		defer close(batchChan)
		batch := make([]uint64, 0, s.batchSize)
		for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
			batch = append(batch, blockNum)
			if len(batch) >= s.batchSize {
				select {
				case <-ctx.Done():
					return
				case batchChan <- batch:
				}
				batch = make([]uint64, 0, s.batchSize)
			}
		}
		// Send remaining blocks
		if len(batch) > 0 {
			select {
			case <-ctx.Done():
				return
			case batchChan <- batch:
			}
		}
	}()

	// Process results
	var highestProcessedBlock uint64 = startBlock
	for result := range resultChan {
		select {
		case <-ctx.Done():
			// Save state before exiting
			if err := s.db.SaveSyncState(highestProcessedBlock); err != nil {
				log.Printf("Warning: failed to save sync state on exit: %v", err)
			}
			return ctx.Err()
		default:
		}

		if result.Err != nil {
			log.Printf("Error processing block %d: %v", result.BlockNum, result.Err)
			atomic.AddInt64(&processedBlocks, 1)
			continue
		}

		// Store failed transactions in database
		for _, failedTx := range result.FailedTxs {
			if err := s.db.InsertFailedTransaction(failedTx); err != nil {
				log.Printf("Warning: failed to store tx %s: %v", failedTx.TransactionHash, err)
				continue
			}
			atomic.AddInt64(&failedTxCount, 1)
			log.Printf("Found failed %s tx: %s at block %d", failedTx.FunctionName, failedTx.TransactionHash, result.BlockNum)
		}

		// Track highest processed block for checkpointing
		if result.BlockNum > highestProcessedBlock {
			highestProcessedBlock = result.BlockNum
		}

		processed := atomic.AddInt64(&processedBlocks, 1)

		// Log progress every 1000 blocks
		if processed%1000 == 0 {
			log.Printf("Progress: %d/%d blocks (%.2f%%), failed txs found: %d",
				processed, totalBlocks, float64(processed)/float64(totalBlocks)*100, atomic.LoadInt64(&failedTxCount))
		}

		// Save checkpoint every checkpointInterval blocks
		if highestProcessedBlock-lastCheckpoint >= s.checkpointInterval {
			if err := s.db.SaveSyncState(highestProcessedBlock); err != nil {
				log.Printf("Warning: failed to save checkpoint at block %d: %v", highestProcessedBlock, err)
			} else {
				log.Printf("Checkpoint saved at block %d", highestProcessedBlock)
				lastCheckpoint = highestProcessedBlock
			}
		}
	}

	// Final checkpoint
	if err := s.db.SaveSyncState(endBlock); err != nil {
		log.Printf("Warning: failed to save final sync state: %v", err)
	}

	log.Printf("Scan complete. Total failed transactions found: %d", atomic.LoadInt64(&failedTxCount))

	return nil
}

// LiquidationCallSelector is the function selector for liquidationCall
const LiquidationCallSelector = "00a718a9"

// processBlockConcurrent processes all transactions in a block and returns failed transactions
// This version is safe for concurrent use as it doesn't write to the database directly
func (s *Scanner) processBlockConcurrent(ctx context.Context, blockNum uint64) ([]*FailedTransaction, error) {
	block, err := s.getBlockWithRetry(ctx, blockNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	return s.processBlockConcurrentPreloaded(ctx, block)
}

// processBlockConcurrentPreloaded processes a pre-fetched block
func (s *Scanner) processBlockConcurrentPreloaded(ctx context.Context, block *types.Block) ([]*FailedTransaction, error) {
	blockNum := block.NumberU64()

	// Fetch all receipts for the block in one call (major performance improvement)
	receiptMap, err := s.getBlockReceiptsAll(ctx, blockNum)
	if err != nil {
		// Fallback to individual receipt fetching if eth_getBlockReceipts not supported
		return s.processBlockConcurrentFallback(ctx, block)
	}

	var failedTxs []*FailedTransaction
	blockTimestamp := block.Time()
	sparkLendAddrLower := strings.ToLower(s.sparkLendContract.Hex()[2:]) // Remove "0x" prefix

	for _, tx := range block.Transactions() {
		if tx.To() == nil {
			continue
		}

		var funcName string
		var isRelevant bool
		var isIndirectCall bool

		// Case 1: Direct call to SparkLend contract
		if *tx.To() == s.sparkLendContract {
			funcName, isRelevant = GetFunctionName(tx.Data())
			if !isRelevant {
				continue
			}
		} else {
			// Case 2: Indirect call via MEV bot/aggregator
			// Check if calldata contains both SparkLend address AND liquidationCall selector
			calldataHex := strings.ToLower(hex.EncodeToString(tx.Data()))

			containsSparkLend := strings.Contains(calldataHex, sparkLendAddrLower)
			containsLiquidationSelector := strings.Contains(calldataHex, LiquidationCallSelector)

			if containsSparkLend && containsLiquidationSelector {
				funcName = "liquidationCall"
				isRelevant = true
				isIndirectCall = true
			} else {
				continue
			}
		}

		// Get receipt from the pre-fetched map
		receiptJSON, ok := receiptMap[tx.Hash()]
		if !ok {
			log.Printf("Warning: receipt not found for tx %s in block %d", tx.Hash().Hex(), blockNum)
			continue
		}

		// Skip successful transactions (status = 0x1)
		if receiptJSON.Status == "0x1" {
			continue
		}

		// This is a failed transaction - process it
		var failedTx *FailedTransaction
		if isIndirectCall {
			failedTx, err = s.processFailedIndirectTransactionJSON(ctx, tx, receiptJSON, block, funcName, blockTimestamp)
		} else {
			failedTx, err = s.processFailedTransactionJSON(ctx, tx, receiptJSON, block, funcName, blockTimestamp)
		}
		if err != nil {
			log.Printf("Warning: failed to process tx %s: %v", tx.Hash().Hex(), err)
			continue
		}

		failedTxs = append(failedTxs, failedTx)
	}

	return failedTxs, nil
}

// processBlockConcurrentFallback is the old implementation that fetches receipts one by one
// Used as fallback when eth_getBlockReceipts is not available
func (s *Scanner) processBlockConcurrentFallback(ctx context.Context, block *types.Block) ([]*FailedTransaction, error) {
	var failedTxs []*FailedTransaction
	blockTimestamp := block.Time()
	sparkLendAddrLower := strings.ToLower(s.sparkLendContract.Hex()[2:])

	for _, tx := range block.Transactions() {
		if tx.To() == nil {
			continue
		}

		var funcName string
		var isRelevant bool
		var isIndirectCall bool

		if *tx.To() == s.sparkLendContract {
			funcName, isRelevant = GetFunctionName(tx.Data())
			if !isRelevant {
				continue
			}
		} else {
			calldataHex := strings.ToLower(hex.EncodeToString(tx.Data()))
			containsSparkLend := strings.Contains(calldataHex, sparkLendAddrLower)
			containsLiquidationSelector := strings.Contains(calldataHex, LiquidationCallSelector)

			if containsSparkLend && containsLiquidationSelector {
				funcName = "liquidationCall"
				isRelevant = true
				isIndirectCall = true
			} else {
				continue
			}
		}

		receipt, err := s.getReceiptWithRetry(ctx, tx.Hash())
		if err != nil {
			log.Printf("Warning: failed to get receipt for tx %s: %v", tx.Hash().Hex(), err)
			continue
		}

		if receipt.Status == types.ReceiptStatusSuccessful {
			continue
		}

		var failedTx *FailedTransaction
		if isIndirectCall {
			failedTx, err = s.processFailedIndirectTransaction(ctx, tx, receipt, block, funcName, blockTimestamp)
		} else {
			failedTx, err = s.processFailedTransaction(ctx, tx, receipt, block, funcName, blockTimestamp)
		}
		if err != nil {
			log.Printf("Warning: failed to process tx %s: %v", tx.Hash().Hex(), err)
			continue
		}

		failedTxs = append(failedTxs, failedTx)
	}

	return failedTxs, nil
}

// processFailedTransaction processes a single failed transaction
func (s *Scanner) processFailedTransaction(
	ctx context.Context,
	tx *types.Transaction,
	receipt *types.Receipt,
	block *types.Block,
	funcName string,
	blockTimestamp uint64,
) (*FailedTransaction, error) {
	// Decode function arguments
	argsJSON, err := DecodeCalldata(funcName, tx.Data())
	if err != nil {
		argsJSON = fmt.Sprintf(`{"raw": "0x%s", "error": "%s"}`, hex.EncodeToString(tx.Data()), err.Error())
	}

	// Get the sender address
	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	// Get revert reason by re-simulating the call (if not skipped)
	var revertReason string
	if !s.skipRevertReason {
		revertReason = s.getRevertReason(ctx, tx, block.NumberU64())
	}

	return &FailedTransaction{
		BlockNumber:     block.NumberU64(),
		BlockTimestamp:  blockTimestamp,
		TransactionHash: tx.Hash().Hex(),
		FromAddress:     from.Hex(),
		ToAddress:       tx.To().Hex(),
		FunctionName:    funcName,
		FunctionArgs:    argsJSON,
		GasUsed:         fmt.Sprintf("%d", receipt.GasUsed),
		RevertReason:    revertReason,
	}, nil
}

// processFailedTransactionJSON processes a failed transaction using JSON receipt data
func (s *Scanner) processFailedTransactionJSON(
	ctx context.Context,
	tx *types.Transaction,
	receiptJSON *ReceiptJSON,
	block *types.Block,
	funcName string,
	blockTimestamp uint64,
) (*FailedTransaction, error) {
	argsJSON, err := DecodeCalldata(funcName, tx.Data())
	if err != nil {
		argsJSON = fmt.Sprintf(`{"raw": "0x%s", "error": "%s"}`, hex.EncodeToString(tx.Data()), err.Error())
	}

	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	var revertReason string
	if !s.skipRevertReason {
		revertReason = s.getRevertReason(ctx, tx, block.NumberU64())
	}

	return &FailedTransaction{
		BlockNumber:     block.NumberU64(),
		BlockTimestamp:  blockTimestamp,
		TransactionHash: tx.Hash().Hex(),
		FromAddress:     from.Hex(),
		ToAddress:       tx.To().Hex(),
		FunctionName:    funcName,
		FunctionArgs:    argsJSON,
		GasUsed:         strings.TrimPrefix(receiptJSON.GasUsed, "0x"),
		RevertReason:    revertReason,
	}, nil
}

// processFailedIndirectTransactionJSON processes a failed indirect transaction using JSON receipt data
func (s *Scanner) processFailedIndirectTransactionJSON(
	ctx context.Context,
	tx *types.Transaction,
	receiptJSON *ReceiptJSON,
	block *types.Block,
	funcName string,
	blockTimestamp uint64,
) (*FailedTransaction, error) {
	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	argsJSON := s.extractLiquidationCallArgs(tx.Data())
	var revertReason string
	if !s.skipRevertReason {
		revertReason = s.getRevertReason(ctx, tx, block.NumberU64())
	}

	// Parse hex gas used to decimal
	gasUsedHex := strings.TrimPrefix(receiptJSON.GasUsed, "0x")
	gasUsed, _ := new(big.Int).SetString(gasUsedHex, 16)

	return &FailedTransaction{
		BlockNumber:     block.NumberU64(),
		BlockTimestamp:  blockTimestamp,
		TransactionHash: tx.Hash().Hex(),
		FromAddress:     from.Hex(),
		ToAddress:       tx.To().Hex(),
		FunctionName:    funcName + " (via aggregator)",
		FunctionArgs:    argsJSON,
		GasUsed:         gasUsed.String(),
		RevertReason:    revertReason,
	}, nil
}

// processFailedIndirectTransaction processes a failed transaction that went through an aggregator/MEV bot
// It attempts to extract the liquidationCall parameters from the embedded calldata
func (s *Scanner) processFailedIndirectTransaction(
	ctx context.Context,
	tx *types.Transaction,
	receipt *types.Receipt,
	block *types.Block,
	funcName string,
	blockTimestamp uint64,
) (*FailedTransaction, error) {
	// Get the sender address
	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	// Try to extract liquidationCall args from the calldata
	argsJSON := s.extractLiquidationCallArgs(tx.Data())

	// Get revert reason by re-simulating the call (if not skipped)
	var revertReason string
	if !s.skipRevertReason {
		revertReason = s.getRevertReason(ctx, tx, block.NumberU64())
	}

	return &FailedTransaction{
		BlockNumber:     block.NumberU64(),
		BlockTimestamp:  blockTimestamp,
		TransactionHash: tx.Hash().Hex(),
		FromAddress:     from.Hex(),
		ToAddress:       tx.To().Hex(),
		FunctionName:    funcName + " (via aggregator)",
		FunctionArgs:    argsJSON,
		GasUsed:         fmt.Sprintf("%d", receipt.GasUsed),
		RevertReason:    revertReason,
	}, nil
}

// extractLiquidationCallArgs attempts to find and decode liquidationCall parameters from embedded calldata
func (s *Scanner) extractLiquidationCallArgs(data []byte) string {
	calldataHex := hex.EncodeToString(data)

	// Look for the liquidationCall selector in the calldata
	idx := strings.Index(calldataHex, LiquidationCallSelector)
	if idx == -1 {
		return fmt.Sprintf(`{"raw": "0x%s", "note": "liquidationCall selector found but args not extractable"}`, calldataHex)
	}

	// The selector is 4 bytes = 8 hex chars
	// After selector: collateralAsset(32) + debtAsset(32) + user(32) + debtToCover(32) + receiveAToken(32) = 160 bytes = 320 hex chars
	argsStart := idx + 8 // skip the 4-byte selector
	argsEnd := argsStart + 320

	if argsEnd > len(calldataHex) {
		return fmt.Sprintf(`{"raw": "0x%s", "note": "insufficient data after selector"}`, calldataHex)
	}

	argsHex := calldataHex[argsStart:argsEnd]
	argsBytes, err := hex.DecodeString(argsHex)
	if err != nil {
		return fmt.Sprintf(`{"raw": "0x%s", "error": "failed to decode args hex"}`, calldataHex)
	}

	// Decode the 5 parameters (each 32 bytes)
	args := make(map[string]interface{})
	args["collateralAsset"] = common.BytesToAddress(argsBytes[0:32]).Hex()
	args["debtAsset"] = common.BytesToAddress(argsBytes[32:64]).Hex()
	args["user"] = common.BytesToAddress(argsBytes[64:96]).Hex()
	args["debtToCover"] = new(big.Int).SetBytes(argsBytes[96:128]).String()
	args["receiveAToken"] = argsBytes[159] != 0 // Last byte of the bool word
	args["viaAggregator"] = true

	jsonBytes, err := json.Marshal(args)
	if err != nil {
		return fmt.Sprintf(`{"raw": "0x%s", "error": "failed to marshal args"}`, calldataHex)
	}

	return string(jsonBytes)
}

// getRevertReason simulates the call to get the revert reason
func (s *Scanner) getRevertReason(ctx context.Context, tx *types.Transaction, blockNum uint64) string {
	// Create call message
	msg := ethereum.CallMsg{
		To:    tx.To(),
		Data:  tx.Data(),
		Value: tx.Value(),
		Gas:   tx.Gas(),
	}

	// Try to get the sender for the From field
	signer := types.LatestSignerForChainID(tx.ChainId())
	if from, err := types.Sender(signer, tx); err == nil {
		msg.From = from
	}

	// Set gas price based on transaction type
	if tx.Type() == types.DynamicFeeTxType {
		msg.GasFeeCap = tx.GasFeeCap()
		msg.GasTipCap = tx.GasTipCap()
	} else {
		msg.GasPrice = tx.GasPrice()
	}

	// Simulate the call at the block before the transaction
	blockNumBig := new(big.Int).SetUint64(blockNum)
	_, err := s.client.CallContract(ctx, msg, blockNumBig)
	if err != nil {
		// Extract revert reason from error
		errStr := err.Error()

		// Check for execution reverted with data
		if strings.Contains(errStr, "execution reverted") {
			// Try to extract hex data from error
			if idx := strings.Index(errStr, "0x"); idx != -1 {
				hexPart := errStr[idx:]
				// Clean up the hex part
				if spaceIdx := strings.IndexAny(hexPart, " \n\t"); spaceIdx != -1 {
					hexPart = hexPart[:spaceIdx]
				}
				decoded := DecodeRevertReasonFromHex(hexPart)
				if decoded != "" && decoded != hexPart {
					return decoded
				}
				return hexPart
			}
			return "execution reverted"
		}

		return errStr
	}

	return "unknown"
}

// getBlockWithRetry gets a block with retry logic
func (s *Scanner) getBlockWithRetry(ctx context.Context, blockNum uint64) (*types.Block, error) {
	var block *types.Block
	var err error

	for i := 0; i < MaxRetries; i++ {
		block, err = s.client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
		if err == nil {
			return block, nil
		}

		if i < MaxRetries-1 {
			time.Sleep(RetryDelay)
		}
	}

	return nil, err
}

// getReceiptWithRetry gets a transaction receipt with retry logic
func (s *Scanner) getReceiptWithRetry(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	var receipt *types.Receipt
	var err error

	for i := 0; i < MaxRetries; i++ {
		receipt, err = s.client.TransactionReceipt(ctx, txHash)
		if err == nil {
			return receipt, nil
		}

		if i < MaxRetries-1 {
			time.Sleep(RetryDelay)
		}
	}

	return nil, err
}

// rpcBlock is used for JSON-RPC block parsing (mirrors go-ethereum's ethclient approach)
type rpcBlock struct {
	Hash         *common.Hash        `json:"hash"`
	Transactions []rpcTransaction    `json:"transactions"`
	UncleHashes  []common.Hash       `json:"uncles"`
	Withdrawals  []*types.Withdrawal `json:"withdrawals,omitempty"`
}

// rpcTransaction wraps types.Transaction with extra info from JSON-RPC
type rpcTransaction struct {
	tx *types.Transaction
	txExtraInfo
}

// txExtraInfo holds extra transaction fields from JSON-RPC that aren't in types.Transaction
type txExtraInfo struct {
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

// UnmarshalJSON implements json.Unmarshaler for rpcTransaction
func (tx *rpcTransaction) UnmarshalJSON(msg []byte) error {
	if err := json.Unmarshal(msg, &tx.tx); err != nil {
		return err
	}
	return json.Unmarshal(msg, &tx.txExtraInfo)
}

// getBatchBlocks fetches multiple blocks in a single batch RPC call using rpc.BatchCall
// Uses json.RawMessage approach from go-ethereum's ethclient to properly unmarshal blocks
func (s *Scanner) getBatchBlocks(ctx context.Context, blockNums []uint64) (map[uint64]*types.Block, error) {
	if len(blockNums) == 0 {
		return nil, nil
	}

	// Build batch elements using json.RawMessage as result type
	batch := make([]rpc.BatchElem, len(blockNums))
	results := make([]json.RawMessage, len(blockNums))

	for i, blockNum := range blockNums {
		batch[i] = rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{rpc.BlockNumber(blockNum), true}, // true = include full tx objects
			Result: &results[i],
		}
	}

	var lastErr error
	for retry := 0; retry < MaxRetries; retry++ {
		err := s.rpcClient.BatchCallContext(ctx, batch)
		if err != nil {
			lastErr = err
			if retry < MaxRetries-1 {
				time.Sleep(RetryDelay)
			}
			continue
		}

		// Parse blocks from raw JSON responses using go-ethereum's pattern:
		// 1. Unmarshal into types.Header (has proper JSON support)
		// 2. Unmarshal into rpcBlock for transactions and other body data
		// 3. Construct block using NewBlockWithHeader().WithBody()
		blocks := make(map[uint64]*types.Block)
		for i, elem := range batch {
			if elem.Error != nil {
				log.Printf("Warning: failed to get block %d: %v", blockNums[i], elem.Error)
				continue
			}

			raw := results[i]
			if len(raw) == 0 || string(raw) == "null" {
				log.Printf("Warning: block %d returned null", blockNums[i])
				continue
			}

			// Decode header first
			var head *types.Header
			if err := json.Unmarshal(raw, &head); err != nil {
				log.Printf("Warning: failed to unmarshal header for block %d: %v", blockNums[i], err)
				continue
			}
			if head == nil {
				log.Printf("Warning: block %d header is nil", blockNums[i])
				continue
			}

			// Decode body (transactions, uncles, withdrawals)
			var body rpcBlock
			if err := json.Unmarshal(raw, &body); err != nil {
				log.Printf("Warning: failed to unmarshal body for block %d: %v", blockNums[i], err)
				continue
			}

			// Extract transactions
			txs := make([]*types.Transaction, len(body.Transactions))
			for j, tx := range body.Transactions {
				txs[j] = tx.tx
			}

			// Construct the block
			block := types.NewBlockWithHeader(head).WithBody(types.Body{
				Transactions: txs,
				Withdrawals:  body.Withdrawals,
			})

			blocks[blockNums[i]] = block
		}

		return blocks, nil
	}

	return nil, lastErr
}

// getBlockReceiptsAll fetches all receipts for a block in a single RPC call
// This is much more efficient than fetching receipts one by one
func (s *Scanner) getBlockReceiptsAll(ctx context.Context, blockNum uint64) (map[common.Hash]*ReceiptJSON, error) {
	var lastErr error
	for i := 0; i < MaxRetries; i++ {
		var receipts []ReceiptJSON
		err := s.rpcClient.CallContext(ctx, &receipts, "eth_getBlockReceipts", rpc.BlockNumber(blockNum))
		if err != nil {
			lastErr = err
			if i < MaxRetries-1 {
				time.Sleep(RetryDelay)
			}
			continue
		}

		// Build map by transaction hash
		receiptMap := make(map[common.Hash]*ReceiptJSON, len(receipts))
		for j := range receipts {
			txHash := common.HexToHash(receipts[j].TransactionHash)
			receiptMap[txHash] = &receipts[j]
		}

		return receiptMap, nil
	}

	return nil, lastErr
}

// ============================================================================
// TRACING-BASED APPROACH (100% accurate but slower)
// Uses trace_replayTransaction to find ALL internal calls to SparkLend
// ============================================================================

// TraceAction represents the action in a trace entry (trace_replayTransaction format)
type TraceAction struct {
	From     string `json:"from"`
	To       string `json:"to"`
	CallType string `json:"callType"`
	Input    string `json:"input"`
	Value    string `json:"value"`
	Gas      string `json:"gas"`
}

// TraceResultField represents the result field in a trace entry
type TraceResultField struct {
	Output  string `json:"output"`
	GasUsed string `json:"gasUsed"`
}

// TraceEntry represents a single entry in trace_replayTransaction output
type TraceEntry struct {
	Action       TraceAction       `json:"action"`
	Result       *TraceResultField `json:"result,omitempty"`
	Error        string            `json:"error,omitempty"`
	Subtraces    int               `json:"subtraces"`
	TraceAddress []int             `json:"traceAddress"`
	Type         string            `json:"type"`
}

// TraceReplayResult represents the result of trace_replayTransaction
type TraceReplayResult struct {
	Trace []TraceEntry `json:"trace"`
}

// processBlockWithTracing processes a block using transaction tracing for 100% accuracy
func (s *Scanner) processBlockWithTracing(ctx context.Context, blockNum uint64) ([]*FailedTransaction, error) {
	block, err := s.getBlockWithRetry(ctx, blockNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	return s.processBlockWithTracingPreloaded(ctx, block)
}

// processBlockWithTracingPreloaded processes a pre-fetched block using tracing
func (s *Scanner) processBlockWithTracingPreloaded(ctx context.Context, block *types.Block) ([]*FailedTransaction, error) {
	blockNum := block.NumberU64()

	// Fetch all receipts for the block in one call
	receiptMap, err := s.getBlockReceiptsAll(ctx, blockNum)
	if err != nil {
		// Fallback to individual receipt fetching
		return s.processBlockWithTracingFallback(ctx, block)
	}

	var failedTxs []*FailedTransaction
	blockTimestamp := block.Time()

	// First pass: identify all failed transactions
	var failedTxHashes []common.Hash
	failedTxMap := make(map[common.Hash]*types.Transaction)

	for _, tx := range block.Transactions() {
		if tx.To() == nil {
			continue
		}

		receiptJSON, ok := receiptMap[tx.Hash()]
		if !ok {
			continue
		}

		// Only process failed transactions
		if receiptJSON.Status == "0x1" {
			continue
		}

		failedTxHashes = append(failedTxHashes, tx.Hash())
		failedTxMap[tx.Hash()] = tx
	}

	if len(failedTxHashes) == 0 {
		return nil, nil
	}

	// Batch trace all failed transactions
	traceResults := s.batchTraceLiquidationCalls(ctx, failedTxHashes)

	// Process results
	for txHash, liquidationCall := range traceResults {
		if liquidationCall == nil {
			continue
		}

		tx := failedTxMap[txHash]
		receiptJSON := receiptMap[txHash]

		failedTx, err := s.processTracedFailedTransactionJSON(ctx, tx, receiptJSON, block, liquidationCall, blockTimestamp)
		if err != nil {
			log.Printf("Warning: failed to process traced tx %s: %v", txHash.Hex(), err)
			continue
		}

		failedTxs = append(failedTxs, failedTx)
	}

	return failedTxs, nil
}

// processBlockWithTracingFallback is the old implementation for when eth_getBlockReceipts is not available
func (s *Scanner) processBlockWithTracingFallback(ctx context.Context, block *types.Block) ([]*FailedTransaction, error) {
	var failedTxs []*FailedTransaction
	blockTimestamp := block.Time()

	for _, tx := range block.Transactions() {
		if tx.To() == nil {
			continue
		}

		receipt, err := s.getReceiptWithRetry(ctx, tx.Hash())
		if err != nil {
			log.Printf("Warning: failed to get receipt for tx %s: %v", tx.Hash().Hex(), err)
			continue
		}

		if receipt.Status == types.ReceiptStatusSuccessful {
			continue
		}

		liquidationCall := s.traceLiquidationCall(ctx, tx.Hash())
		if liquidationCall == nil {
			continue
		}

		failedTx, err := s.processTracedFailedTransaction(ctx, tx, receipt, block, liquidationCall, blockTimestamp)
		if err != nil {
			log.Printf("Warning: failed to process traced tx %s: %v", tx.Hash().Hex(), err)
			continue
		}

		failedTxs = append(failedTxs, failedTx)
	}

	return failedTxs, nil
}

// batchTraceLiquidationCalls traces multiple transactions in a single batch RPC call
func (s *Scanner) batchTraceLiquidationCalls(ctx context.Context, txHashes []common.Hash) map[common.Hash]*TracedLiquidationCall {
	if len(txHashes) == 0 {
		return nil
	}

	// Build batch elements
	batch := make([]rpc.BatchElem, len(txHashes))
	traceResults := make([]TraceReplayResult, len(txHashes))

	for i, txHash := range txHashes {
		batch[i] = rpc.BatchElem{
			Method: "trace_replayTransaction",
			Args:   []interface{}{txHash.Hex(), []string{"trace"}},
			Result: &traceResults[i],
		}
	}

	err := s.rpcClient.BatchCallContext(ctx, batch)
	if err != nil {
		log.Printf("Warning: batch trace call failed: %v", err)
		return nil
	}

	results := make(map[common.Hash]*TracedLiquidationCall)

	for i, elem := range batch {
		if elem.Error != nil {
			continue
		}

		liquidationCall := s.findLiquidationCallInTraceEntries(traceResults[i].Trace)
		if liquidationCall != nil {
			results[txHashes[i]] = liquidationCall
		}
	}

	return results
}

// processTracedFailedTransactionJSON processes a failed transaction found via tracing using JSON receipt
func (s *Scanner) processTracedFailedTransactionJSON(
	ctx context.Context,
	tx *types.Transaction,
	receiptJSON *ReceiptJSON,
	block *types.Block,
	liquidationCall *TracedLiquidationCall,
	blockTimestamp uint64,
) (*FailedTransaction, error) {
	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	funcName := liquidationCall.FuncName
	if !liquidationCall.IsDirect {
		funcName += " (via internal call)"
	}

	revertReason := liquidationCall.Error
	if revertReason == "" && !s.skipRevertReason {
		revertReason = s.getRevertReason(ctx, tx, block.NumberU64())
	}

	// Parse hex gas used to decimal
	gasUsedHex := strings.TrimPrefix(receiptJSON.GasUsed, "0x")
	gasUsed, _ := new(big.Int).SetString(gasUsedHex, 16)

	return &FailedTransaction{
		BlockNumber:     block.NumberU64(),
		BlockTimestamp:  blockTimestamp,
		TransactionHash: tx.Hash().Hex(),
		FromAddress:     from.Hex(),
		ToAddress:       tx.To().Hex(),
		FunctionName:    funcName,
		FunctionArgs:    liquidationCall.ArgsJSON,
		GasUsed:         gasUsed.String(),
		RevertReason:    revertReason,
	}, nil
}

// TracedLiquidationCall holds info about a liquidation call found via tracing
type TracedLiquidationCall struct {
	To       string
	Input    string
	Error    string
	FuncName string
	ArgsJSON string
	IsDirect bool // true if top-level call, false if internal call
}

// traceLiquidationCall traces a transaction to find liquidationCall attempts
func (s *Scanner) traceLiquidationCall(ctx context.Context, txHash common.Hash) *TracedLiquidationCall {
	var traceResult TraceReplayResult
	err := s.rpcClient.CallContext(ctx, &traceResult, "trace_replayTransaction", txHash.Hex(), []string{"trace"})
	if err != nil {
		log.Printf("Warning: trace call failed for tx %s: %v", txHash.Hex(), err)
		return nil
	}

	// Search the trace entries for calls to SparkLend's liquidationCall
	return s.findLiquidationCallInTraceEntries(traceResult.Trace)
}

// findLiquidationCallInTraceEntries searches the flat trace array for liquidationCall
func (s *Scanner) findLiquidationCallInTraceEntries(entries []TraceEntry) *TracedLiquidationCall {
	sparkLendAddr := strings.ToLower(s.sparkLendContract.Hex())

	for i, entry := range entries {
		// Only look at CALL type entries
		if entry.Type != "call" {
			continue
		}

		callTo := strings.ToLower(entry.Action.To)

		// Check if this call is to SparkLend
		if callTo == sparkLendAddr {
			input := strings.TrimPrefix(entry.Action.Input, "0x")
			if len(input) >= 8 {
				selector := input[:8]
				// Check if it's liquidationCall (00a718a9)
				if selector == LiquidationCallSelector {
					// Decode the arguments
					inputBytes, _ := hex.DecodeString(input)
					argsJSON, _ := DecodeCalldata("liquidationCall", inputBytes)

					// traceAddress is empty for top-level call
					isDirect := len(entry.TraceAddress) == 0

					return &TracedLiquidationCall{
						To:       entry.Action.To,
						Input:    entry.Action.Input,
						Error:    entry.Error,
						FuncName: "liquidationCall",
						ArgsJSON: argsJSON,
						IsDirect: isDirect,
					}
				}
			}
		}
		_ = i // suppress unused variable warning
	}

	return nil
}

// processTracedFailedTransaction processes a failed transaction found via tracing
func (s *Scanner) processTracedFailedTransaction(
	ctx context.Context,
	tx *types.Transaction,
	receipt *types.Receipt,
	block *types.Block,
	liquidationCall *TracedLiquidationCall,
	blockTimestamp uint64,
) (*FailedTransaction, error) {
	// Get the sender address
	signer := types.LatestSignerForChainID(tx.ChainId())
	from, err := types.Sender(signer, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to get sender: %w", err)
	}

	funcName := liquidationCall.FuncName
	if !liquidationCall.IsDirect {
		funcName += " (via internal call)"
	}

	// Use the traced error if available, otherwise get revert reason (if not skipped)
	revertReason := liquidationCall.Error
	if revertReason == "" && !s.skipRevertReason {
		revertReason = s.getRevertReason(ctx, tx, block.NumberU64())
	}

	return &FailedTransaction{
		BlockNumber:     block.NumberU64(),
		BlockTimestamp:  blockTimestamp,
		TransactionHash: tx.Hash().Hex(),
		FromAddress:     from.Hex(),
		ToAddress:       tx.To().Hex(),
		FunctionName:    funcName,
		FunctionArgs:    liquidationCall.ArgsJSON,
		GasUsed:         fmt.Sprintf("%d", receipt.GasUsed),
		RevertReason:    revertReason,
	}, nil
}
