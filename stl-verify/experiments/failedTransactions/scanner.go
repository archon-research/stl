package main

import (
	"context"
	"encoding/hex"
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
	db                 *Database
	sparkLendContract  common.Address
	startBlock         uint64
	endBlock           uint64
	concurrency        int
	checkpointInterval uint64
}

// BlockResult holds the result of processing a block
type BlockResult struct {
	BlockNum  uint64
	FailedTxs []*FailedTransaction
	Err       error
}

// NewScanner creates a new Scanner instance
func NewScanner(client *ethclient.Client, db *Database, sparkLendAddress string, startBlock, endBlock uint64, concurrency int, checkpointInterval uint64) *Scanner {
	if concurrency < 1 {
		concurrency = 1
	}
	if checkpointInterval < 1 {
		checkpointInterval = 1000
	}
	return &Scanner{
		client:             client,
		db:                 db,
		sparkLendContract:  common.HexToAddress(sparkLendAddress),
		startBlock:         startBlock,
		endBlock:           endBlock,
		concurrency:        concurrency,
		checkpointInterval: checkpointInterval,
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
	log.Printf("Using %d concurrent workers", s.concurrency)

	// Use atomic counter for thread-safe counting
	var failedTxCount int64
	var processedBlocks int64
	lastCheckpoint := startBlock
	totalBlocks := endBlock - startBlock + 1

	// Create channels for work distribution
	blockChan := make(chan uint64, s.concurrency*2)
	resultChan := make(chan BlockResult, s.concurrency*2)

	// Create a WaitGroup for workers
	var wg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < s.concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for blockNum := range blockChan {
				failedTxs, err := s.processBlockConcurrent(ctx, blockNum)
				resultChan <- BlockResult{
					BlockNum:  blockNum,
					FailedTxs: failedTxs,
					Err:       err,
				}
			}
		}(i)
	}

	// Start a goroutine to close result channel when all workers are done
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Start a goroutine to feed blocks to workers
	go func() {
		defer close(blockChan)
		for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
			select {
			case <-ctx.Done():
				return
			case blockChan <- blockNum:
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

// processBlockConcurrent processes all transactions in a block and returns failed transactions
// This version is safe for concurrent use as it doesn't write to the database directly
func (s *Scanner) processBlockConcurrent(ctx context.Context, blockNum uint64) ([]*FailedTransaction, error) {
	block, err := s.getBlockWithRetry(ctx, blockNum)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	var failedTxs []*FailedTransaction
	blockTimestamp := block.Time()

	for _, tx := range block.Transactions() {
		// Skip if not to SparkLend contract
		if tx.To() == nil || *tx.To() != s.sparkLendContract {
			continue
		}

		// Check if it's one of the designated SparkLend functions
		// Ignore all transactions that are not supply, borrow, repay, withdraw, or liquidationCall
		funcName, isDesignatedFunction := GetFunctionName(tx.Data())
		if !isDesignatedFunction {
			continue
		}

		// Get transaction receipt to check status
		receipt, err := s.getReceiptWithRetry(ctx, tx.Hash())
		if err != nil {
			// Log but don't fail the entire block
			continue
		}

		// Skip successful transactions (status = 1)
		if receipt.Status == types.ReceiptStatusSuccessful {
			continue
		}

		// This is a failed transaction - process it
		failedTx, err := s.processFailedTransaction(ctx, tx, receipt, block, funcName, blockTimestamp)
		if err != nil {
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

	// Get revert reason by re-simulating the call
	revertReason := s.getRevertReason(ctx, tx, block.NumberU64())

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
