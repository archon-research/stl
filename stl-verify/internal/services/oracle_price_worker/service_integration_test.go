//go:build integration

package oracle_price_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration mock helpers (SQS + multicall)
// ---------------------------------------------------------------------------

var testOracleAddr = common.HexToAddress("0x0000000000000000000000000000000000000BBB")

// integrationMulticaller creates a mock multicaller that returns given prices for all blocks.
func integrationMulticaller(prices []*big.Int) *testutil.MockMulticaller {
	return newOracleMulticaller(testOracleAddr, prices)
}

// integrationMulticallerBlockDependent creates a mock multicaller where prices depend on block number.
func integrationMulticallerBlockDependent(numTokens int) *testutil.MockMulticaller {
	addrData, _ := packOracleAddrResult(testOracleAddr)
	return &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
			bn := blockNumber.Int64()
			prices := make([]*big.Int, numTokens)
			for i := range numTokens {
				prices[i] = new(big.Int).Mul(
					big.NewInt(bn),
					big.NewInt(int64(i+1)*100_000_000),
				)
			}
			pricesData, _ := packPricesResult(prices)

			if len(calls) == 2 {
				return []outbound.Result{
					{Success: true, ReturnData: addrData},
					{Success: true, ReturnData: pricesData},
				}, nil
			}
			return []outbound.Result{
				{Success: true, ReturnData: pricesData},
			}, nil
		},
	}
}

// blockEventMsg creates an SQS message containing a block event.
func blockEventMsg(blockNumber int64, version int, blockTimestamp int64, receiptHandle string) sqstypes.Message {
	event := blockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      fmt.Sprintf("0x%064x", blockNumber),
		BlockTimestamp: blockTimestamp,
	}
	data, _ := json.Marshal(event)
	body := string(data)
	return sqstypes.Message{
		Body:          &body,
		ReceiptHandle: &receiptHandle,
	}
}

// sqsClientWithMessages creates a mock SQS client that delivers the provided messages
// on the first ReceiveMessage call, then blocks until context cancellation.
func sqsClientWithMessages(messages []sqstypes.Message) *mockSQSClient {
	delivered := false
	return &mockSQSClient{
		receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !delivered {
				delivered = true
				return &sqs.ReceiveMessageOutput{Messages: messages}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
}

// sqsClientWithSequentialMessages creates a mock SQS client that delivers messages
// in sequence, one batch per ReceiveMessage call.
func sqsClientWithSequentialMessages(batches [][]sqstypes.Message) *mockSQSClient {
	callIndex := 0
	return &mockSQSClient{
		receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if callIndex < len(batches) {
				batch := batches[callIndex]
				callIndex++
				return &sqs.ReceiveMessageOutput{Messages: batch}, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
}

// ---------------------------------------------------------------------------
// Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_WorkerStartAndProcessBlock(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-worker", "Test Worker", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000051", "WK1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000052", "WK2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID2)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000051"),
		tokenID2: common.HexToAddress("0x0000000000000000000000000000000000000052"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))    // $2000
	price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(100000000)) // $1

	mc := integrationMulticaller([]*big.Int{price1, price2})

	// Create SQS client that delivers one block event
	messages := []sqstypes.Message{
		blockEventMsg(18000000, 1, blockTimestamp, "receipt-1"),
	}
	sqsClient := sqsClientWithMessages(messages)

	cfg := Config{
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		OracleSource:    "test-worker",
		PollInterval:    1 * time.Millisecond,
		WaitTimeSeconds: 0,
		Logger:          logger,
	}

	svc, err := NewService(cfg, sqsClient, mc, repo, tokenAddresses)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for processing
	deadline := time.After(5 * time.Second)
	for {
		var count int
		pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&count)
		if count >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for prices to be stored")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify prices stored
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 2 {
		t.Errorf("expected 2 price records, got %d", priceCount)
	}

	// Verify correct block number
	var storedBlock int64
	err = pool.QueryRow(ctx, `SELECT DISTINCT block_number FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&storedBlock)
	if err != nil {
		t.Fatalf("failed to query stored block: %v", err)
	}
	if storedBlock != 18000000 {
		t.Errorf("expected block 18000000, got %d", storedBlock)
	}

	// Verify price values
	var storedPriceUSD float64
	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_source_id = $1 AND token_id = $2`, sourceID, tokenID1).Scan(&storedPriceUSD)
	if err != nil {
		t.Fatalf("failed to query price for token1: %v", err)
	}
	if storedPriceUSD != 2000.0 {
		t.Errorf("expected price 2000.0 for token1, got %f", storedPriceUSD)
	}

	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_source_id = $1 AND token_id = $2`, sourceID, tokenID2).Scan(&storedPriceUSD)
	if err != nil {
		t.Fatalf("failed to query price for token2: %v", err)
	}
	if storedPriceUSD != 1.0 {
		t.Errorf("expected price 1.0 for token2, got %f", storedPriceUSD)
	}

	// Verify delete was called
	sqsClient.mu.Lock()
	deleteCalls := sqsClient.deleteMessageCalls
	sqsClient.mu.Unlock()
	if deleteCalls < 1 {
		t.Errorf("expected at least 1 DeleteMessage call, got %d", deleteCalls)
	}

	if err := svc.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestIntegration_WorkerChangeDetection(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-worker-cd", "Test Worker CD", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000061", "CD1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000062", "CD2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID2)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000061"),
		tokenID2: common.HexToAddress("0x0000000000000000000000000000000000000062"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Same prices for both blocks - second block should be skipped by change detection
	price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
	price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
	mc := integrationMulticaller([]*big.Int{price1, price2})

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Deliver two messages with the same prices
	batches := [][]sqstypes.Message{
		{blockEventMsg(18000000, 1, blockTimestamp, "receipt-1")},
		{blockEventMsg(18000001, 1, blockTimestamp+12, "receipt-2")},
	}
	sqsClient := sqsClientWithSequentialMessages(batches)

	cfg := Config{
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		OracleSource:    "test-worker-cd",
		PollInterval:    1 * time.Millisecond,
		WaitTimeSeconds: 0,
		Logger:          logger,
	}

	svc, err := NewService(cfg, sqsClient, mc, repo, tokenAddresses)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for both messages to be processed (at least 2 receive calls + 2 deletes)
	deadline := time.After(5 * time.Second)
	for {
		sqsClient.mu.Lock()
		deletes := sqsClient.deleteMessageCalls
		sqsClient.mu.Unlock()
		if deletes >= 2 {
			break
		}
		select {
		case <-deadline:
			sqsClient.mu.Lock()
			t.Fatalf("timed out waiting for message processing (deletes=%d)", sqsClient.deleteMessageCalls)
			sqsClient.mu.Unlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Only the first block should have prices (change detection skips second)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 2 {
		t.Errorf("expected 2 price records (first block only), got %d", priceCount)
	}

	// Verify only block 18000000 has prices
	var storedBlock int64
	err = pool.QueryRow(ctx, `SELECT DISTINCT block_number FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&storedBlock)
	if err != nil {
		t.Fatalf("failed to query stored block: %v", err)
	}
	if storedBlock != 18000000 {
		t.Errorf("expected prices at block 18000000 only, got %d", storedBlock)
	}

	if err := svc.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestIntegration_WorkerMultipleBlocksWithPriceChanges(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-worker-multi", "Test Worker Multi", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000071", "MT1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000072", "MT2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID2)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000071"),
		tokenID2: common.HexToAddress("0x0000000000000000000000000000000000000072"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Block-dependent prices: each block gets unique prices
	mc := integrationMulticallerBlockDependent(2)

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Deliver 3 blocks with unique prices
	batches := [][]sqstypes.Message{
		{blockEventMsg(18000000, 1, blockTimestamp, "receipt-1")},
		{blockEventMsg(18000001, 1, blockTimestamp+12, "receipt-2")},
		{blockEventMsg(18000002, 1, blockTimestamp+24, "receipt-3")},
	}
	sqsClient := sqsClientWithSequentialMessages(batches)

	cfg := Config{
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		OracleSource:    "test-worker-multi",
		PollInterval:    1 * time.Millisecond,
		WaitTimeSeconds: 0,
		Logger:          logger,
	}

	svc, err := NewService(cfg, sqsClient, mc, repo, tokenAddresses)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for all 3 messages to be processed
	deadline := time.After(5 * time.Second)
	for {
		sqsClient.mu.Lock()
		deletes := sqsClient.deleteMessageCalls
		sqsClient.mu.Unlock()
		if deletes >= 3 {
			break
		}
		select {
		case <-deadline:
			sqsClient.mu.Lock()
			t.Fatalf("timed out waiting for all messages (deletes=%d)", sqsClient.deleteMessageCalls)
			sqsClient.mu.Unlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// All 3 blocks should have prices (each has unique prices)
	// 3 blocks * 2 tokens = 6 prices
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 6 {
		t.Errorf("expected 6 price records, got %d", priceCount)
	}

	// Verify all 3 blocks are represented
	var distinctBlocks int
	err = pool.QueryRow(ctx, `SELECT COUNT(DISTINCT block_number) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&distinctBlocks)
	if err != nil {
		t.Fatalf("failed to query distinct blocks: %v", err)
	}
	if distinctBlocks != 3 {
		t.Errorf("expected 3 distinct blocks, got %d", distinctBlocks)
	}

	if err := svc.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestIntegration_WorkerStartStop(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-worker-ss", "Test Worker SS", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000081", "SS1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000081"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// SQS client that blocks until context is cancelled (no messages)
	sqsClient := &mockSQSClient{
		receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	price1 := new(big.Int).Mul(big.NewInt(100), big.NewInt(1e8))
	mc := integrationMulticaller([]*big.Int{price1})

	cfg := Config{
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		OracleSource:    "test-worker-ss",
		PollInterval:    1 * time.Millisecond,
		WaitTimeSeconds: 0,
		Logger:          logger,
	}

	svc, err := NewService(cfg, sqsClient, mc, repo, tokenAddresses)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Start should initialize successfully
	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify service initialized properly
	if svc.oracleSource == nil {
		t.Error("oracleSource not set after Start")
	}
	if len(svc.assets) != 1 {
		t.Errorf("expected 1 asset, got %d", len(svc.assets))
	}

	// Stop should not error
	err = svc.Stop()
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}

	// No prices should have been stored (no messages)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 0 {
		t.Errorf("expected 0 price records, got %d", priceCount)
	}
}

func TestIntegration_WorkerWithSeededMigrationData(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	// Use the sparklend source seeded by migration
	var oracleSourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM oracle_source WHERE name = 'sparklend'`).Scan(&oracleSourceID)
	if err != nil {
		t.Fatalf("failed to get sparklend oracle source: %v", err)
	}

	// Build token address map from migration-seeded data
	tokenAddresses := make(map[int64]common.Address)
	rows, err := pool.Query(ctx, `
		SELECT oa.token_id, t.address
		FROM oracle_asset oa
		JOIN token t ON t.id = oa.token_id
		WHERE oa.oracle_source_id = $1 AND oa.enabled = true
		ORDER BY oa.id
	`, oracleSourceID)
	if err != nil {
		t.Fatalf("failed to query token addresses: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var tokenID int64
		var addressBytes []byte
		if err := rows.Scan(&tokenID, &addressBytes); err != nil {
			t.Fatalf("failed to scan token address: %v", err)
		}
		tokenAddresses[tokenID] = common.BytesToAddress(addressBytes)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("failed to iterate token addresses: %v", err)
	}

	numTokens := len(tokenAddresses)
	if numTokens == 0 {
		t.Fatal("no token addresses found from migration seed data")
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	mc := integrationMulticallerBlockDependent(numTokens)

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	messages := []sqstypes.Message{
		blockEventMsg(20000000, 1, blockTimestamp, "receipt-seeded"),
	}
	sqsClient := sqsClientWithMessages(messages)

	cfg := Config{
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		OracleSource:    "sparklend",
		PollInterval:    1 * time.Millisecond,
		WaitTimeSeconds: 0,
		Logger:          logger,
	}

	svc, err := NewService(cfg, sqsClient, mc, repo, tokenAddresses)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the message to be processed
	deadline := time.After(5 * time.Second)
	for {
		var count int
		pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, oracleSourceID).Scan(&count)
		if count >= numTokens {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for prices to be stored (expected %d)", numTokens)
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Verify all tokens got prices
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1 AND block_number = 20000000`, oracleSourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != numTokens {
		t.Errorf("expected %d prices (one per seeded token), got %d", numTokens, priceCount)
	}

	if err := svc.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}

func TestIntegration_WorkerGetLatestPricesInitialization(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-worker-cache", "Test Worker Cache", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000091", "CA1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000092", "CA2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID2)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000091"),
		tokenID2: common.HexToAddress("0x0000000000000000000000000000000000000092"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Pre-seed some prices in the database to test GetLatestPrices initialization
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	oracleAddrBytes := testOracleAddr.Bytes()
	err = repo.UpsertPrices(ctx, []*entity.OnchainTokenPrice{
		{
			TokenID:        tokenID1,
			OracleSourceID: int16(sourceID),
			BlockNumber:    17999999,
			BlockVersion:   0,
			Timestamp:      blockTimestamp,
			OracleAddress:  oracleAddrBytes,
			PriceUSD:       2000.0,
		},
		{
			TokenID:        tokenID2,
			OracleSourceID: int16(sourceID),
			BlockNumber:    17999999,
			BlockVersion:   0,
			Timestamp:      blockTimestamp,
			OracleAddress:  oracleAddrBytes,
			PriceUSD:       1.0,
		},
	})
	if err != nil {
		t.Fatalf("failed to pre-seed prices: %v", err)
	}

	// Create service with constant prices matching the pre-seeded values
	// This means the new block event should NOT trigger an upsert (prices unchanged)
	price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
	price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
	mc := integrationMulticaller([]*big.Int{price1, price2})

	newBlockTimestamp := blockTimestamp.Add(12 * time.Second).Unix()
	messages := []sqstypes.Message{
		blockEventMsg(18000000, 1, newBlockTimestamp, "receipt-cache"),
	}
	sqsClient := sqsClientWithMessages(messages)

	cfg := Config{
		QueueURL:        "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		OracleSource:    "test-worker-cache",
		PollInterval:    1 * time.Millisecond,
		WaitTimeSeconds: 0,
		Logger:          logger,
	}

	svc, err := NewService(cfg, sqsClient, mc, repo, tokenAddresses)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the message to be processed (delete indicates processing completed)
	deadline := time.After(5 * time.Second)
	for {
		sqsClient.mu.Lock()
		deletes := sqsClient.deleteMessageCalls
		sqsClient.mu.Unlock()
		if deletes >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for message to be processed")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	// Should still have only 2 prices (the pre-seeded ones)
	// The new block had the same prices, so change detection filtered them out
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 2 {
		t.Errorf("expected 2 price records (pre-seeded only, same prices skipped), got %d", priceCount)
	}

	if err := svc.Stop(); err != nil {
		t.Errorf("Stop failed: %v", err)
	}
}
