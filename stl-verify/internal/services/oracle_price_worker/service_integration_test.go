//go:build integration

package oracle_price_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration mock helpers (SQS + multicall)
// ---------------------------------------------------------------------------

// integrationMulticaller creates a mock multicaller that returns given prices for all blocks.
func integrationMulticaller(t *testing.T, prices []*big.Int) *testutil.MockMulticaller {
	t.Helper()
	return newOracleMulticallerWithT(t, prices)
}

// integrationMulticallerBlockDependent creates a mock multicaller where prices depend on block number.
func integrationMulticallerBlockDependent(numTokens int) *testutil.MockMulticaller {
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
			pricesData := testutil.PackAssetPrices(&testing.T{}, prices)
			return []outbound.Result{
				{Success: true, ReturnData: pricesData},
			}, nil
		},
	}
}

// blockEventMessage creates an outbound.SQSMessage containing a block event.
func blockEventMessage(blockNumber int64, version int, blockTimestamp int64, receiptHandle string) outbound.SQSMessage {
	event := blockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      fmt.Sprintf("0x%064x", blockNumber),
		BlockTimestamp: blockTimestamp,
	}
	data, _ := json.Marshal(event)
	return outbound.SQSMessage{
		MessageID:     fmt.Sprintf("msg-%d", blockNumber),
		Body:          string(data),
		ReceiptHandle: receiptHandle,
	}
}

// consumerWithMessages creates a mock consumer that delivers the provided messages
// on the first ReceiveMessages call, then blocks until context cancellation.
func consumerWithMessages(messages []outbound.SQSMessage) *mockConsumer {
	delivered := false
	return &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if !delivered {
				delivered = true
				return messages, nil
			}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
}

// consumerWithSequentialMessages creates a mock consumer that delivers messages
// in sequence, one batch per ReceiveMessages call.
func consumerWithSequentialMessages(batches [][]outbound.SQSMessage) *mockConsumer {
	callIndex := 0
	return &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if callIndex < len(batches) {
				batch := batches[callIndex]
				callIndex++
				return batch, nil
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

	// Disable migration-seeded oracle to isolate test data
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false WHERE name = 'sparklend'`); err != nil {
		t.Fatalf("disable sparklend oracle: %v", err)
	}

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-worker", "Test Worker", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000051", "WK1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000052", "WK2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))    // $2000
	price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(100000000)) // $1

	mc := integrationMulticaller(t, []*big.Int{price1, price2})

	// Create consumer that delivers one block event
	messages := []outbound.SQSMessage{
		blockEventMessage(18000000, 1, blockTimestamp, "receipt-1"),
	}
	consumer := consumerWithMessages(messages)

	cfg := Config{
		PollInterval: 1 * time.Millisecond,
		Logger:       logger,
	}

	svc, err := NewService(cfg, consumer, mc, repo)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for processing
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		var count int
		pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&count)
		return count >= 2
	}, "prices to be stored")

	// Verify prices stored
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 2 {
		t.Errorf("expected 2 price records, got %d", priceCount)
	}

	// Verify correct block number
	var storedBlock int64
	err = pool.QueryRow(ctx, `SELECT DISTINCT block_number FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&storedBlock)
	if err != nil {
		t.Fatalf("failed to query stored block: %v", err)
	}
	if storedBlock != 18000000 {
		t.Errorf("expected block 18000000, got %d", storedBlock)
	}

	// Verify price values
	var storedPriceUSD float64
	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_id = $1 AND token_id = $2`, oracleID, tokenID1).Scan(&storedPriceUSD)
	if err != nil {
		t.Fatalf("failed to query price for token1: %v", err)
	}
	if storedPriceUSD != 2000.0 {
		t.Errorf("expected price 2000.0 for token1, got %f", storedPriceUSD)
	}

	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_id = $1 AND token_id = $2`, oracleID, tokenID2).Scan(&storedPriceUSD)
	if err != nil {
		t.Fatalf("failed to query price for token2: %v", err)
	}
	if storedPriceUSD != 1.0 {
		t.Errorf("expected price 1.0 for token2, got %f", storedPriceUSD)
	}

	// Verify delete was called
	consumer.mu.Lock()
	deleteCalls := consumer.deleteMessageCalls
	consumer.mu.Unlock()
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

	// Disable migration-seeded oracle to isolate test data
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false WHERE name = 'sparklend'`); err != nil {
		t.Fatalf("disable sparklend oracle: %v", err)
	}

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-worker-cd", "Test Worker CD", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000061", "CD1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000062", "CD2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Same prices for both blocks - second block should be skipped by change detection
	price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
	price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
	mc := integrationMulticaller(t, []*big.Int{price1, price2})

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Deliver two messages with the same prices
	batches := [][]outbound.SQSMessage{
		{blockEventMessage(18000000, 1, blockTimestamp, "receipt-1")},
		{blockEventMessage(18000001, 1, blockTimestamp+12, "receipt-2")},
	}
	consumer := consumerWithSequentialMessages(batches)

	cfg := Config{
		PollInterval: 1 * time.Millisecond,
		Logger:       logger,
	}

	svc, err := NewService(cfg, consumer, mc, repo)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for both messages to be processed (at least 2 receive calls + 2 deletes)
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return consumer.deleteMessageCalls >= 2
	}, "both messages to be processed")

	// Only the first block should have prices (change detection skips second)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 2 {
		t.Errorf("expected 2 price records (first block only), got %d", priceCount)
	}

	// Verify only block 18000000 has prices
	var storedBlock int64
	err = pool.QueryRow(ctx, `SELECT DISTINCT block_number FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&storedBlock)
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

	// Disable migration-seeded oracle to isolate test data
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false WHERE name = 'sparklend'`); err != nil {
		t.Fatalf("disable sparklend oracle: %v", err)
	}

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-worker-multi", "Test Worker Multi", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000071", "MT1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000072", "MT2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Block-dependent prices: each block gets unique prices
	mc := integrationMulticallerBlockDependent(2)

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	// Deliver 3 blocks with unique prices
	batches := [][]outbound.SQSMessage{
		{blockEventMessage(18000000, 1, blockTimestamp, "receipt-1")},
		{blockEventMessage(18000001, 1, blockTimestamp+12, "receipt-2")},
		{blockEventMessage(18000002, 1, blockTimestamp+24, "receipt-3")},
	}
	consumer := consumerWithSequentialMessages(batches)

	cfg := Config{
		PollInterval: 1 * time.Millisecond,
		Logger:       logger,
	}

	svc, err := NewService(cfg, consumer, mc, repo)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for all 3 messages to be processed
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return consumer.deleteMessageCalls >= 3
	}, "all 3 messages to be processed")

	// All 3 blocks should have prices (each has unique prices)
	// 3 blocks * 2 tokens = 6 prices
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 6 {
		t.Errorf("expected 6 price records, got %d", priceCount)
	}

	// Verify all 3 blocks are represented
	var distinctBlocks int
	err = pool.QueryRow(ctx, `SELECT COUNT(DISTINCT block_number) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&distinctBlocks)
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

	// Disable migration-seeded oracle to isolate test data
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false WHERE name = 'sparklend'`); err != nil {
		t.Fatalf("disable sparklend oracle: %v", err)
	}

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-worker-ss", "Test Worker SS", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000081", "SS1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Consumer that blocks until context is cancelled (no messages)
	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	price1 := new(big.Int).Mul(big.NewInt(100), big.NewInt(1e8))
	mc := integrationMulticaller(t, []*big.Int{price1})

	cfg := Config{
		PollInterval: 1 * time.Millisecond,
		Logger:       logger,
	}

	svc, err := NewService(cfg, consumer, mc, repo)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Start should initialize successfully
	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Verify service initialized properly
	if len(svc.units) == 0 {
		t.Error("units not set after Start")
	}
	if len(svc.units) != 1 {
		t.Errorf("expected 1 unit, got %d", len(svc.units))
	}

	// Stop should not error
	err = svc.Stop()
	if err != nil {
		t.Errorf("Stop failed: %v", err)
	}

	// No prices should have been stored (no messages)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
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

	// Count enabled oracle assets from all enabled oracles
	var numTokens int
	err := pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM oracle_asset oa
		JOIN oracle o ON o.id = oa.oracle_id
		WHERE o.enabled = true AND oa.enabled = true
	`).Scan(&numTokens)
	if err != nil {
		t.Fatalf("failed to count enabled oracle assets: %v", err)
	}
	if numTokens == 0 {
		t.Fatal("no enabled oracle assets found from migration seed data")
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	mc := integrationMulticallerBlockDependent(numTokens)

	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	messages := []outbound.SQSMessage{
		blockEventMessage(20000000, 1, blockTimestamp, "receipt-seeded"),
	}
	consumer := consumerWithMessages(messages)

	cfg := Config{
		PollInterval: 1 * time.Millisecond,
		Logger:       logger,
	}

	svc, err := NewService(cfg, consumer, mc, repo)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the message to be processed
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		var count int
		pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE block_number = 20000000`).Scan(&count)
		return count >= numTokens
	}, "prices to be stored for seeded migration data")

	// Verify all tokens got prices
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE block_number = 20000000`).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != numTokens {
		t.Errorf("expected %d prices (one per enabled token), got %d", numTokens, priceCount)
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

	// Disable migration-seeded oracle to isolate test data
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false WHERE name = 'sparklend'`); err != nil {
		t.Fatalf("disable sparklend oracle: %v", err)
	}

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-worker-cache", "Test Worker Cache", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000091", "CA1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000092", "CA2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Pre-seed some prices in the database to test GetLatestPrices initialization
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	err = repo.UpsertPrices(ctx, []*entity.OnchainTokenPrice{
		{
			TokenID:      tokenID1,
			OracleID:     int16(oracleID),
			BlockNumber:  17999999,
			BlockVersion: 0,
			Timestamp:    blockTimestamp,
			PriceUSD:     2000.0,
		},
		{
			TokenID:      tokenID2,
			OracleID:     int16(oracleID),
			BlockNumber:  17999999,
			BlockVersion: 0,
			Timestamp:    blockTimestamp,
			PriceUSD:     1.0,
		},
	})
	if err != nil {
		t.Fatalf("failed to pre-seed prices: %v", err)
	}

	// Create service with constant prices matching the pre-seeded values
	// This means the new block event should NOT trigger an upsert (prices unchanged)
	price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
	price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
	mc := integrationMulticaller(t, []*big.Int{price1, price2})

	newBlockTimestamp := blockTimestamp.Add(12 * time.Second).Unix()
	messages := []outbound.SQSMessage{
		blockEventMessage(18000000, 1, newBlockTimestamp, "receipt-cache"),
	}
	consumer := consumerWithMessages(messages)

	cfg := Config{
		PollInterval: 1 * time.Millisecond,
		Logger:       logger,
	}

	svc, err := NewService(cfg, consumer, mc, repo)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Start(ctx)
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Wait for the message to be processed (delete indicates processing completed)
	testutil.WaitForCondition(t, 5*time.Second, func() bool {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		return consumer.deleteMessageCalls >= 1
	}, "message to be processed")

	// Should still have only 2 prices (the pre-seeded ones)
	// The new block had the same prices, so change detection filtered them out
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
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
