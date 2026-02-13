//go:build integration

package oracle_backfill

import (
	"context"
	"math/big"
	"testing"
	"time"

	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration test mock helpers (blockchain layer)
// ---------------------------------------------------------------------------

// integrationMockHeaderFetcher returns headers with timestamps derived from block number.
func integrationMockHeaderFetcher() *mockHeaderFetcher {
	return &mockHeaderFetcher{
		headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
			return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
		},
	}
}

// integrationMockMulticallFactory creates a MulticallFactory returning mock multicall
// clients that return block-dependent prices for the given number of tokens.
// Each token gets a distinct price: tokenIndex * blockNumber * 100_000_000 (1e8 = $1 in oracle format).
// Uses individual getAssetPrice results (one per token) matching FetchOraclePricesIndividual.
func integrationMockMulticallFactory(t *testing.T, numTokens int) MulticallFactory {
	t.Helper()
	return func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				bn := blockNumber.Int64()
				results := make([]outbound.Result, numTokens)
				for i := range numTokens {
					// Each token gets a unique price per block:
					// token0: bn * 100_000_000 (= $bn)
					// token1: bn * 200_000_000 (= $2*bn)
					price := new(big.Int).Mul(
						big.NewInt(bn),
						big.NewInt(int64(i+1)*100_000_000),
					)
					results[i] = outbound.Result{
						Success:    true,
						ReturnData: testutil.PackAssetPrice(t, price),
					}
				}
				return results, nil
			},
		}, nil
	}
}

// integrationMockMulticallFactoryConstant creates a MulticallFactory that always
// returns the same prices regardless of block number.
// Uses individual getAssetPrice results (one per token) matching FetchOraclePricesIndividual.
func integrationMockMulticallFactoryConstant(t *testing.T, prices []*big.Int) MulticallFactory {
	t.Helper()
	return func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: defaultMulticallExecute(t, prices, nil),
		}, nil
	}
}

// ---------------------------------------------------------------------------
// Integration Tests
// ---------------------------------------------------------------------------

func TestIntegration_BackfillRun_HappyPath(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	// Disable feed-based oracles — the mock multicaller only handles aave_oracle format.
	if _, err := pool.Exec(ctx, `UPDATE oracle SET enabled = false WHERE oracle_type IN ('chainlink_feed', 'chronicle')`); err != nil {
		t.Fatalf("disable feed oracles: %v", err)
	}

	// Set deployment_block and protocol binding below the test range so clamping doesn't skip blocks
	if _, err := pool.Exec(ctx, `UPDATE oracle SET deployment_block = 0 WHERE name = 'sparklend'`); err != nil {
		t.Fatalf("update deployment_block: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE protocol_oracle SET from_block = 0`); err != nil {
		t.Fatalf("update protocol_oracle from_block: %v", err)
	}

	// Get the oracle ID seeded by migration
	var oracleID int64
	err := pool.QueryRow(ctx, `SELECT id FROM oracle WHERE name = 'sparklend'`).Scan(&oracleID)
	if err != nil {
		t.Fatalf("failed to get sparklend oracle: %v", err)
	}

	// Count enabled assets for the seeded oracle
	var enabledAssetCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM oracle_asset WHERE oracle_id = $1 AND enabled = true`, oracleID).Scan(&enabledAssetCount)
	if err != nil {
		t.Fatalf("failed to count enabled assets: %v", err)
	}
	if enabledAssetCount == 0 {
		t.Fatal("expected at least some enabled oracle assets from seed data")
	}

	// Create real repository
	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create onchain price repository: %v", err)
	}

	// Create service — no tokenAddresses needed, service loads from DB
	svc, err := NewService(
		Config{
			Concurrency: 2,
			BatchSize:   50,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, enabledAssetCount),
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Run backfill for 6 blocks
	fromBlock := int64(100)
	toBlock := int64(105)
	err = svc.Run(ctx, fromBlock, toBlock)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify prices were stored in the database
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}

	// With block-dependent prices (every block is different), every block stores prices
	// for every token. 6 blocks x numTokens = expected total.
	expectedPrices := 6 * enabledAssetCount
	if priceCount != expectedPrices {
		t.Errorf("expected %d price records, got %d", expectedPrices, priceCount)
	}

	// Verify block numbers stored
	var minBlock, maxBlock int64
	err = pool.QueryRow(ctx, `SELECT MIN(block_number), MAX(block_number) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&minBlock, &maxBlock)
	if err != nil {
		t.Fatalf("failed to query block range: %v", err)
	}
	if minBlock != fromBlock {
		t.Errorf("expected min block %d, got %d", fromBlock, minBlock)
	}
	if maxBlock != toBlock {
		t.Errorf("expected max block %d, got %d", toBlock, maxBlock)
	}

	// Verify a sample price is positive
	var samplePrice float64
	err = pool.QueryRow(ctx, `
		SELECT price_usd FROM onchain_token_price
		WHERE oracle_id = $1 AND block_number = $2
		ORDER BY token_id LIMIT 1
	`, oracleID, fromBlock).Scan(&samplePrice)
	if err != nil {
		t.Fatalf("failed to query sample price: %v", err)
	}
	if samplePrice <= 0 {
		t.Errorf("expected positive price, got %f", samplePrice)
	}
}

func TestIntegration_BackfillRun_ChangeDetection(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	// Create our own oracle to avoid interference from migration seed data.
	oracleID := testutil.SeedOracle(t, ctx, pool, "test-oracle", "Test Oracle", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000001", "TK1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000002", "TK2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)

	// Use constant prices: same price for every block.
	// Only the first block should be stored (change detection filters the rest).
	constantPrices := []*big.Int{big.NewInt(100_000_000), big.NewInt(250_000_000_000)}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	svc, err := NewService(
		Config{
			Concurrency: 1,
			BatchSize:   100,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactoryConstant(t, constantPrices),
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Run backfill for 5 blocks
	err = svc.Run(ctx, 100, 104)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// With constant prices, only the first block should have stored prices (2 tokens = 2 records)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 2 {
		t.Errorf("expected 2 price records (one per token, first block only), got %d", priceCount)
	}

	// Verify only block 100 has prices
	var storedBlock int64
	err = pool.QueryRow(ctx, `SELECT DISTINCT block_number FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&storedBlock)
	if err != nil {
		t.Fatalf("failed to query stored block: %v", err)
	}
	if storedBlock != 100 {
		t.Errorf("expected prices stored at block 100, got %d", storedBlock)
	}

	// Verify actual price values
	var priceUSD1, priceUSD2 float64
	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_id = $1 AND token_id = $2`, oracleID, tokenID1).Scan(&priceUSD1)
	if err != nil {
		t.Fatalf("failed to query TK1 price: %v", err)
	}
	// 100_000_000 / 1e8 = 1.0
	if priceUSD1 != 1.0 {
		t.Errorf("expected TK1 price 1.0, got %f", priceUSD1)
	}

	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_id = $1 AND token_id = $2`, oracleID, tokenID2).Scan(&priceUSD2)
	if err != nil {
		t.Fatalf("failed to query TK2 price: %v", err)
	}
	// 250_000_000_000 / 1e8 = 2500.0
	if priceUSD2 != 2500.0 {
		t.Errorf("expected TK2 price 2500.0, got %f", priceUSD2)
	}
}

func TestIntegration_BackfillRun_UpsertIdempotency(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-idempotent", "Test Idempotent", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000021", "IDP1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// First run: insert prices for blocks 100-102
	svc, err := NewService(
		Config{
			Concurrency: 1,
			BatchSize:   100,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 1),
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Run(ctx, 100, 102)
	if err != nil {
		t.Fatalf("Run (first) failed: %v", err)
	}

	var countFirst int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&countFirst)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}

	// Manually re-run UpsertPrices with the same data to verify ON CONFLICT DO NOTHING.
	// The resume logic would skip this, so we test idempotency at the repo level.
	sampleTimestamp := time.Unix(1700000100, 0).UTC()
	repo.UpsertPrices(ctx, []*entity.OnchainTokenPrice{
		{
			TokenID:      tokenID1,
			OracleID:     int16(oracleID),
			BlockNumber:  100,
			BlockVersion: 0,
			Timestamp:    sampleTimestamp,
			PriceUSD:     999.99, // Different price, but same PK
		},
	})

	var countSecond int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&countSecond)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}

	// ON CONFLICT DO NOTHING means count should stay the same
	if countFirst != countSecond {
		t.Errorf("expected idempotent upsert: first=%d, second=%d", countFirst, countSecond)
	}
}

func TestIntegration_BackfillRun_GetLatestBlock(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-latest-block", "Test Latest Block", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000031", "LB1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Before any data: GetLatestBlock should return 0
	latestBlock, err := repo.GetLatestBlock(ctx, oracleID)
	if err != nil {
		t.Fatalf("GetLatestBlock failed: %v", err)
	}
	if latestBlock != 0 {
		t.Errorf("expected latest block 0 before any data, got %d", latestBlock)
	}

	// Run backfill
	svc, err := NewService(
		Config{
			Concurrency: 1,
			BatchSize:   100,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 1),
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Run(ctx, 200, 205)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// After backfill: GetLatestBlock should return 205
	latestBlock, err = repo.GetLatestBlock(ctx, oracleID)
	if err != nil {
		t.Fatalf("GetLatestBlock after backfill failed: %v", err)
	}
	if latestBlock != 205 {
		t.Errorf("expected latest block 205, got %d", latestBlock)
	}
}

func TestIntegration_BackfillRun_RespectsDeploymentBlock(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	// Create oracle with deployment block at 200
	oracleID := testutil.SeedOracleWithDeploymentBlock(t, ctx, pool, "test-deploy", "Test Deploy", 1, "0x0000000000000000000000000000000000000AAA", 200)
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000051", "DEP1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	svc, err := NewService(
		Config{
			Concurrency: 1,
			BatchSize:   100,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 1),
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Request blocks 100-205, but oracle was deployed at block 200
	err = svc.Run(ctx, 100, 205)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Only blocks 200-205 should have prices (6 blocks x 1 token = 6 records)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 6 {
		t.Errorf("expected 6 price records (blocks 200-205), got %d", priceCount)
	}

	// Verify min block is the deployment block
	var minBlock int64
	err = pool.QueryRow(ctx, `SELECT MIN(block_number) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&minBlock)
	if err != nil {
		t.Fatalf("failed to query min block: %v", err)
	}
	if minBlock != 200 {
		t.Errorf("expected min block 200 (deployment block), got %d", minBlock)
	}
}

func TestIntegration_BackfillRun_RespectsSupersession(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	// Create two oracles: oracle1 deployed at block 100, oracle2 deployed at block 150
	oracle1ID := testutil.SeedOracleWithDeploymentBlock(t, ctx, pool, "test-old-oracle", "Test Old Oracle", 1, "0x0000000000000000000000000000000000000AAA", 100)
	oracle2ID := testutil.SeedOracleWithDeploymentBlock(t, ctx, pool, "test-new-oracle", "Test New Oracle", 1, "0x0000000000000000000000000000000000000BBB", 150)

	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000061", "SUP1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracle1ID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracle2ID, tokenID1)

	// Create protocol and bind oracle1 from block 100, then oracle2 from block 160
	protocolID := testutil.SeedProtocol(t, ctx, pool, 1, "0x0000000000000000000000000000000000000FFF", "test-protocol", "lending", 100, "")
	testutil.SeedProtocolOracle(t, ctx, pool, protocolID, oracle1ID, 100)
	testutil.SeedProtocolOracle(t, ctx, pool, protocolID, oracle2ID, 160)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	svc, err := NewService(
		Config{
			Concurrency: 1,
			BatchSize:   100,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 1),
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	// Request blocks 80-180
	err = svc.Run(ctx, 80, 180)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Oracle1: validFrom = max(deploymentBlock=100, bindingFrom=100) = 100
	//          validTo = 159 (superseded at block 160)
	//          Clamped range: 100-159 = 60 blocks
	var oracle1Count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracle1ID).Scan(&oracle1Count)
	if err != nil {
		t.Fatalf("failed to query oracle1 count: %v", err)
	}
	if oracle1Count != 60 {
		t.Errorf("expected 60 price records for oracle1 (blocks 100-159), got %d", oracle1Count)
	}

	// Oracle2: validFrom = max(deploymentBlock=150, bindingFrom=160) = 160
	//          validTo = 0 (still active)
	//          Clamped range: 160-180 = 21 blocks
	var oracle2Count int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracle2ID).Scan(&oracle2Count)
	if err != nil {
		t.Fatalf("failed to query oracle2 count: %v", err)
	}
	if oracle2Count != 21 {
		t.Errorf("expected 21 price records for oracle2 (blocks 160-180), got %d", oracle2Count)
	}

	// Verify oracle1 has no prices beyond block 159
	var oracle1MaxBlock int64
	err = pool.QueryRow(ctx, `SELECT MAX(block_number) FROM onchain_token_price WHERE oracle_id = $1`, oracle1ID).Scan(&oracle1MaxBlock)
	if err != nil {
		t.Fatalf("failed to query oracle1 max block: %v", err)
	}
	if oracle1MaxBlock > 159 {
		t.Errorf("expected oracle1 max block <= 159, got %d", oracle1MaxBlock)
	}
}

func TestIntegration_BackfillRun_PartialTokenFailure(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-partial", "Test Partial", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000071", "PF1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000072", "PF2", 18)
	tokenID3 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000073", "PF3", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID3)

	// token3 (index 2) fails at blocks 100-102 (early blocks), succeeds at 103-104.
	// This simulates a token that didn't have a price source until a later block.
	mcFactory := func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				bn := blockNumber.Int64()
				results := make([]outbound.Result, 3)
				// token1 and token2 always succeed with block-dependent prices
				results[0] = outbound.Result{
					Success:    true,
					ReturnData: testutil.PackAssetPrice(t, new(big.Int).Mul(big.NewInt(bn), big.NewInt(100_000_000))),
				}
				results[1] = outbound.Result{
					Success:    true,
					ReturnData: testutil.PackAssetPrice(t, new(big.Int).Mul(big.NewInt(bn), big.NewInt(200_000_000))),
				}
				// token3 fails at blocks 100-102
				if bn <= 102 {
					results[2] = outbound.Result{Success: false, ReturnData: nil}
				} else {
					results[2] = outbound.Result{
						Success:    true,
						ReturnData: testutil.PackAssetPrice(t, new(big.Int).Mul(big.NewInt(bn), big.NewInt(300_000_000))),
					}
				}
				return results, nil
			},
		}, nil
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	svc, err := NewService(
		Config{
			Concurrency: 1,
			BatchSize:   100,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		mcFactory,
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Run(ctx, 100, 104)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Blocks 100-102: 2 tokens each (token3 fails) = 6
	// Blocks 103-104: 3 tokens each = 6
	// Total = 12 prices (all unique due to block-dependent prices)
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 12 {
		t.Errorf("expected 12 price records, got %d", priceCount)
	}

	// Verify token3 has no prices at blocks 100-102
	var token3EarlyCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1 AND token_id = $2 AND block_number <= 102`, oracleID, tokenID3).Scan(&token3EarlyCount)
	if err != nil {
		t.Fatalf("failed to query token3 early count: %v", err)
	}
	if token3EarlyCount != 0 {
		t.Errorf("expected 0 prices for token3 at blocks 100-102, got %d", token3EarlyCount)
	}

	// Verify token3 has prices at blocks 103-104
	var token3LateCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1 AND token_id = $2 AND block_number >= 103`, oracleID, tokenID3).Scan(&token3LateCount)
	if err != nil {
		t.Fatalf("failed to query token3 late count: %v", err)
	}
	if token3LateCount != 2 {
		t.Errorf("expected 2 prices for token3 at blocks 103-104, got %d", token3LateCount)
	}
}

// TestIntegration_BackfillRun_DuplicateBlocksSafeWithOnConflict verifies that
// re-running the backfill for the same block range doesn't produce duplicate rows
// or errors. This exercises the ON CONFLICT DO NOTHING clause in UpsertPrices
// through the full service path.
func TestIntegration_BackfillRun_DuplicateBlocksSafeWithOnConflict(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-dup-safe", "Test Dup Safe", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000091", "DUP1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000092", "DUP2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	newService := func() *Service {
		svc, err := NewService(
			Config{Concurrency: 1, BatchSize: 100, Logger: logger},
			integrationMockHeaderFetcher(),
			integrationMockMulticallFactory(t, 2),
			repo,
			nil,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		return svc
	}

	// First run: blocks 100-104
	if err := newService().Run(ctx, 100, 104); err != nil {
		t.Fatalf("Run (first): %v", err)
	}

	var countFirst int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&countFirst)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}
	// 5 blocks × 2 tokens = 10
	if countFirst != 10 {
		t.Fatalf("expected 10 after first run, got %d", countFirst)
	}

	// Second run: same range. All blocks re-processed.
	// ON CONFLICT DO NOTHING prevents duplicate rows.
	if err := newService().Run(ctx, 100, 104); err != nil {
		t.Fatalf("Run (second): %v", err)
	}

	var countSecond int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&countSecond)
	if err != nil {
		t.Fatalf("failed to query count: %v", err)
	}

	// ON CONFLICT DO NOTHING: count must be unchanged
	if countSecond != countFirst {
		t.Errorf("expected same count after idempotent re-run: first=%d, second=%d", countFirst, countSecond)
	}
}

func TestIntegration_BackfillRun_MultipleSelectiveChanges(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)

	oracleID := testutil.SeedOracle(t, ctx, pool, "test-selective", "Test Selective", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000041", "SC1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000042", "SC2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID2)

	// Block 100: token1=$100, token2=$2500 (new -> stored)
	// Block 101: token1=$100, token2=$2500 (same -> NOT stored)
	// Block 102: token1=$200, token2=$2500 (token1 changed -> stored)
	// Block 103: token1=$200, token2=$2500 (same -> NOT stored)
	// Block 104: token1=$200, token2=$3000 (token2 changed -> stored)
	pricesByBlock := map[int64][]*big.Int{
		100: {big.NewInt(100_00000000), big.NewInt(2500_00000000)},
		101: {big.NewInt(100_00000000), big.NewInt(2500_00000000)},
		102: {big.NewInt(200_00000000), big.NewInt(2500_00000000)},
		103: {big.NewInt(200_00000000), big.NewInt(2500_00000000)},
		104: {big.NewInt(200_00000000), big.NewInt(3000_00000000)},
	}

	mcFactory := func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				prices := pricesByBlock[blockNumber.Int64()]
				return multicallResult(t, prices), nil
			},
		}, nil
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	svc, err := NewService(
		Config{
			Concurrency: 1,
			BatchSize:   100,
			Logger:      logger,
		},
		integrationMockHeaderFetcher(),
		mcFactory,
		repo,
		nil,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Run(ctx, 100, 104)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Expected stored prices:
	// Block 100: token1 + token2 = 2
	// Block 102: token1 = 1 (only token1 changed)
	// Block 104: token2 = 1 (only token2 changed)
	// Total: 4
	var priceCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1`, oracleID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 4 {
		t.Errorf("expected 4 price records (change detection), got %d", priceCount)
	}

	// Verify specific blocks have entries
	for _, blockNum := range []int64{100, 102, 104} {
		var count int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1 AND block_number = $2`, oracleID, blockNum).Scan(&count)
		if err != nil {
			t.Fatalf("failed to query block %d: %v", blockNum, err)
		}
		if count == 0 {
			t.Errorf("expected at least one price at block %d, got 0", blockNum)
		}
	}

	// Verify blocks 101 and 103 have no entries
	for _, blockNum := range []int64{101, 103} {
		var count int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_id = $1 AND block_number = $2`, oracleID, blockNum).Scan(&count)
		if err != nil {
			t.Fatalf("failed to query block %d: %v", blockNum, err)
		}
		if count != 0 {
			t.Errorf("expected 0 prices at block %d (no change), got %d", blockNum, count)
		}
	}
}
