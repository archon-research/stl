//go:build integration

package oracle_backfill

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
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
func integrationMockMulticallFactory(t *testing.T, numTokens int) MulticallFactory {
	t.Helper()
	return func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				bn := blockNumber.Int64()
				prices := make([]*big.Int, numTokens)
				for i := range numTokens {
					// Each token gets a unique price per block:
					// token0: bn * 100_000_000 (= $bn)
					// token1: bn * 200_000_000 (= $2*bn)
					prices[i] = new(big.Int).Mul(
						big.NewInt(bn),
						big.NewInt(int64(i+1)*100_000_000),
					)
				}
				return multicallResult(t, calls, prices), nil
			},
		}, nil
	}
}

// integrationMockMulticallFactoryConstant creates a MulticallFactory that always
// returns the same prices regardless of block number.
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

	// Get the oracle source ID seeded by migration
	var oracleSourceID int64
	err := pool.QueryRow(ctx, `SELECT id FROM oracle_source WHERE name = 'sparklend'`).Scan(&oracleSourceID)
	if err != nil {
		t.Fatalf("failed to get sparklend oracle source: %v", err)
	}

	// Get two token IDs from the migration-seeded tokens (WETH and DAI)
	var wethTokenID, daiTokenID int64
	err = pool.QueryRow(ctx, `SELECT id FROM token WHERE symbol = 'WETH' AND chain_id = 1`).Scan(&wethTokenID)
	if err != nil {
		t.Fatalf("failed to get WETH token: %v", err)
	}
	err = pool.QueryRow(ctx, `SELECT id FROM token WHERE symbol = 'DAI' AND chain_id = 1`).Scan(&daiTokenID)
	if err != nil {
		t.Fatalf("failed to get DAI token: %v", err)
	}

	// The migration already seeds oracle_asset rows linking sparklend to tokens.
	// We need to figure out which assets are enabled and build the token address map.
	// For simplicity, let's use the existing seeded data and query the token addresses.
	var enabledAssetCount int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM oracle_asset WHERE oracle_source_id = $1 AND enabled = true`, oracleSourceID).Scan(&enabledAssetCount)
	if err != nil {
		t.Fatalf("failed to count enabled assets: %v", err)
	}
	if enabledAssetCount == 0 {
		t.Fatal("expected at least some enabled oracle assets from seed data")
	}

	// Build tokenAddresses map from the database
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
		t.Fatal("no token addresses found")
	}

	// Create real repository
	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create onchain price repository: %v", err)
	}

	// Create service with mock blockchain interfaces
	svc, err := NewService(
		Config{
			Concurrency:  2,
			BatchSize:    50,
			OracleSource: "sparklend",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, numTokens),
		repo,
		tokenAddresses,
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, oracleSourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}

	// With block-dependent prices (every block is different), every block stores prices
	// for every token. 6 blocks x numTokens = expected total.
	expectedPrices := 6 * numTokens
	if priceCount != expectedPrices {
		t.Errorf("expected %d price records, got %d", expectedPrices, priceCount)
	}

	// Verify block numbers stored
	var minBlock, maxBlock int64
	err = pool.QueryRow(ctx, `SELECT MIN(block_number), MAX(block_number) FROM onchain_token_price WHERE oracle_source_id = $1`, oracleSourceID).Scan(&minBlock, &maxBlock)
	if err != nil {
		t.Fatalf("failed to query block range: %v", err)
	}
	if minBlock != fromBlock {
		t.Errorf("expected min block %d, got %d", fromBlock, minBlock)
	}
	if maxBlock != toBlock {
		t.Errorf("expected max block %d, got %d", toBlock, maxBlock)
	}

	// Verify a specific price value for WETH at block 100
	// WETH is the first token in ordering, price = blockNum * 1 * 1e8 -> ConvertOraclePriceToUSD -> blockNum * 1.0
	// Actually the order depends on oracle_asset ordering by ID, which is seeded by migration.
	// We can verify any token - let's just check that prices are > 0
	var samplePrice float64
	err = pool.QueryRow(ctx, `
		SELECT price_usd FROM onchain_token_price
		WHERE oracle_source_id = $1 AND block_number = $2
		ORDER BY token_id LIMIT 1
	`, oracleSourceID, fromBlock).Scan(&samplePrice)
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

	// Use the sparklend source and only two custom tokens (to keep things deterministic)
	// We create our own oracle source to avoid interference from migration seed data.
	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-oracle", "Test Oracle", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000001", "TK1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000002", "TK2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID2)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000001"),
		tokenID2: common.HexToAddress("0x0000000000000000000000000000000000000002"),
	}

	// Use constant prices: same price for every block.
	// Only the first block should be stored (change detection filters the rest).
	constantPrices := []*big.Int{big.NewInt(100_000_000), big.NewInt(250_000_000_000)}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	svc, err := NewService(
		Config{
			Concurrency:  1,
			BatchSize:    100,
			OracleSource: "test-oracle",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactoryConstant(t, constantPrices),
		repo,
		tokenAddresses,
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 2 {
		t.Errorf("expected 2 price records (one per token, first block only), got %d", priceCount)
	}

	// Verify only block 100 has prices
	var storedBlock int64
	err = pool.QueryRow(ctx, `SELECT DISTINCT block_number FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&storedBlock)
	if err != nil {
		t.Fatalf("failed to query stored block: %v", err)
	}
	if storedBlock != 100 {
		t.Errorf("expected prices stored at block 100, got %d", storedBlock)
	}

	// Verify actual price values
	var priceUSD1, priceUSD2 float64
	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_source_id = $1 AND token_id = $2`, sourceID, tokenID1).Scan(&priceUSD1)
	if err != nil {
		t.Fatalf("failed to query TK1 price: %v", err)
	}
	// 100_000_000 / 1e8 = 1.0
	if priceUSD1 != 1.0 {
		t.Errorf("expected TK1 price 1.0, got %f", priceUSD1)
	}

	err = pool.QueryRow(ctx, `SELECT price_usd FROM onchain_token_price WHERE oracle_source_id = $1 AND token_id = $2`, sourceID, tokenID2).Scan(&priceUSD2)
	if err != nil {
		t.Fatalf("failed to query TK2 price: %v", err)
	}
	// 250_000_000_000 / 1e8 = 2500.0
	if priceUSD2 != 2500.0 {
		t.Errorf("expected TK2 price 2500.0, got %f", priceUSD2)
	}
}

func TestIntegration_BackfillRun_ResumeFromLatestBlock(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-resume", "Test Resume", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000011", "RSM1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000012", "RSM2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID2)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000011"),
		tokenID2: common.HexToAddress("0x0000000000000000000000000000000000000012"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// First run: blocks 100-104
	svc, err := NewService(
		Config{
			Concurrency:  1,
			BatchSize:    100,
			OracleSource: "test-resume",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 2),
		repo,
		tokenAddresses,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Run(ctx, 100, 104)
	if err != nil {
		t.Fatalf("Run (first) failed: %v", err)
	}

	var countAfterFirst int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&countAfterFirst)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	// 5 blocks * 2 tokens = 10 prices (all unique due to block-dependent prices)
	if countAfterFirst != 10 {
		t.Errorf("expected 10 prices after first run, got %d", countAfterFirst)
	}

	// Second run: same range should resume from latest block (104) and skip all
	svc2, err := NewService(
		Config{
			Concurrency:  1,
			BatchSize:    100,
			OracleSource: "test-resume",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 2),
		repo,
		tokenAddresses,
	)
	if err != nil {
		t.Fatalf("NewService (second) failed: %v", err)
	}

	err = svc2.Run(ctx, 100, 104)
	if err != nil {
		t.Fatalf("Run (second) failed: %v", err)
	}

	// Count should be unchanged (resume detected block 104 as latest, from >= 105)
	var countAfterSecond int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&countAfterSecond)
	if err != nil {
		t.Fatalf("failed to query price count after second run: %v", err)
	}
	if countAfterSecond != countAfterFirst {
		t.Errorf("expected %d prices after second (resumed) run, got %d", countAfterFirst, countAfterSecond)
	}

	// Third run: extend range 100-109. Should resume from block 105.
	svc3, err := NewService(
		Config{
			Concurrency:  1,
			BatchSize:    100,
			OracleSource: "test-resume",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 2),
		repo,
		tokenAddresses,
	)
	if err != nil {
		t.Fatalf("NewService (third) failed: %v", err)
	}

	err = svc3.Run(ctx, 100, 109)
	if err != nil {
		t.Fatalf("Run (third) failed: %v", err)
	}

	var countAfterThird int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&countAfterThird)
	if err != nil {
		t.Fatalf("failed to query price count after third run: %v", err)
	}
	// Should have 10 (original) + 5 new blocks (105-109) * 2 tokens = 10 more = 20 total
	expectedTotal := 20
	if countAfterThird != expectedTotal {
		t.Errorf("expected %d prices after third (extended) run, got %d", expectedTotal, countAfterThird)
	}

	// Verify block range coverage
	var minBlock, maxBlock int64
	err = pool.QueryRow(ctx, `SELECT MIN(block_number), MAX(block_number) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&minBlock, &maxBlock)
	if err != nil {
		t.Fatalf("failed to query block range: %v", err)
	}
	if minBlock != 100 {
		t.Errorf("expected min block 100, got %d", minBlock)
	}
	if maxBlock != 109 {
		t.Errorf("expected max block 109, got %d", maxBlock)
	}
}

func TestIntegration_BackfillRun_UpsertIdempotency(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-idempotent", "Test Idempotent", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000021", "IDP1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000021"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// First run: insert prices for blocks 100-102
	svc, err := NewService(
		Config{
			Concurrency:  1,
			BatchSize:    100,
			OracleSource: "test-idempotent",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 1),
		repo,
		tokenAddresses,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Run(ctx, 100, 102)
	if err != nil {
		t.Fatalf("Run (first) failed: %v", err)
	}

	var countFirst int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&countFirst)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}

	// Manually re-run UpsertPrices with the same data to verify ON CONFLICT DO NOTHING.
	// The resume logic would skip this, so we test idempotency at the repo level.
	sampleTimestamp := time.Unix(1700000100, 0).UTC()
	oracleAddrBytes := oracleAddr.Bytes()
	repo.UpsertPrices(ctx, []*entity.OnchainTokenPrice{
		{
			TokenID:        tokenID1,
			OracleSourceID: int16(sourceID),
			BlockNumber:    100,
			BlockVersion:   0,
			Timestamp:      sampleTimestamp,
			OracleAddress:  oracleAddrBytes,
			PriceUSD:       999.99, // Different price, but same PK
		},
	})

	var countSecond int
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&countSecond)
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

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-latest-block", "Test Latest Block", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000031", "LB1", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000031"),
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	// Before any data: GetLatestBlock should return 0
	latestBlock, err := repo.GetLatestBlock(ctx, sourceID)
	if err != nil {
		t.Fatalf("GetLatestBlock failed: %v", err)
	}
	if latestBlock != 0 {
		t.Errorf("expected latest block 0 before any data, got %d", latestBlock)
	}

	// Run backfill
	svc, err := NewService(
		Config{
			Concurrency:  1,
			BatchSize:    100,
			OracleSource: "test-latest-block",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		integrationMockMulticallFactory(t, 1),
		repo,
		tokenAddresses,
	)
	if err != nil {
		t.Fatalf("NewService failed: %v", err)
	}

	err = svc.Run(ctx, 200, 205)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// After backfill: GetLatestBlock should return 205
	latestBlock, err = repo.GetLatestBlock(ctx, sourceID)
	if err != nil {
		t.Fatalf("GetLatestBlock after backfill failed: %v", err)
	}
	if latestBlock != 205 {
		t.Errorf("expected latest block 205, got %d", latestBlock)
	}
}

func TestIntegration_BackfillRun_MultipleSelectiveChanges(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	t.Cleanup(cleanup)

	ctx := context.Background()
	logger := testutil.DiscardLogger()

	sourceID := testutil.SeedOracleSource(t, ctx, pool, "test-selective", "Test Selective", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID1 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000041", "SC1", 18)
	tokenID2 := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000042", "SC2", 18)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID1)
	testutil.SeedOracleAsset(t, ctx, pool, sourceID, tokenID2)

	tokenAddresses := map[int64]common.Address{
		tokenID1: common.HexToAddress("0x0000000000000000000000000000000000000041"),
		tokenID2: common.HexToAddress("0x0000000000000000000000000000000000000042"),
	}

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
				return multicallResult(t, calls, prices), nil
			},
		}, nil
	}

	repo, err := postgres.NewOnchainPriceRepository(pool, logger, 100)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	svc, err := NewService(
		Config{
			Concurrency:  1,
			BatchSize:    100,
			OracleSource: "test-selective",
			Logger:       logger,
		},
		integrationMockHeaderFetcher(),
		mcFactory,
		repo,
		tokenAddresses,
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
	err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1`, sourceID).Scan(&priceCount)
	if err != nil {
		t.Fatalf("failed to query price count: %v", err)
	}
	if priceCount != 4 {
		t.Errorf("expected 4 price records (change detection), got %d", priceCount)
	}

	// Verify specific blocks have entries
	for _, blockNum := range []int64{100, 102, 104} {
		var count int
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1 AND block_number = $2`, sourceID, blockNum).Scan(&count)
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
		err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM onchain_token_price WHERE oracle_source_id = $1 AND block_number = $2`, sourceID, blockNum).Scan(&count)
		if err != nil {
			t.Fatalf("failed to query block %d: %v", blockNum, err)
		}
		if count != 0 {
			t.Errorf("expected 0 prices at block %d (no change), got %d", blockNum, count)
		}
	}
}
