package oracle_price_worker

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	curvePoolAddr     = common.HexToAddress("0xE79c1C7E24755574438A26D5e062AD2626c04662")
	curveUSDCFeedAddr = common.HexToAddress("0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6")
	curveAUSDFeedAddr = common.HexToAddress("0xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD")
)

const curveLPTokenID = 587717

// curveLPNGOracleSetup configures the mock repo for a curve_lp_ng oracle
// pricing the AUSD/USDC pool LP token against its two coin USD feeds.
func curveLPNGOracleSetup(r *mockRepo) {
	r.getEnabledOraclesByChainFn = func(_ context.Context, _ int64) ([]*entity.Oracle, error) {
		return []*entity.Oracle{{
			ID: 7, Name: "curve_ausdusdc_lp", Enabled: true,
			OracleType: entity.OracleTypeCurveLPNG, Address: curvePoolAddr, PriceDecimals: 8,
		}}, nil
	}
	r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
		return []*entity.OracleAsset{
			{ID: 1, OracleID: 7, TokenID: curveLPTokenID, Enabled: true, FeedAddress: curveUSDCFeedAddr, FeedDecimals: 8, QuoteCurrency: "USD"},
			{ID: 2, OracleID: 7, TokenID: curveLPTokenID, Enabled: true, FeedAddress: curveAUSDFeedAddr, FeedDecimals: 18, QuoteCurrency: "USD"},
		}, nil
	}
	r.getTokenInfosFn = func(_ context.Context, _ int64) (map[int64]outbound.TokenInfo, error) {
		return map[int64]outbound.TokenInfo{curveLPTokenID: {Address: curvePoolAddr.Bytes(), Decimals: 18}}, nil
	}
	r.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{}, nil
	}
}

// curveMockMulticaller implements outbound.Multicaller. Number-pinned Execute
// serves only the one-time feed decimals validation; per-block pricing must
// arrive via ExecuteAtHash.
type curveMockMulticaller struct {
	mu              sync.Mutex
	executeFn       func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error)
	executeAtHashFn func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error)
	hashCalls       int
	lastHash        common.Hash
}

func (m *curveMockMulticaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	if m.executeFn != nil {
		return m.executeFn(ctx, calls, blockNumber)
	}
	return nil, errors.New("Execute not mocked")
}

func (m *curveMockMulticaller) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	m.mu.Lock()
	m.hashCalls++
	m.lastHash = blockHash
	m.mu.Unlock()
	if m.executeAtHashFn != nil {
		return m.executeAtHashFn(ctx, calls, blockHash)
	}
	return nil, errors.New("ExecuteAtHash not mocked")
}

func (m *curveMockMulticaller) Address() common.Address {
	return common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")
}

func packCurveVirtualPrice(t *testing.T, price *big.Int) []byte {
	t.Helper()
	poolABI, err := abis.GetCurveNGPoolABI()
	if err != nil {
		t.Fatalf("loading Curve NG pool ABI: %v", err)
	}
	data, err := poolABI.Methods["get_virtual_price"].Outputs.Pack(price)
	if err != nil {
		t.Fatalf("packing get_virtual_price return: %v", err)
	}
	return data
}

// curveDecimalsValidationFn answers the one-time ValidateFeedDecimals batch
// with the given on-chain decimals for the USDC and AUSD feeds.
func curveDecimalsValidationFn(t *testing.T, usdcDecimals, ausdDecimals uint8) func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	return func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 {
			t.Fatalf("expected 2 decimals() validation calls, got %d", len(calls))
		}
		return []outbound.Result{
			{Success: true, ReturnData: testutil.PackDecimals(t, usdcDecimals)},
			{Success: true, ReturnData: testutil.PackDecimals(t, ausdDecimals)},
		}, nil
	}
}

// curvePriceResultsFn answers each per-block pricing batch with the given
// virtual price and feed answers.
func curvePriceResultsFn(t *testing.T, virtualPrice, usdcAnswer, ausdAnswer *big.Int) func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	return func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) != 3 {
			t.Fatalf("expected 3 pricing calls (get_virtual_price + 2 feeds), got %d", len(calls))
		}
		return []outbound.Result{
			{Success: true, ReturnData: packCurveVirtualPrice(t, virtualPrice)},
			{Success: true, ReturnData: testutil.PackLatestRoundData(t, big.NewInt(1), usdcAnswer, big.NewInt(1000), big.NewInt(1000), big.NewInt(1))},
			{Success: true, ReturnData: testutil.PackLatestRoundData(t, big.NewInt(1), ausdAnswer, big.NewInt(1000), big.NewInt(1000), big.NewInt(1))},
		}, nil
	}
}

func startCurveLPNGService(t *testing.T, repo *mockRepo, mc outbound.Multicaller) *Service {
	t.Helper()
	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	svc, err := NewService(validConfig(), consumer, defaultBlockCacheReader(), repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})
	return svc
}

// ---------------------------------------------------------------------------
// TestStart_CurveLPNGOracle: curve_lp_ng oracle initialization
// ---------------------------------------------------------------------------

func TestStart_CurveLPNGOracle(t *testing.T) {
	repo := &mockRepo{}
	curveLPNGOracleSetup(repo)

	svc := startCurveLPNGService(t, repo, &curveMockMulticaller{})

	if len(svc.units) != 1 {
		t.Fatalf("units count = %d, want 1", len(svc.units))
	}
	unit := svc.units[0]
	if unit.Oracle.OracleType != entity.OracleTypeCurveLPNG {
		t.Errorf("OracleType = %q, want %q", unit.Oracle.OracleType, entity.OracleTypeCurveLPNG)
	}
	if unit.CurveLPNGPool == nil {
		t.Fatal("CurveLPNGPool is nil")
	}
	if unit.CurveLPNGPool.PoolAddress != curvePoolAddr {
		t.Errorf("PoolAddress = %s, want %s", unit.CurveLPNGPool.PoolAddress, curvePoolAddr)
	}
	if len(unit.CurveLPNGPool.CoinFeeds) != 2 {
		t.Errorf("CoinFeeds len = %d, want 2", len(unit.CurveLPNGPool.CoinFeeds))
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle: end-to-end LP price processing, including
// the one-time coin feed decimals validation and the hash-pinned price fetch.
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	curveLPNGOracleSetup(repo)

	// virtual_price 1.25, USDC 0.75 (8 dec), AUSD 1.5 (18 dec) → min 0.75 → $0.9375.
	mc := &curveMockMulticaller{
		executeFn: curveDecimalsValidationFn(t, 8, 18),
		executeAtHashFn: curvePriceResultsFn(t,
			big.NewInt(1_250_000_000_000_000_000),
			big.NewInt(75_000_000),
			big.NewInt(1_500_000_000_000_000_000)),
	}

	svc := startCurveLPNGService(t, repo, mc)

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22800000, Version: 1,
		BlockHash: "0x00000000000000000000000000000000000000000000000000c0ffee00000001", BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	mc.mu.Lock()
	if mc.hashCalls != 1 {
		t.Errorf("ExecuteAtHash calls = %d, want 1 (pricing must be hash-pinned)", mc.hashCalls)
	}
	if want := common.HexToHash(event.BlockHash); mc.lastHash != want {
		t.Errorf("ExecuteAtHash blockHash = %s, want %s", mc.lastHash, want)
	}
	mc.mu.Unlock()

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 1 {
		t.Fatalf("UpsertPrices call count = %d, want 1", repo.upsertPricesCalls)
	}
	if len(repo.lastUpserted) != 1 {
		t.Fatalf("lastUpserted length = %d, want 1", len(repo.lastUpserted))
	}
	p := repo.lastUpserted[0]
	if p.TokenID != curveLPTokenID {
		t.Errorf("TokenID = %d, want %d", p.TokenID, curveLPTokenID)
	}
	if p.OracleID != 7 {
		t.Errorf("OracleID = %d, want 7", p.OracleID)
	}
	if p.PriceUSD != 0.9375 {
		t.Errorf("PriceUSD = %f, want 0.9375", p.PriceUSD)
	}
	if p.BlockNumber != 22800000 {
		t.Errorf("BlockNumber = %d, want 22800000", p.BlockNumber)
	}
	if p.BlockVersion != 1 {
		t.Errorf("BlockVersion = %d, want 1 (the event's version)", p.BlockVersion)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle_ReorgRepublishBypassesCache: a reorg
// republish (Version > 0) of an already-priced block must upsert at the new
// block_version even when the price is unchanged; readers order by
// block_version DESC, so suppressing the write would leave the reorged block
// without a row at its current version.
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle_ReorgRepublishBypassesCache(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	curveLPNGOracleSetup(repo)

	mc := &curveMockMulticaller{
		executeFn: curveDecimalsValidationFn(t, 8, 18),
		executeAtHashFn: curvePriceResultsFn(t,
			big.NewInt(1_250_000_000_000_000_000),
			big.NewInt(75_000_000),
			big.NewInt(1_500_000_000_000_000_000)),
	}

	svc := startCurveLPNGService(t, repo, mc)

	v0 := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22800000, Version: 0,
		BlockHash: "0x00000000000000000000000000000000000000000000000000000000c0ffee0a", BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), v0); err != nil {
		t.Fatalf("processBlock (v0): %v", err)
	}

	v1 := v0
	v1.Version = 1
	v1.BlockHash = "0x00000000000000000000000000000000000000000000000000000000c0ffee0b"
	if err := svc.processBlock(context.Background(), v1); err != nil {
		t.Fatalf("processBlock (v1 republish): %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 2 {
		t.Fatalf("UpsertPrices call count = %d, want 2 (reorg republish must re-emit)", repo.upsertPricesCalls)
	}
	if len(repo.lastUpserted) != 1 {
		t.Fatalf("lastUpserted length = %d, want 1", len(repo.lastUpserted))
	}
	p := repo.lastUpserted[0]
	if p.BlockVersion != 1 {
		t.Errorf("BlockVersion = %d, want 1", p.BlockVersion)
	}
	if p.PriceUSD != 0.9375 {
		t.Errorf("PriceUSD = %f, want 0.9375", p.PriceUSD)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle_SamePriceNextBlockSkipsUpsert: the
// change-detection cache shared with the feed path suppresses writes when the
// LP price is unchanged (fresh v0 blocks; reorg republishes always re-emit).
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle_SamePriceNextBlockSkipsUpsert(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	curveLPNGOracleSetup(repo)

	mc := &curveMockMulticaller{
		executeFn: curveDecimalsValidationFn(t, 8, 18),
		executeAtHashFn: curvePriceResultsFn(t,
			big.NewInt(1_250_000_000_000_000_000),
			big.NewInt(75_000_000),
			big.NewInt(1_500_000_000_000_000_000)),
	}

	svc := startCurveLPNGService(t, repo, mc)

	first := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22800000, Version: 0,
		BlockHash: "0x00000000000000000000000000000000000000000000000000c0ffee00000001", BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), first); err != nil {
		t.Fatalf("processBlock (first): %v", err)
	}

	second := first
	second.BlockNumber = 22800001
	second.BlockHash = "0x00000000000000000000000000000000000000000000000000c0ffee00000002"
	if err := svc.processBlock(context.Background(), second); err != nil {
		t.Fatalf("processBlock (second): %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 1 {
		t.Errorf("UpsertPrices call count = %d, want 1 (unchanged price must not re-upsert)", repo.upsertPricesCalls)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle_UpsertFailureIsRetriable: a failed upsert
// must leave the change-detection cache uncommitted, so the SQS redelivery of
// the same block detects the price again and retries the write instead of
// acking with the row never persisted.
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle_UpsertFailureIsRetriable(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	curveLPNGOracleSetup(repo)
	failNext := true
	repo.upsertPricesFn = func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
		if failNext {
			failNext = false
			return errors.New("database write failure")
		}
		return nil
	}

	mc := &curveMockMulticaller{
		executeFn: curveDecimalsValidationFn(t, 8, 18),
		executeAtHashFn: curvePriceResultsFn(t,
			big.NewInt(1_250_000_000_000_000_000),
			big.NewInt(75_000_000),
			big.NewInt(1_500_000_000_000_000_000)),
	}

	svc := startCurveLPNGService(t, repo, mc)

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22800000, Version: 0,
		BlockHash: "0x00000000000000000000000000000000000000000000000000000000c0ffee01", BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), event); err == nil {
		t.Fatal("expected processBlock to fail when UpsertPrices fails")
	}

	if err := svc.processBlock(context.Background(), event); err != nil {
		t.Fatalf("processBlock (redelivery): %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 2 {
		t.Fatalf("UpsertPrices call count = %d, want 2 (redelivery must retry the failed write)", repo.upsertPricesCalls)
	}
	if len(repo.lastUpserted) != 1 {
		t.Fatalf("lastUpserted length = %d, want 1", len(repo.lastUpserted))
	}
	if repo.lastUpserted[0].PriceUSD != 0.9375 {
		t.Errorf("retried PriceUSD = %f, want 0.9375", repo.lastUpserted[0].PriceUSD)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle_MissingBlockHash: an event without a block
// hash must fail hard rather than silently downgrading to a number-pinned read.
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle_MissingBlockHash(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	curveLPNGOracleSetup(repo)

	mc := &curveMockMulticaller{
		executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
			t.Fatal("ExecuteAtHash should not be called without a block hash")
			return nil, nil
		},
	}

	svc := startCurveLPNGService(t, repo, mc)
	svc.decimalsValidated = true // covered by the decimals-mismatch test

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22800000, Version: 1,
		BlockTimestamp: blockTimestamp, // BlockHash intentionally empty
	}
	err := svc.processBlock(context.Background(), event)
	if err == nil {
		t.Fatal("expected error for a missing block hash, got nil")
	}
	if !strings.Contains(err.Error(), "missing block hash") {
		t.Errorf("error = %q, expected it to contain 'missing block hash'", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 0 {
		t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle_MalformedBlockHash: a malformed hash on the
// event must fail hard as malformed (BlockEvent.ParsedBlockHash validates
// strictly). common.HexToHash would silently coerce it into a zero-padded,
// plausible-looking hash and pin the read to a block that does not exist.
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle_MalformedBlockHash(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	tests := []struct {
		name      string
		blockHash string
	}{
		{name: "short hex", blockHash: "0xabc"},
		{name: "no 0x prefix", blockHash: "00000000000000000000000000000000000000000000000000000000c0ffee0c"},
		{name: "non-hex characters", blockHash: "0x00000000000000000000000000000000000000000000000000000000zzzzzz0d"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &mockRepo{}
			curveLPNGOracleSetup(repo)

			mc := &curveMockMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					t.Error("ExecuteAtHash must not be called for a malformed block hash")
					return nil, errors.New("must not be called")
				},
			}

			svc := startCurveLPNGService(t, repo, mc)
			svc.decimalsValidated = true // covered by the decimals-mismatch test

			event := outbound.BlockEvent{
				ChainID: 1, BlockNumber: 22800000, Version: 1,
				BlockHash: tc.blockHash, BlockTimestamp: blockTimestamp,
			}
			err := svc.processBlock(context.Background(), event)
			if err == nil {
				t.Fatal("expected error for a malformed block hash, got nil")
			}
			if !strings.Contains(err.Error(), "malformed block hash") {
				t.Errorf("error = %q, expected it to contain 'malformed block hash'", err)
			}
			if !strings.Contains(err.Error(), tc.blockHash) {
				t.Errorf("error = %q, expected it to name the actual value %q", err, tc.blockHash)
			}
			if strings.Contains(err.Error(), "missing block hash") {
				t.Errorf("error = %q, must report malformed, not missing", err)
			}

			repo.mu.Lock()
			defer repo.mu.Unlock()
			if repo.upsertPricesCalls != 0 {
				t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle_PoolCallFails: a reverting
// get_virtual_price errors the block (SQS retries) and persists nothing.
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle_PoolCallFails(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	curveLPNGOracleSetup(repo)

	mc := &curveMockMulticaller{
		executeAtHashFn: func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
			results := make([]outbound.Result, len(calls))
			for i := range results {
				results[i] = outbound.Result{Success: false}
			}
			return results, nil
		},
	}

	svc := startCurveLPNGService(t, repo, mc)
	svc.decimalsValidated = true // covered by the decimals-mismatch test

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22800000, Version: 1,
		BlockHash: "0x00000000000000000000000000000000000000000000000000c0ffee00000004", BlockTimestamp: blockTimestamp,
	}
	err := svc.processBlock(context.Background(), event)
	if err == nil {
		t.Fatal("expected error when the pool call fails, got nil")
	}
	if !strings.Contains(err.Error(), "get_virtual_price call failed") {
		t.Errorf("error = %q, expected it to contain 'get_virtual_price call failed'", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 0 {
		t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_CurveLPNGOracle_FeedDecimalsMismatch: on-chain coin feed
// decimals contradicting the registry fail hard before any price is written
// (a mismatch would mis-scale the min() input by orders of magnitude).
// ---------------------------------------------------------------------------

func TestProcessBlock_CurveLPNGOracle_FeedDecimalsMismatch(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	curveLPNGOracleSetup(repo) // seeds AUSD feed_decimals = 18

	mc := &curveMockMulticaller{
		// decimals() returns 8 for the AUSD feed on-chain, contradicting the seeded 18.
		executeFn: curveDecimalsValidationFn(t, 8, 8),
	}

	svc := startCurveLPNGService(t, repo, mc)

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22800000, Version: 1,
		BlockHash: "0x00000000000000000000000000000000000000000000000000c0ffee00000005", BlockTimestamp: blockTimestamp,
	}
	err := svc.processBlock(context.Background(), event)
	if err == nil {
		t.Fatal("expected decimals mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "feed decimals") {
		t.Errorf("error = %q, expected it to contain 'feed decimals'", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 0 {
		t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
	}
}
