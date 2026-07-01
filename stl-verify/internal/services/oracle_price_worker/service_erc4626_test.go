package oracle_price_worker

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	fsUSDSAddr   = common.HexToAddress("0x2BBE31d63E6813E3AC858C04dae43FB2a72B0D11")
	usdsFeedAddr = common.HexToAddress("0xfF30586cD0F29eD462364C7e81375FC0C71219b1")
)

// erc4626OracleSetup configures the mock repo for an erc4626_share oracle pricing
// fsUSDS (token_id 10) against the USDS USD feed.
func erc4626OracleSetup(r *mockRepo) {
	r.getEnabledOraclesByChainFn = func(_ context.Context, _ int64) ([]*entity.Oracle, error) {
		return []*entity.Oracle{{
			ID: 5, Name: "fluid_fsusds", Enabled: true,
			OracleType: entity.OracleTypeERC4626Share,
		}}, nil
	}
	r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
		return []*entity.OracleAsset{{
			ID: 1, OracleID: 5, TokenID: 10, Enabled: true,
			FeedAddress: usdsFeedAddr, FeedDecimals: 8, QuoteCurrency: "USD",
		}}, nil
	}
	r.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
		return map[int64][]byte{10: fsUSDSAddr.Bytes()}, nil
	}
	r.getTokenDecimalsFn = func(_ context.Context, _ int64) (map[int64]int, error) {
		return map[int64]int{10: 18}, nil
	}
	r.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{}, nil
	}
}

func e18(n int64) *big.Int {
	return new(big.Int).Mul(big.NewInt(n), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
}

// newERC4626Multicaller returns a multicaller producing [convertToAssets, latestRoundData]
// pairs: assets per vault and feedAnswer per underlying feed, in call order.
func newERC4626Multicaller(t *testing.T, assets *big.Int, feedAnswer *big.Int) *testutil.MockMulticaller {
	t.Helper()
	convertData := testutil.PackConvertToAssets(t, assets)
	roundData := testutil.PackLatestRoundData(t,
		big.NewInt(1), feedAnswer, big.NewInt(1000), big.NewInt(1000), big.NewInt(1))
	return &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			if len(calls) != 2 {
				t.Fatalf("expected 2 calls (convertToAssets + latestRoundData), got %d", len(calls))
			}
			return []outbound.Result{
				{Success: true, ReturnData: convertData},
				{Success: true, ReturnData: roundData},
			}, nil
		},
	}
}

// ---------------------------------------------------------------------------
// TestStart_ERC4626Oracle — erc4626_share oracle initialization
// ---------------------------------------------------------------------------

func TestStart_ERC4626Oracle(t *testing.T) {
	repo := &mockRepo{}
	erc4626OracleSetup(repo)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	mc := newERC4626Multicaller(t, e18(1), big.NewInt(100_000_000))
	svc, err := NewService(validConfig(), consumer, defaultBlockCacheReader(), repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	}()

	if len(svc.units) != 1 {
		t.Fatalf("units count = %d, want 1", len(svc.units))
	}
	unit := svc.units[0]
	if unit.Oracle.OracleType != entity.OracleTypeERC4626Share {
		t.Errorf("OracleType = %q, want %q", unit.Oracle.OracleType, entity.OracleTypeERC4626Share)
	}
	if len(unit.ERC4626Vaults) != 1 {
		t.Fatalf("ERC4626Vaults count = %d, want 1", len(unit.ERC4626Vaults))
	}
	if unit.ERC4626Vaults[0].VaultAddress != fsUSDSAddr {
		t.Errorf("VaultAddress = %s, want %s", unit.ERC4626Vaults[0].VaultAddress, fsUSDSAddr)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_ERC4626Oracle — end-to-end share price processing
// ---------------------------------------------------------------------------

func TestProcessBlock_ERC4626Oracle(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	erc4626OracleSetup(repo)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	// convertToAssets(1e18) = 1.05e18 → ratio 1.05; USDS/USD = 1.0 → $1.05.
	assets := new(big.Int).Add(e18(1), new(big.Int).Div(e18(1), big.NewInt(20)))
	mc := newERC4626Multicaller(t, assets, big.NewInt(100_000_000))

	cfg := validConfig()
	cfg.PollInterval = 1 * time.Millisecond

	svc, err := NewService(cfg, consumer, defaultBlockCacheReader(), repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	svc.decimalsValidated = true // covered separately by the decimals-mismatch test
	defer func() {
		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	}()

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22000000, Version: 1,
		BlockHash: "0xfsusds1", BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 1 {
		t.Errorf("UpsertPrices call count = %d, want 1", repo.upsertPricesCalls)
	}
	if len(repo.lastUpserted) != 1 {
		t.Fatalf("lastUpserted length = %d, want 1", len(repo.lastUpserted))
	}
	p := repo.lastUpserted[0]
	if p.TokenID != 10 {
		t.Errorf("TokenID = %d, want 10", p.TokenID)
	}
	if p.OracleID != 5 {
		t.Errorf("OracleID = %d, want 5", p.OracleID)
	}
	if p.PriceUSD != 1.05 {
		t.Errorf("PriceUSD = %f, want 1.05", p.PriceUSD)
	}
	if p.BlockNumber != 22000000 {
		t.Errorf("BlockNumber = %d, want 22000000", p.BlockNumber)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_ERC4626Oracle_UnderlyingFeedDecimalsMismatch — on-chain feed
// decimals differ from seeded feed_decimals, so processing fails hard rather than
// writing a mis-scaled price.
// ---------------------------------------------------------------------------

func TestProcessBlock_ERC4626Oracle_UnderlyingFeedDecimalsMismatch(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	erc4626OracleSetup(repo) // seeds feed_decimals = 8

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	// decimals() returns 18 on-chain, contradicting the seeded 8.
	mc := &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			return []outbound.Result{
				{Success: true, ReturnData: testutil.PackDecimals(t, 18)},
			}, nil
		},
	}

	svc, err := NewService(validConfig(), consumer, defaultBlockCacheReader(), repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	}()

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22000000, Version: 1,
		BlockHash: "0xfsusdsdecmismatch", BlockTimestamp: blockTimestamp,
	}
	err = svc.processBlock(context.Background(), event)
	if err == nil {
		t.Fatal("expected decimals mismatch error, got nil")
	}
	if !strings.Contains(err.Error(), "feed decimals") {
		t.Errorf("error = %q, expected it to contain 'feed decimals'", err)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_ERC4626Oracle_VaultFails — sole vault reverts, no upsert and
// processing errors so SQS retries rather than dropping the price.
// ---------------------------------------------------------------------------

func TestProcessBlock_ERC4626Oracle_VaultFails(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	erc4626OracleSetup(repo)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	mc := &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			results := make([]outbound.Result, len(calls))
			for i := range results {
				results[i] = outbound.Result{Success: false}
			}
			return results, nil
		},
	}

	cfg := validConfig()
	cfg.PollInterval = 1 * time.Millisecond

	svc, err := NewService(cfg, consumer, defaultBlockCacheReader(), repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	svc.decimalsValidated = true // covered separately by the decimals-mismatch test
	defer func() {
		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	}()

	event := outbound.BlockEvent{
		ChainID: 1, BlockNumber: 22000000, Version: 1,
		BlockHash: "0xfsusdsfail", BlockTimestamp: blockTimestamp,
	}
	err = svc.processBlock(context.Background(), event)
	if err == nil {
		t.Fatal("expected error when the sole vault fails, got nil")
	}
	if !strings.Contains(err.Error(), "erc4626 vaults failed") {
		t.Errorf("error = %q, expected it to contain 'erc4626 vaults failed'", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()
	if repo.upsertPricesCalls != 0 {
		t.Errorf("UpsertPrices call count = %d, want 0 (all vault calls failed)", repo.upsertPricesCalls)
	}
}
