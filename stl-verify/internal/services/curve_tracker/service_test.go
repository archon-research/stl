package curve_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"testing"
	"time"

	curveapi "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/curve"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
)

// ── Mocks ──

type mockCurveRepository struct {
	saved    []*entity.CurvePoolSnapshot
	tokenIDs map[string]int64 // key: "chainID:address"
}

func newMockRepo(tokenIDs map[string]int64) *mockCurveRepository {
	if tokenIDs == nil {
		tokenIDs = make(map[string]int64)
	}
	return &mockCurveRepository{tokenIDs: tokenIDs}
}

func (m *mockCurveRepository) SaveSnapshots(ctx context.Context, snapshots []*entity.CurvePoolSnapshot) error {
	m.saved = append(m.saved, snapshots...)
	return nil
}

func (m *mockCurveRepository) LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error) {
	key := fmt.Sprintf("%d:%s", chainID, address.Hex())
	if id, ok := m.tokenIDs[key]; ok {
		return id, nil
	}
	return 0, fmt.Errorf("not found")
}

type mockMulticaller struct {
	executeFn func(ctx context.Context, calls []outbound.Call, block *big.Int) ([]outbound.Result, error)
}

func (m *mockMulticaller) Execute(ctx context.Context, calls []outbound.Call, block *big.Int) ([]outbound.Result, error) {
	return m.executeFn(ctx, calls, block)
}

// ── DefaultPools ──

func TestDefaultPools(t *testing.T) {
	pools := DefaultPools()

	if len(pools) != 3 {
		t.Fatalf("expected 3 pools, got %d", len(pools))
	}

	expected := map[string]string{
		"sUSDSUSDT": "0x00836fE54625BE242BcFa286207795405ca4fD10",
		"PYUSDUSDS": "0xA632D59b9B804a956BfaA9b48Af3A1b74808FC1f",
		"AUSDUSDC":  "0xE79C1C7E24755574438A26D5e062Ad2626C04662",
	}

	for _, p := range pools {
		want, ok := expected[p.Name]
		if !ok {
			t.Errorf("unexpected pool name: %s", p.Name)
			continue
		}
		if p.Address != common.HexToAddress(want) {
			t.Errorf("pool %s: address = %s, want %s", p.Name, p.Address.Hex(), want)
		}
		if p.ChainID != 1 {
			t.Errorf("pool %s: chainID = %d, want 1", p.Name, p.ChainID)
		}
	}
}

// ── TVL Calculation ──

func TestCalculateTVL_StablecoinPool(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		CoinBalances: []CoinBalance{
			{Balance: "50000000000000", Decimals: 6, Symbol: "USDC"},              // 50M USDC
			{Balance: "50000000000000000000000000", Decimals: 18, Symbol: "USDS"}, // 50M USDS
		},
	}

	tvl := svc.calculateTVL(snap)
	if tvl == nil {
		t.Fatal("expected non-nil TVL")
	}

	// Should be ~100M
	tvlFloat, _ := tvl.Float64()
	if tvlFloat < 99_000_000 || tvlFloat > 101_000_000 {
		t.Errorf("expected TVL ~100M, got %.2f", tvlFloat)
	}
}

func TestCalculateTVL_sUSDSUSDT(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	// Real data from sUSDSUSDT pool
	snap := &PoolSnapshot{
		CoinBalances: []CoinBalance{
			{Balance: "8108953488903302589788809", Decimals: 18, Symbol: "sUSDS"},
			{Balance: "41168269354572", Decimals: 6, Symbol: "USDT"},
		},
	}

	tvl := svc.calculateTVL(snap)
	if tvl == nil {
		t.Fatal("expected non-nil TVL")
	}

	// sUSDS ~8.1M + USDT ~41.2M ≈ 49.3M (slightly under actual due to sUSDS yield)
	tvlFloat, _ := tvl.Float64()
	if tvlFloat < 48_000_000 || tvlFloat > 51_000_000 {
		t.Errorf("expected TVL ~49M, got %.2f", tvlFloat)
	}
}

func TestCalculateTVL_Empty(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	tvl := svc.calculateTVL(&PoolSnapshot{})
	if tvl != nil {
		t.Error("expected nil TVL for empty pool")
	}
}

func TestCalculateTVL_ZeroDecimals_Skipped(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		CoinBalances: []CoinBalance{
			{Balance: "1000000", Decimals: 0, Symbol: "UNKNOWN"},  // should be skipped
			{Balance: "50000000000", Decimals: 6, Symbol: "USDC"}, // 50K USDC
		},
	}

	tvl := svc.calculateTVL(snap)
	if tvl == nil {
		t.Fatal("expected non-nil TVL")
	}

	tvlFloat, _ := tvl.Float64()
	if tvlFloat < 49_000 || tvlFloat > 51_000 {
		t.Errorf("expected TVL ~50K (skipping zero-decimals coin), got %.2f", tvlFloat)
	}
}

// ── Token ID Resolution ──

func TestResolveTokenIDs(t *testing.T) {
	sUSDS := common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")
	usdt := common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")

	repo := newMockRepo(map[string]int64{
		fmt.Sprintf("1:%s", sUSDS.Hex()): 12,
		fmt.Sprintf("1:%s", usdt.Hex()):  9,
	})

	svc := &Service{
		repo:   repo,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	snapshots := []*PoolSnapshot{
		{
			ChainID: 1,
			CoinBalances: []CoinBalance{
				{Address: sUSDS},
				{Address: usdt},
			},
		},
	}

	ids, err := svc.resolveTokenIDs(context.Background(), []common.Address{sUSDS, usdt}, snapshots)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ids[sUSDS] != 12 {
		t.Errorf("sUSDS token_id = %d, want 12", ids[sUSDS])
	}
	if ids[usdt] != 9 {
		t.Errorf("USDT token_id = %d, want 9", ids[usdt])
	}
}

func TestResolveTokenIDs_MissingToken(t *testing.T) {
	unknown := common.HexToAddress("0xdeadbeef")

	repo := newMockRepo(nil)
	svc := &Service{
		repo:   repo,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	snapshots := []*PoolSnapshot{
		{ChainID: 1, CoinBalances: []CoinBalance{{Address: unknown}}},
	}

	ids, err := svc.resolveTokenIDs(context.Background(), []common.Address{unknown}, snapshots)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, ok := ids[unknown]; ok {
		t.Error("expected missing token to not be in results")
	}
}

// ── Build Entity ──

func TestBuildEntity_WithTVLAndAPY(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		PoolAddress: common.HexToAddress("0xaaaa"),
		ChainID:     1,
		BlockNumber: 24720000,
		NCoins:      2,
		CoinBalances: []CoinBalance{
			{Address: common.HexToAddress("0xbbbb"), Balance: "50000000000", Decimals: 6, Symbol: "USDC", TokenID: 3},
			{Address: common.HexToAddress("0xcccc"), Balance: "50000000000000000000000", Decimals: 18, Symbol: "USDS", TokenID: 13},
		},
		TotalSupply:  big.NewInt(100000000),
		VirtualPrice: big.NewInt(1000000000000000000),
		Fee:          big.NewInt(100000),
		AmpFactor:    10000,
		OraclePrices: []OraclePrice{{Index: 0, Price: "1.000000000000000000"}},
	}

	feeAPY := 1.33
	apyData := map[common.Address]*curveapi.PoolAPY{
		snap.PoolAddress: {FeeAPY: feeAPY},
	}

	e, err := svc.buildEntity(snap, apyData, time.Now())
	if err != nil {
		t.Fatalf("buildEntity failed: %v", err)
	}

	// TVL should be populated
	if e.TvlUSD == nil {
		t.Error("expected tvl_usd to be set")
	}

	// Fee APY should be populated
	if e.FeeAPY == nil {
		t.Error("expected fee_apy to be set")
	}

	// CRV APY should be nil (not provided)
	if e.CrvAPYMin != nil || e.CrvAPYMax != nil {
		t.Error("expected crv_apy to be nil when not provided")
	}

	// Coin balances should include token_id
	var coins []CoinBalance
	if err := json.Unmarshal(e.CoinBalances, &coins); err != nil {
		t.Fatalf("unmarshal coin balances: %v", err)
	}
	if len(coins) != 2 {
		t.Fatalf("expected 2 coins, got %d", len(coins))
	}
	if coins[0].TokenID != 3 {
		t.Errorf("coin 0 token_id = %d, want 3", coins[0].TokenID)
	}
	if coins[1].TokenID != 13 {
		t.Errorf("coin 1 token_id = %d, want 13", coins[1].TokenID)
	}
}

// ── Entity Validation ──

func TestCurvePoolSnapshot_Validate(t *testing.T) {
	tests := []struct {
		name    string
		snap    entity.CurvePoolSnapshot
		wantErr bool
	}{
		{
			name: "valid",
			snap: entity.CurvePoolSnapshot{
				PoolAddress: []byte{0x01},
				ChainID:     1,
				BlockNumber: 100,
				NCoins:      2,
			},
			wantErr: false,
		},
		{
			name: "missing pool address",
			snap: entity.CurvePoolSnapshot{
				ChainID:     1,
				BlockNumber: 100,
				NCoins:      2,
			},
			wantErr: true,
		},
		{
			name: "missing chain ID",
			snap: entity.CurvePoolSnapshot{
				PoolAddress: []byte{0x01},
				BlockNumber: 100,
				NCoins:      2,
			},
			wantErr: true,
		},
		{
			name: "missing block number",
			snap: entity.CurvePoolSnapshot{
				PoolAddress: []byte{0x01},
				ChainID:     1,
				NCoins:      2,
			},
			wantErr: true,
		},
		{
			name: "missing n_coins",
			snap: entity.CurvePoolSnapshot{
				PoolAddress: []byte{0x01},
				ChainID:     1,
				BlockNumber: 100,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.snap.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ── Snapshot Struct ──

func TestPoolSnapshot_CoinBalances(t *testing.T) {
	snap := &PoolSnapshot{
		PoolAddress: common.HexToAddress("0xaaaa"),
		ChainID:     1,
		BlockNumber: 24720000,
		NCoins:      2,
		CoinBalances: []CoinBalance{
			{
				Address:  common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"),
				Balance:  "11732289241147089339012420",
				Decimals: 18,
				Symbol:   "sUSDS",
				TokenID:  12,
			},
			{
				Address:  common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"),
				Balance:  "37214492547797",
				Decimals: 6,
				Symbol:   "USDT",
				TokenID:  9,
			},
		},
	}

	if len(snap.CoinBalances) != 2 {
		t.Fatalf("expected 2 coin balances, got %d", len(snap.CoinBalances))
	}

	if snap.CoinBalances[0].Symbol != "sUSDS" {
		t.Errorf("expected sUSDS, got %s", snap.CoinBalances[0].Symbol)
	}
	if snap.CoinBalances[0].TokenID != 12 {
		t.Errorf("expected token_id 12, got %d", snap.CoinBalances[0].TokenID)
	}
	if snap.CoinBalances[1].Symbol != "USDT" {
		t.Errorf("expected USDT, got %s", snap.CoinBalances[1].Symbol)
	}
	if snap.CoinBalances[1].TokenID != 9 {
		t.Errorf("expected token_id 9, got %d", snap.CoinBalances[1].TokenID)
	}
}
