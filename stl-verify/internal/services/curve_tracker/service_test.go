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
	return 0, outbound.ErrTokenNotFound
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

	seen := make(map[string]struct{})
	for _, p := range pools {
		want, ok := expected[p.Name]
		if !ok {
			t.Errorf("unexpected pool name: %s", p.Name)
			continue
		}
		if _, dup := seen[p.Name]; dup {
			t.Errorf("duplicate pool name: %s", p.Name)
		}
		seen[p.Name] = struct{}{}
		if p.Address != common.HexToAddress(want) {
			t.Errorf("pool %s: address = %s, want %s", p.Name, p.Address.Hex(), want)
		}
		if p.ChainID != 1 {
			t.Errorf("pool %s: chainID = %d, want 1", p.Name, p.ChainID)
		}
	}
	for name := range expected {
		if _, ok := seen[name]; !ok {
			t.Errorf("expected pool %s not found in DefaultPools()", name)
		}
	}
}

// ── PoolsForChainID ──

func TestPoolsForChainID(t *testing.T) {
	pools := []PoolConfig{
		{Address: common.HexToAddress("0xaaa"), ChainID: 1, Name: "eth-pool"},
		{Address: common.HexToAddress("0xbbb"), ChainID: 42161, Name: "arb-pool"},
		{Address: common.HexToAddress("0xccc"), ChainID: 1, Name: "eth-pool-2"},
	}

	ethPools := PoolsForChainID(pools, 1)
	if len(ethPools) != 2 {
		t.Fatalf("expected 2 Ethereum pools, got %d", len(ethPools))
	}

	arbPools := PoolsForChainID(pools, 42161)
	if len(arbPools) != 1 {
		t.Fatalf("expected 1 Arbitrum pool, got %d", len(arbPools))
	}

	noPools := PoolsForChainID(pools, 999)
	if len(noPools) != 0 {
		t.Fatalf("expected 0 pools for chain 999, got %d", len(noPools))
	}
}

// ── TVL Calculation ──

func TestCalculateTVL_StablecoinPool(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		CoinBalances: []CoinBalance{
			{Balance: "50000000000000", Decimals: 6, Symbol: "USDC"},
			{Balance: "50000000000000000000000000", Decimals: 18, Symbol: "USDS"},
		},
	}

	tvl := svc.calculateTVL(snap)
	if tvl == nil {
		t.Fatal("expected non-nil TVL")
	}

	tvlFloat, _ := tvl.Float64()
	if tvlFloat < 99_000_000 || tvlFloat > 101_000_000 {
		t.Errorf("expected TVL ~100M, got %.2f", tvlFloat)
	}
}

func TestCalculateTVL_sUSDSUSDT(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		CoinBalances: []CoinBalance{
			{Balance: "8108953488903302589788809", Decimals: 18, Symbol: "sUSDS"},
			{Balance: "37214492547797", Decimals: 6, Symbol: "USDT"},
		},
	}

	tvl := svc.calculateTVL(snap)
	if tvl == nil {
		t.Fatal("expected non-nil TVL")
	}

	tvlFloat, _ := tvl.Float64()
	if tvlFloat < 44_000_000 || tvlFloat > 47_000_000 {
		t.Errorf("expected TVL ~45M, got %.2f", tvlFloat)
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
			{Balance: "1000000", Decimals: 0, Symbol: "UNKNOWN"},
			{Balance: "50000000000", Decimals: 6, Symbol: "USDC"},
		},
	}

	tvl := svc.calculateTVL(snap)
	if tvl == nil {
		t.Fatal("expected non-nil TVL")
	}

	tvlFloat, _ := tvl.Float64()
	if tvlFloat < 49_000 || tvlFloat > 51_000 {
		t.Errorf("expected TVL ~50K, got %.2f", tvlFloat)
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
	unknown := common.HexToAddress("0x000000000000000000000000000000000000dead")

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
		PoolAddress: common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10"),
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

	feeDaily := 1.25
	feeWeekly := 1.33
	apyData := map[common.Address]*outbound.APYData{
		snap.PoolAddress: {FeeAPYDaily: &feeDaily, FeeAPYWeekly: &feeWeekly},
	}

	blockTime := time.Date(2026, 3, 25, 21, 0, 0, 0, time.UTC)
	e, err := svc.buildEntity(snap, apyData, blockTime)
	if err != nil {
		t.Fatalf("buildEntity failed: %v", err)
	}

	if e.TvlUSD == nil {
		t.Error("expected tvl_usd to be set")
	}
	if e.FeeAPYDaily == nil {
		t.Error("expected fee_apy_daily to be set")
	}
	if e.FeeAPYWeekly == nil {
		t.Error("expected fee_apy_weekly to be set")
	}
	if e.CrvAPYMin != nil || e.CrvAPYMax != nil {
		t.Error("expected crv_apy to be nil when not provided")
	}
	if e.SnapshotTime != blockTime {
		t.Errorf("expected snapshot_time = %v, got %v", blockTime, e.SnapshotTime)
	}

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

func TestBuildEntity_NilAPY_DoesNotCoerceToZero(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		PoolAddress:  common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10"),
		ChainID:      1,
		BlockNumber:  24720000,
		NCoins:       2,
		CoinBalances: []CoinBalance{{Balance: "1000000", Decimals: 6}},
		TotalSupply:  big.NewInt(1000000),
		VirtualPrice: big.NewInt(1000000000000000000),
		Fee:          big.NewInt(100000),
	}

	blockTime := time.Date(2026, 3, 25, 21, 0, 0, 0, time.UTC)
	e, err := svc.buildEntity(snap, nil, blockTime)
	if err != nil {
		t.Fatalf("buildEntity failed: %v", err)
	}

	if e.FeeAPYDaily != nil {
		t.Errorf("expected nil FeeAPYDaily when no APY data, got %v", *e.FeeAPYDaily)
	}
	if e.FeeAPYWeekly != nil {
		t.Errorf("expected nil FeeAPYWeekly when no APY data, got %v", *e.FeeAPYWeekly)
	}
	if e.CrvAPYMin != nil {
		t.Errorf("expected nil CrvAPYMin, got %v", *e.CrvAPYMin)
	}
	if e.CrvAPYMax != nil {
		t.Errorf("expected nil CrvAPYMax, got %v", *e.CrvAPYMax)
	}
}

func TestBuildEntity_CallsValidate(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		PoolAddress:  common.Address{}, // invalid — 0x0 produces 20 zero bytes but ChainID=0
		ChainID:      0,
		BlockNumber:  0,
		NCoins:       0,
		TotalSupply:  big.NewInt(1),
		VirtualPrice: big.NewInt(1),
		Fee:          big.NewInt(1),
	}

	blockTime := time.Date(2026, 3, 25, 21, 0, 0, 0, time.UTC)
	_, err := svc.buildEntity(snap, nil, blockTime)
	if err == nil {
		t.Fatal("expected buildEntity to fail on invalid entity")
	}
}

// ── Entity Validation ──

func TestCurvePoolSnapshot_Validate(t *testing.T) {
	validAddr := common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10").Bytes()
	now := time.Now()

	tests := []struct {
		name    string
		snap    entity.CurvePoolSnapshot
		wantErr bool
	}{
		{
			name: "valid",
			snap: entity.CurvePoolSnapshot{
				PoolAddress:  validAddr,
				ChainID:      1,
				BlockNumber:  100,
				NCoins:       2,
				SnapshotTime: now,
			},
			wantErr: false,
		},
		{
			name: "invalid pool address length",
			snap: entity.CurvePoolSnapshot{
				PoolAddress:  []byte{0x01},
				ChainID:      1,
				BlockNumber:  100,
				NCoins:       2,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "missing pool address",
			snap: entity.CurvePoolSnapshot{
				ChainID:      1,
				BlockNumber:  100,
				NCoins:       2,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "missing chain ID",
			snap: entity.CurvePoolSnapshot{
				PoolAddress:  validAddr,
				BlockNumber:  100,
				NCoins:       2,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "missing block number",
			snap: entity.CurvePoolSnapshot{
				PoolAddress:  validAddr,
				ChainID:      1,
				NCoins:       2,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "missing n_coins",
			snap: entity.CurvePoolSnapshot{
				PoolAddress:  validAddr,
				ChainID:      1,
				BlockNumber:  100,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "zero snapshot_time",
			snap: entity.CurvePoolSnapshot{
				PoolAddress: validAddr,
				ChainID:     1,
				BlockNumber: 100,
				NCoins:      2,
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
