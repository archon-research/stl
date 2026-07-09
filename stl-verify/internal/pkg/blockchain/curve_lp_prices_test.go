package blockchain

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockHashMulticaller implements outbound.HashPinnedMulticaller for testing.
type mockHashMulticaller struct {
	executeAtHashFn func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error)
}

func (m *mockHashMulticaller) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	if m.executeAtHashFn != nil {
		return m.executeAtHashFn(ctx, calls, blockHash)
	}
	return nil, errors.New("ExecuteAtHash not mocked")
}

func curveNGPoolABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsed, err := abis.GetCurveNGPoolABI()
	if err != nil {
		t.Fatalf("loading Curve NG pool ABI: %v", err)
	}
	return parsed
}

func packVirtualPrice(t *testing.T, price *big.Int) []byte {
	t.Helper()
	data, err := curveNGPoolABI(t).Methods["get_virtual_price"].Outputs.Pack(price)
	if err != nil {
		t.Fatalf("packing get_virtual_price return: %v", err)
	}
	return data
}

func TestFetchCurveLPNGPrices(t *testing.T) {
	poolABI := curveNGPoolABI(t)
	fABI := feedABI(t)
	blockNum := int64(22_800_000)
	blockHash := common.HexToHash("0xc0ffee")

	poolAddr := common.HexToAddress("0xE79c1C7E24755574438A26D5e062AD2626c04662")
	usdcFeed := common.HexToAddress("0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6")
	ausdFeed := common.HexToAddress("0xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD")

	pool := CurveLPNGPoolConfig{
		TokenID:     587717,
		PoolAddress: poolAddr,
		CoinFeeds: []FeedConfig{
			{TokenID: 587717, FeedAddress: usdcFeed, FeedDecimals: 8, QuoteCurrency: "USD"},
			{TokenID: 587717, FeedAddress: ausdFeed, FeedDecimals: 18, QuoteCurrency: "USD"},
		},
	}

	// virtual_price 1.25, USDC 0.75 (8 dec), AUSD 1.5 (18 dec) → min 0.75 → $0.9375.
	// All values are exact float64s so the equality assert is stable.
	vp125 := big.NewInt(1_250_000_000_000_000_000)
	usdc075 := big.NewInt(75_000_000)
	ausd150 := big.NewInt(1_500_000_000_000_000_000)

	tests := []struct {
		name        string
		pool        CurveLPNGPoolConfig
		blockHash   common.Hash
		mock        *mockHashMulticaller
		wantErr     bool
		errContains string
		wantPrice   float64
	}{
		{
			name:      "happy path - virtual price times cheapest coin",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, calls []outbound.Call, gotHash common.Hash) ([]outbound.Result, error) {
					if gotHash != blockHash {
						t.Errorf("blockHash = %s, want %s", gotHash, blockHash)
					}
					if len(calls) != 3 {
						t.Fatalf("expected 3 calls (get_virtual_price + 2 latestRoundData), got %d", len(calls))
					}
					if calls[0].Target != poolAddr {
						t.Errorf("call[0] target = %s, want pool %s", calls[0].Target, poolAddr)
					}
					if calls[1].Target != usdcFeed {
						t.Errorf("call[1] target = %s, want feed %s", calls[1].Target, usdcFeed)
					}
					if calls[2].Target != ausdFeed {
						t.Errorf("call[2] target = %s, want feed %s", calls[2].Target, ausdFeed)
					}
					return []outbound.Result{
						{Success: true, ReturnData: packVirtualPrice(t, vp125)},
						{Success: true, ReturnData: packRoundData(t, usdc075, big.NewInt(1000))},
						{Success: true, ReturnData: packRoundData(t, ausd150, big.NewInt(1000))},
					}, nil
				},
			},
			wantPrice: 0.9375,
		},
		{
			name:      "min picks the other feed when order flips",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					// USDC 1.5, AUSD 0.75 → min still 0.75 regardless of position.
					return []outbound.Result{
						{Success: true, ReturnData: packVirtualPrice(t, vp125)},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(150_000_000), big.NewInt(1000))},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(750_000_000_000_000_000), big.NewInt(1000))},
					}, nil
				},
			},
			wantPrice: 0.9375,
		},
		{
			name:      "get_virtual_price reverts returns error",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: false},
						{Success: true, ReturnData: packRoundData(t, usdc075, big.NewInt(1000))},
						{Success: true, ReturnData: packRoundData(t, ausd150, big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "get_virtual_price call failed",
		},
		{
			name:      "zero virtual price returns error",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: packVirtualPrice(t, big.NewInt(0))},
						{Success: true, ReturnData: packRoundData(t, usdc075, big.NewInt(1000))},
						{Success: true, ReturnData: packRoundData(t, ausd150, big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "non-positive virtual price",
		},
		{
			name:      "one coin feed reverts returns error",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					// The failed feed could be the depegged one, so pricing from the
					// remaining feeds would overstate the LP value; must error.
					return []outbound.Result{
						{Success: true, ReturnData: packVirtualPrice(t, vp125)},
						{Success: true, ReturnData: packRoundData(t, usdc075, big.NewInt(1000))},
						{Success: false},
					}, nil
				},
			},
			wantErr: true, errContains: "coin feed",
		},
		{
			name:      "non-positive feed answer returns error",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: packVirtualPrice(t, vp125)},
						{Success: true, ReturnData: packRoundData(t, big.NewInt(-1), big.NewInt(1000))},
						{Success: true, ReturnData: packRoundData(t, ausd150, big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "non-positive answer",
		},
		{
			name:      "multicall error returns error",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return nil, errors.New("RPC down")
				},
			},
			wantErr: true, errContains: "executing multicall at block",
		},
		{
			name:      "result count mismatch returns error",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{{Success: true, ReturnData: packVirtualPrice(t, vp125)}}, nil
				},
			},
			wantErr: true, errContains: "expected 3 multicall results",
		},
		{
			name:      "virtual price unpack error returns error",
			pool:      pool,
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: []byte{0xde, 0xad}},
						{Success: true, ReturnData: packRoundData(t, usdc075, big.NewInt(1000))},
						{Success: true, ReturnData: packRoundData(t, ausd150, big.NewInt(1000))},
					}, nil
				},
			},
			wantErr: true, errContains: "unpacking get_virtual_price",
		},
		{
			name: "fewer than two coin feeds returns error",
			pool: CurveLPNGPoolConfig{
				TokenID:     587717,
				PoolAddress: poolAddr,
				CoinFeeds: []FeedConfig{
					{TokenID: 587717, FeedAddress: usdcFeed, FeedDecimals: 8, QuoteCurrency: "USD"},
				},
			},
			blockHash: blockHash,
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					t.Fatal("ExecuteAtHash should not be called")
					return nil, nil
				},
			},
			wantErr: true, errContains: "at least 2 coin feeds",
		},
		{
			name:      "zero block hash returns error",
			pool:      pool,
			blockHash: common.Hash{},
			mock: &mockHashMulticaller{
				executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
					t.Fatal("ExecuteAtHash should not be called")
					return nil, nil
				},
			},
			wantErr: true, errContains: "block hash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := FetchCurveLPNGPrices(
				context.Background(),
				tt.mock,
				poolABI,
				fABI,
				tt.pool,
				blockNum,
				tt.blockHash,
			)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(results) != 1 {
				t.Fatalf("results len = %d, want 1 (one LP token per pool)", len(results))
			}
			got := results[0]
			if got.TokenID != tt.pool.TokenID {
				t.Errorf("TokenID = %d, want %d", got.TokenID, tt.pool.TokenID)
			}
			if !got.Success {
				t.Error("Success = false, want true")
			}
			if got.Price != tt.wantPrice {
				t.Errorf("Price = %v, want %v", got.Price, tt.wantPrice)
			}
		})
	}
}

// TestFetchCurveLPNGPrices_RealisticAUSDUSDCValues sanity-checks the formula
// with the live values recorded in the design doc: virtual_price ~1.0006 and
// both stables near peg should price the LP a hair above the cheapest dollar.
func TestFetchCurveLPNGPrices_RealisticAUSDUSDCValues(t *testing.T) {
	poolABI := curveNGPoolABI(t)
	fABI := feedABI(t)
	blockHash := common.HexToHash("0xc0ffee")

	pool := CurveLPNGPoolConfig{
		TokenID:     587717,
		PoolAddress: common.HexToAddress("0xE79c1C7E24755574438A26D5e062AD2626c04662"),
		CoinFeeds: []FeedConfig{
			{TokenID: 587717, FeedAddress: common.HexToAddress("0x8fFfFfd4AfB6115b954Bd326cbe7B4BA576818f6"), FeedDecimals: 8, QuoteCurrency: "USD"},
			{TokenID: 587717, FeedAddress: common.HexToAddress("0xB00341502DfEA6Ced8A5786b4059d29dA5E4D1FD"), FeedDecimals: 18, QuoteCurrency: "USD"},
		},
	}

	vp, ok := new(big.Int).SetString("1000600000000000000", 10) // 1.0006
	if !ok {
		t.Fatal("parsing virtual price")
	}
	ausd, ok := new(big.Int).SetString("999750000000000000", 10) // 0.99975, 18 dec
	if !ok {
		t.Fatal("parsing AUSD answer")
	}

	mock := &mockHashMulticaller{
		executeAtHashFn: func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
			return []outbound.Result{
				{Success: true, ReturnData: packVirtualPrice(t, vp)},
				{Success: true, ReturnData: packRoundData(t, big.NewInt(99_998_000), big.NewInt(1000))}, // USDC 0.99998
				{Success: true, ReturnData: packRoundData(t, ausd, big.NewInt(1000))},
			}, nil
		},
	}

	results, err := FetchCurveLPNGPrices(context.Background(), mock, poolABI, fABI, pool, 22_800_000, blockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results len = %d, want 1", len(results))
	}
	want := 1.0006 * 0.99975 // ~1.00035, min is the AUSD side
	if diff := results[0].Price - want; diff > 1e-12 || diff < -1e-12 {
		t.Errorf("Price = %.12f, want %.12f", results[0].Price, want)
	}
}
