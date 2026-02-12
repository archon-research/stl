package blockchain

import (
	"context"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// mockMulticaller implements outbound.Multicaller for testing.
type mockMulticaller struct {
	executeFn func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error)
}

func (m *mockMulticaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	return m.executeFn(ctx, calls, blockNumber)
}

func (m *mockMulticaller) Address() common.Address {
	return common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")
}

// testOracleABI loads the oracle ABI. It calls t.Fatal on error.
func testOracleABI(t *testing.T) *abi.ABI {
	t.Helper()
	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		t.Fatalf("loading oracle ABI: %v", err)
	}
	return oracleABI
}

// abiPackPrices packs a slice of *big.Int as the return data for getAssetsPrices.
func abiPackPrices(t *testing.T, prices []*big.Int) []byte {
	return testutil.PackAssetPrices(t, prices)
}

func TestFetchOraclePrices(t *testing.T) {
	oracleABI := testOracleABI(t)

	oracleAddr := common.HexToAddress("0x0000000000000000000000000000000000000002")

	asset1 := common.HexToAddress("0x0000000000000000000000000000000000000010")
	asset2 := common.HexToAddress("0x0000000000000000000000000000000000000020")
	assets := []common.Address{asset1, asset2}

	price1 := big.NewInt(100000000)    // 1.00 USD
	price2 := big.NewInt(250000000000) // 2500.00 USD
	expectedPrices := []*big.Int{price1, price2}

	blockNum := int64(12345678)

	tests := []struct {
		name        string
		ctx         context.Context
		mock        *mockMulticaller
		wantErr     bool
		errContains string
		wantPrices  []*big.Int
	}{
		{
			name: "happy path - prices returned",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) != 1 {
						t.Fatalf("expected 1 call, got %d", len(calls))
					}
					return []outbound.Result{
						{Success: true, ReturnData: abiPackPrices(t, expectedPrices)},
					}, nil
				},
			},
			wantErr:    false,
			wantPrices: expectedPrices,
		},
		{
			name: "multicall execution error",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("rpc connection refused")
				},
			},
			wantErr:     true,
			errContains: "executing multicall at block 12345678",
		},
		{
			name: "wrong number of results - zero results",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{}, nil
				},
			},
			wantErr:     true,
			errContains: "expected 1 multicall result, got 0",
		},
		{
			name: "wrong number of results - two results",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: abiPackPrices(t, expectedPrices)},
						{Success: true, ReturnData: nil},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "expected 1 multicall result, got 2",
		},
		{
			name: "getAssetsPrices call failed - Success false",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: false, ReturnData: nil},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "getAssetsPrices call failed at block 12345678",
		},
		{
			name: "unpack getAssetsPrices error - bad return data",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: []byte{0xde, 0xad, 0xbe, 0xef}},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "unpacking getAssetsPrices at block 12345678",
		},
		{
			name: "context cancellation propagated to multicall",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			mock: &mockMulticaller{
				executeFn: func(ctx context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, ctx.Err()
				},
			},
			wantErr:     true,
			errContains: "executing multicall at block 12345678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prices, err := FetchOraclePrices(
				tt.ctx,
				tt.mock,
				oracleABI,
				oracleAddr,
				assets,
				blockNum,
			)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				if prices != nil {
					t.Errorf("expected nil result on error, got %+v", prices)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if prices == nil {
				t.Fatal("expected non-nil result, got nil")
			}
			if len(prices) != len(tt.wantPrices) {
				t.Fatalf("Prices length = %d, want %d", len(prices), len(tt.wantPrices))
			}
			for i, got := range prices {
				if got.Cmp(tt.wantPrices[i]) != 0 {
					t.Errorf("Prices[%d] = %s, want %s", i, got.String(), tt.wantPrices[i].String())
				}
			}
		})
	}
}

func TestFetchOraclePrices_VerifiesCallTargets(t *testing.T) {
	oracleABI := testOracleABI(t)

	oracleAddr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	assets := []common.Address{common.HexToAddress("0xCC")}
	prices := []*big.Int{big.NewInt(42)}

	mock := &mockMulticaller{
		executeFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
			if len(calls) != 1 {
				t.Fatalf("expected 1 call, got %d", len(calls))
			}
			if calls[0].Target != oracleAddr {
				t.Errorf("call[0].Target = %v, want %v", calls[0].Target, oracleAddr)
			}
			if calls[0].AllowFailure {
				t.Error("call[0].AllowFailure = true, want false")
			}
			if blockNumber.Int64() != 99 {
				t.Errorf("blockNumber = %d, want 99", blockNumber.Int64())
			}
			return []outbound.Result{
				{Success: true, ReturnData: abiPackPrices(t, prices)},
			}, nil
		},
	}

	result, err := FetchOraclePrices(
		context.Background(), mock,
		oracleABI,
		oracleAddr,
		assets, 99,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 {
		t.Fatalf("expected 1 price, got %d", len(result))
	}
	if result[0].Cmp(prices[0]) != 0 {
		t.Errorf("price = %s, want %s", result[0].String(), prices[0].String())
	}
}

func TestFetchOraclePrices_EmptyAssets(t *testing.T) {
	oracleABI := testOracleABI(t)

	oracleAddr := common.HexToAddress("0x02")
	var emptyPrices []*big.Int

	mock := &mockMulticaller{
		executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			return []outbound.Result{
				{Success: true, ReturnData: abiPackPrices(t, emptyPrices)},
			}, nil
		},
	}

	result, err := FetchOraclePrices(
		context.Background(), mock,
		oracleABI,
		oracleAddr,
		[]common.Address{}, 100,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected 0 prices, got %d", len(result))
	}
}

func TestFetchOraclePricesIndividual(t *testing.T) {
	oracleABI := testOracleABI(t)

	oracleAddr := common.HexToAddress("0x0000000000000000000000000000000000000002")

	asset1 := common.HexToAddress("0x0000000000000000000000000000000000000010")
	asset2 := common.HexToAddress("0x0000000000000000000000000000000000000020")
	assets := []common.Address{asset1, asset2}

	price1 := big.NewInt(100000000)    // 1.00 USD
	price2 := big.NewInt(250000000000) // 2500.00 USD

	blockNum := int64(12345678)

	tests := []struct {
		name        string
		assets      []common.Address
		mock        *mockMulticaller
		wantErr     bool
		errContains string
		wantResults []AssetPriceResult
	}{
		{
			name:   "happy path - all tokens succeed",
			assets: assets,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) != 2 {
						t.Fatalf("expected 2 calls, got %d", len(calls))
					}
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackAssetPrice(t, price1)},
						{Success: true, ReturnData: testutil.PackAssetPrice(t, price2)},
					}, nil
				},
			},
			wantResults: []AssetPriceResult{
				{Price: price1, Success: true},
				{Price: price2, Success: true},
			},
		},
		{
			name:   "partial failure - second token fails",
			assets: assets,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackAssetPrice(t, price1)},
						{Success: false, ReturnData: nil},
					}, nil
				},
			},
			wantResults: []AssetPriceResult{
				{Price: price1, Success: true},
				{Success: false},
			},
		},
		{
			name:   "all tokens fail - no error returned",
			assets: assets,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: false, ReturnData: nil},
						{Success: false, ReturnData: nil},
					}, nil
				},
			},
			wantResults: []AssetPriceResult{
				{Success: false},
				{Success: false},
			},
		},
		{
			name:   "execute error - returns error",
			assets: assets,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("rpc connection refused")
				},
			},
			wantErr:     true,
			errContains: "executing multicall at block 12345678",
		},
		{
			name:   "result count mismatch - returns error",
			assets: assets,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackAssetPrice(t, price1)},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "expected 2 multicall results, got 1",
		},
		{
			name:   "bad return data - treated as failure",
			assets: assets,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackAssetPrice(t, price1)},
						{Success: true, ReturnData: []byte{0xde, 0xad}}, // bad data
					}, nil
				},
			},
			wantResults: []AssetPriceResult{
				{Price: price1, Success: true},
				{Success: false}, // bad data treated as failure
			},
		},
		{
			name:   "empty assets - returns nil",
			assets: []common.Address{},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					t.Fatal("Execute should not be called for empty assets")
					return nil, nil
				},
			},
			wantResults: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := FetchOraclePricesIndividual(
				context.Background(),
				tt.mock,
				oracleABI,
				oracleAddr,
				tt.assets,
				blockNum,
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

			if tt.wantResults == nil {
				if results != nil {
					t.Errorf("expected nil results, got %+v", results)
				}
				return
			}

			if len(results) != len(tt.wantResults) {
				t.Fatalf("results length = %d, want %d", len(results), len(tt.wantResults))
			}
			for i, got := range results {
				want := tt.wantResults[i]
				if got.Success != want.Success {
					t.Errorf("results[%d].Success = %v, want %v", i, got.Success, want.Success)
				}
				if want.Success {
					if got.Price == nil || got.Price.Cmp(want.Price) != 0 {
						t.Errorf("results[%d].Price = %v, want %v", i, got.Price, want.Price)
					}
				}
			}
		})
	}
}

func TestFetchOraclePricesIndividual_VerifiesCallTargets(t *testing.T) {
	oracleABI := testOracleABI(t)

	oracleAddr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	assets := []common.Address{
		common.HexToAddress("0xCC"),
		common.HexToAddress("0xDD"),
	}
	price := big.NewInt(42)

	mock := &mockMulticaller{
		executeFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
			if len(calls) != 2 {
				t.Fatalf("expected 2 calls, got %d", len(calls))
			}
			for i, call := range calls {
				if call.Target != oracleAddr {
					t.Errorf("call[%d].Target = %v, want %v", i, call.Target, oracleAddr)
				}
				if !call.AllowFailure {
					t.Errorf("call[%d].AllowFailure = false, want true", i)
				}
			}
			if blockNumber.Int64() != 99 {
				t.Errorf("blockNumber = %d, want 99", blockNumber.Int64())
			}
			return []outbound.Result{
				{Success: true, ReturnData: testutil.PackAssetPrice(t, price)},
				{Success: true, ReturnData: testutil.PackAssetPrice(t, price)},
			}, nil
		},
	}

	results, err := FetchOraclePricesIndividual(
		context.Background(),
		mock,
		oracleABI,
		oracleAddr,
		assets,
		99,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	for i, r := range results {
		if !r.Success {
			t.Errorf("results[%d].Success = false, want true", i)
		}
		if r.Price.Cmp(price) != 0 {
			t.Errorf("results[%d].Price = %s, want %s", i, r.Price.String(), price.String())
		}
	}
}

func TestConvertOraclePriceToUSD(t *testing.T) {
	tests := []struct {
		name     string
		rawPrice *big.Int
		want     float64
	}{
		{
			name:     "nil input",
			rawPrice: nil,
			want:     0,
		},
		{
			name:     "zero value",
			rawPrice: big.NewInt(0),
			want:     0,
		},
		{
			name:     "1e8 equals 1 USD",
			rawPrice: big.NewInt(100000000),
			want:     1.0,
		},
		{
			name:     "250000000000 equals 2500 USD",
			rawPrice: big.NewInt(250000000000),
			want:     2500.0,
		},
		{
			name:     "smallest unit 1 equals 0.00000001",
			rawPrice: big.NewInt(1),
			want:     0.00000001,
		},
		{
			name:     "fractional USD - 50 cents",
			rawPrice: big.NewInt(50000000),
			want:     0.5,
		},
		{
			name:     "large price - 100000 USD (e.g. BTC)",
			rawPrice: big.NewInt(10000000000000),
			want:     100000.0,
		},
		{
			name: "very large number - 1e30 raw",
			rawPrice: func() *big.Int {
				v, _ := new(big.Int).SetString("1000000000000000000000000000000", 10)
				return v
			}(),
			want: 1e22,
		},
		{
			name:     "negative value - edge case",
			rawPrice: big.NewInt(-100000000),
			want:     -1.0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ConvertOraclePriceToUSD(tt.rawPrice, 8)
			if !floatEquals(got, tt.want) {
				t.Errorf("ConvertOraclePriceToUSD(%v, 8) = %v, want %v", tt.rawPrice, got, tt.want)
			}
		})
	}
}

// floatEquals compares two float64 values with a relative tolerance.
func floatEquals(a, b float64) bool {
	if a == b {
		return true
	}
	diff := math.Abs(a - b)
	largest := math.Max(math.Abs(a), math.Abs(b))
	return diff <= largest*1e-9
}
