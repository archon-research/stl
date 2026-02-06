package blockchain

import (
	"context"
	"errors"
	"math"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// mockMulticaller implements outbound.Multicaller for testing.
type mockMulticaller struct {
	// executeFn is called for each Execute invocation. The callIndex tracks
	// how many times Execute has been called so tests can return different
	// results for the initial call vs. the retry call.
	executeFn func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int, callIndex int) ([]outbound.Result, error)
	callIndex int
}

func (m *mockMulticaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	idx := m.callIndex
	m.callIndex++
	return m.executeFn(ctx, calls, blockNumber, idx)
}

func (m *mockMulticaller) Address() common.Address {
	return common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")
}

// testABIs loads the real provider and oracle ABIs. It calls t.Fatal on error.
func testABIs(t *testing.T) (*abi.ABI, *abi.ABI) {
	t.Helper()
	providerABI, err := abis.GetPoolAddressProviderABI()
	if err != nil {
		t.Fatalf("loading provider ABI: %v", err)
	}
	oracleABI, err := abis.GetSparkLendOracleABI()
	if err != nil {
		t.Fatalf("loading oracle ABI: %v", err)
	}
	return providerABI, oracleABI
}

// abiPackAddress packs an address as the return data for getPriceOracle.
// The providerABI parameter is accepted for call-site compatibility but unused;
// the ABI is loaded internally by testutil.
func abiPackAddress(t *testing.T, _ *abi.ABI, addr common.Address) []byte {
	return testutil.PackOracleAddress(t, addr)
}

// abiPackPrices packs a slice of *big.Int as the return data for getAssetsPrices.
// The oracleABI parameter is accepted for call-site compatibility but unused;
// the ABI is loaded internally by testutil.
func abiPackPrices(t *testing.T, _ *abi.ABI, prices []*big.Int) []byte {
	return testutil.PackAssetPrices(t, prices)
}

func TestFetchOraclePrices(t *testing.T) {
	providerABI, oracleABI := testABIs(t)

	providerAddr := common.HexToAddress("0x0000000000000000000000000000000000000001")
	cachedOracleAddr := common.HexToAddress("0x0000000000000000000000000000000000000002")
	differentOracleAddr := common.HexToAddress("0x0000000000000000000000000000000000000003")

	asset1 := common.HexToAddress("0x0000000000000000000000000000000000000010")
	asset2 := common.HexToAddress("0x0000000000000000000000000000000000000020")
	assets := []common.Address{asset1, asset2}

	price1 := big.NewInt(100000000)    // 1.00 USD
	price2 := big.NewInt(250000000000) // 2500.00 USD
	expectedPrices := []*big.Int{price1, price2}

	blockNum := int64(12345678)

	tests := []struct {
		name           string
		ctx            context.Context
		mock           *mockMulticaller
		wantErr        bool
		errContains    string
		wantOracleAddr common.Address
		wantPrices     []*big.Int
	}{
		{
			name: "happy path - oracle address matches cache",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					if len(calls) != 2 {
						t.Fatalf("expected 2 calls, got %d", len(calls))
					}
					return []outbound.Result{
						{Success: true, ReturnData: abiPackAddress(t, providerABI, cachedOracleAddr)},
						{Success: true, ReturnData: abiPackPrices(t, oracleABI, expectedPrices)},
					}, nil
				},
			},
			wantErr:        false,
			wantOracleAddr: cachedOracleAddr,
			wantPrices:     expectedPrices,
		},
		{
			name: "oracle address changed - retry path succeeds",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int, callIndex int) ([]outbound.Result, error) {
					if callIndex == 0 {
						// First call: return different oracle address, prices call fails
						return []outbound.Result{
							{Success: true, ReturnData: abiPackAddress(t, providerABI, differentOracleAddr)},
							{Success: false, ReturnData: nil},
						}, nil
					}
					// Retry call: one call to the new oracle address
					if len(calls) != 1 {
						t.Fatalf("retry expected 1 call, got %d", len(calls))
					}
					if calls[0].Target != differentOracleAddr {
						t.Fatalf("retry target = %v, want %v", calls[0].Target, differentOracleAddr)
					}
					return []outbound.Result{
						{Success: true, ReturnData: abiPackPrices(t, oracleABI, expectedPrices)},
					}, nil
				},
			},
			wantErr:        false,
			wantOracleAddr: differentOracleAddr,
			wantPrices:     expectedPrices,
		},
		{
			name: "oracle address matches but prices call failed - retry path",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, callIndex int) ([]outbound.Result, error) {
					if callIndex == 0 {
						// Oracle address matches cache, but prices call failed
						return []outbound.Result{
							{Success: true, ReturnData: abiPackAddress(t, providerABI, cachedOracleAddr)},
							{Success: false, ReturnData: nil},
						}, nil
					}
					// Retry with same cached address
					return []outbound.Result{
						{Success: true, ReturnData: abiPackPrices(t, oracleABI, expectedPrices)},
					}, nil
				},
			},
			wantErr:        false,
			wantOracleAddr: cachedOracleAddr,
			wantPrices:     expectedPrices,
		},
		{
			name: "multicall execution error on first call",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
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
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					return []outbound.Result{}, nil
				},
			},
			wantErr:     true,
			errContains: "expected 2 multicall results, got 0",
		},
		{
			name: "wrong number of results - one result",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: abiPackAddress(t, providerABI, cachedOracleAddr)},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "expected 2 multicall results, got 1",
		},
		{
			name: "wrong number of results - three results",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: abiPackAddress(t, providerABI, cachedOracleAddr)},
						{Success: true, ReturnData: abiPackPrices(t, oracleABI, expectedPrices)},
						{Success: true, ReturnData: nil},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "expected 2 multicall results, got 3",
		},
		{
			name: "getPriceOracle call failed - Success false",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: false, ReturnData: nil},
						{Success: true, ReturnData: abiPackPrices(t, oracleABI, expectedPrices)},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "getPriceOracle call failed at block 12345678",
		},
		{
			name: "unpack getPriceOracle error - bad return data",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: []byte{0x01, 0x02, 0x03}}, // invalid ABI data
						{Success: true, ReturnData: abiPackPrices(t, oracleABI, expectedPrices)},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "unpacking getPriceOracle at block 12345678",
		},
		{
			name: "unpack getAssetsPrices error - bad return data in cache hit path",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: abiPackAddress(t, providerABI, cachedOracleAddr)},
						{Success: true, ReturnData: []byte{0xde, 0xad, 0xbe, 0xef}}, // invalid ABI data
					}, nil
				},
			},
			wantErr:     true,
			errContains: "unpacking getAssetsPrices at block 12345678",
		},
		{
			name: "oracle address changed - retry multicall execution error",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, callIndex int) ([]outbound.Result, error) {
					if callIndex == 0 {
						return []outbound.Result{
							{Success: true, ReturnData: abiPackAddress(t, providerABI, differentOracleAddr)},
							{Success: false, ReturnData: nil},
						}, nil
					}
					return nil, errors.New("retry rpc timeout")
				},
			},
			wantErr:     true,
			errContains: "executing retry multicall at block 12345678",
		},
		{
			name: "oracle address changed - retry returns wrong count",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, callIndex int) ([]outbound.Result, error) {
					if callIndex == 0 {
						return []outbound.Result{
							{Success: true, ReturnData: abiPackAddress(t, providerABI, differentOracleAddr)},
							{Success: false, ReturnData: nil},
						}, nil
					}
					// Return empty results for retry
					return []outbound.Result{}, nil
				},
			},
			wantErr:     true,
			errContains: "getAssetsPrices retry failed at block 12345678",
		},
		{
			name: "oracle address changed - retry returns failure",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, callIndex int) ([]outbound.Result, error) {
					if callIndex == 0 {
						return []outbound.Result{
							{Success: true, ReturnData: abiPackAddress(t, providerABI, differentOracleAddr)},
							{Success: false, ReturnData: nil},
						}, nil
					}
					return []outbound.Result{
						{Success: false, ReturnData: nil},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "getAssetsPrices retry failed at block 12345678",
		},
		{
			name: "oracle address changed - retry unpack error",
			ctx:  context.Background(),
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, callIndex int) ([]outbound.Result, error) {
					if callIndex == 0 {
						return []outbound.Result{
							{Success: true, ReturnData: abiPackAddress(t, providerABI, differentOracleAddr)},
							{Success: false, ReturnData: nil},
						}, nil
					}
					return []outbound.Result{
						{Success: true, ReturnData: []byte{0xff, 0xfe}}, // invalid ABI data
					}, nil
				},
			},
			wantErr:     true,
			errContains: "unpacking retry getAssetsPrices at block 12345678",
		},
		{
			name: "context cancellation propagated to multicall",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			}(),
			mock: &mockMulticaller{
				executeFn: func(ctx context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
					return nil, ctx.Err()
				},
			},
			wantErr:     true,
			errContains: "executing multicall at block 12345678",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := FetchOraclePrices(
				tt.ctx,
				tt.mock,
				providerABI, oracleABI,
				providerAddr, cachedOracleAddr,
				assets,
				blockNum,
			)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if tt.errContains != "" && !containsSubstring(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				if result != nil {
					t.Errorf("expected nil result on error, got %+v", result)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result == nil {
				t.Fatal("expected non-nil result, got nil")
			}
			if result.OracleAddress != tt.wantOracleAddr {
				t.Errorf("OracleAddress = %v, want %v", result.OracleAddress, tt.wantOracleAddr)
			}
			if len(result.Prices) != len(tt.wantPrices) {
				t.Fatalf("Prices length = %d, want %d", len(result.Prices), len(tt.wantPrices))
			}
			for i, got := range result.Prices {
				if got.Cmp(tt.wantPrices[i]) != 0 {
					t.Errorf("Prices[%d] = %s, want %s", i, got.String(), tt.wantPrices[i].String())
				}
			}
		})
	}
}

func TestFetchOraclePrices_VerifiesCallTargets(t *testing.T) {
	providerABI, oracleABI := testABIs(t)

	providerAddr := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	cachedOracleAddr := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")
	assets := []common.Address{common.HexToAddress("0xCC")}
	prices := []*big.Int{big.NewInt(42)}

	mock := &mockMulticaller{
		executeFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int, _ int) ([]outbound.Result, error) {
			if len(calls) != 2 {
				t.Fatalf("expected 2 calls, got %d", len(calls))
			}
			if calls[0].Target != providerAddr {
				t.Errorf("call[0].Target = %v, want %v", calls[0].Target, providerAddr)
			}
			if calls[1].Target != cachedOracleAddr {
				t.Errorf("call[1].Target = %v, want %v", calls[1].Target, cachedOracleAddr)
			}
			if calls[0].AllowFailure {
				t.Error("call[0].AllowFailure = true, want false")
			}
			if calls[1].AllowFailure {
				t.Error("call[1].AllowFailure = true, want false")
			}
			if blockNumber.Int64() != 99 {
				t.Errorf("blockNumber = %d, want 99", blockNumber.Int64())
			}
			return []outbound.Result{
				{Success: true, ReturnData: abiPackAddress(t, providerABI, cachedOracleAddr)},
				{Success: true, ReturnData: abiPackPrices(t, oracleABI, prices)},
			}, nil
		},
	}

	result, err := FetchOraclePrices(
		context.Background(), mock,
		providerABI, oracleABI,
		providerAddr, cachedOracleAddr,
		assets, 99,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.OracleAddress != cachedOracleAddr {
		t.Errorf("OracleAddress = %v, want %v", result.OracleAddress, cachedOracleAddr)
	}
}

func TestFetchOraclePrices_EmptyAssets(t *testing.T) {
	providerABI, oracleABI := testABIs(t)

	providerAddr := common.HexToAddress("0x01")
	cachedOracleAddr := common.HexToAddress("0x02")
	var emptyPrices []*big.Int

	mock := &mockMulticaller{
		executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, _ int) ([]outbound.Result, error) {
			return []outbound.Result{
				{Success: true, ReturnData: abiPackAddress(t, providerABI, cachedOracleAddr)},
				{Success: true, ReturnData: abiPackPrices(t, oracleABI, emptyPrices)},
			}, nil
		},
	}

	result, err := FetchOraclePrices(
		context.Background(), mock,
		providerABI, oracleABI,
		providerAddr, cachedOracleAddr,
		[]common.Address{}, 100,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Prices) != 0 {
		t.Errorf("expected 0 prices, got %d", len(result.Prices))
	}
}

func TestFetchOraclePrices_RetryCallCount(t *testing.T) {
	providerABI, oracleABI := testABIs(t)

	providerAddr := common.HexToAddress("0x01")
	cachedOracleAddr := common.HexToAddress("0x02")
	newOracleAddr := common.HexToAddress("0x03")
	assets := []common.Address{common.HexToAddress("0x10")}
	prices := []*big.Int{big.NewInt(500)}

	callCount := 0
	mock := &mockMulticaller{
		executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int, callIndex int) ([]outbound.Result, error) {
			callCount++
			if callIndex == 0 {
				return []outbound.Result{
					{Success: true, ReturnData: abiPackAddress(t, providerABI, newOracleAddr)},
					{Success: false, ReturnData: nil},
				}, nil
			}
			return []outbound.Result{
				{Success: true, ReturnData: abiPackPrices(t, oracleABI, prices)},
			}, nil
		},
	}

	_, err := FetchOraclePrices(
		context.Background(), mock,
		providerABI, oracleABI,
		providerAddr, cachedOracleAddr,
		assets, 100,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if callCount != 2 {
		t.Errorf("Execute called %d times, want 2", callCount)
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
			got := ConvertOraclePriceToUSD(tt.rawPrice)
			if !floatEquals(got, tt.want) {
				t.Errorf("ConvertOraclePriceToUSD(%v) = %v, want %v", tt.rawPrice, got, tt.want)
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

// containsSubstring checks if s contains substr.
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
