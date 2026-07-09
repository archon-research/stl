package blockchain

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func feedABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsed, err := abis.GetAggregatorV3ABI()
	if err != nil {
		t.Fatalf("loading AggregatorV3 ABI: %v", err)
	}
	return parsed
}

func packRoundData(t *testing.T, answer *big.Int, updatedAt *big.Int) []byte {
	t.Helper()
	return testutil.PackLatestRoundData(t,
		big.NewInt(1),    // roundId
		answer,           // answer
		big.NewInt(1000), // startedAt
		updatedAt,        // updatedAt
		big.NewInt(1),    // answeredInRound
	)
}

func packLatestAnswer(t *testing.T, answer *big.Int) []byte {
	t.Helper()
	return testutil.PackLatestAnswer(t, answer)
}

func TestFetchFeedPrices(t *testing.T) {
	fABI := feedABI(t)
	blockNum := int64(12345678)

	feed1 := common.HexToAddress("0x0000000000000000000000000000000000000AAA")
	feed2 := common.HexToAddress("0x0000000000000000000000000000000000000BBB")

	feeds := []FeedConfig{
		{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
		{TokenID: 2, FeedAddress: feed2, FeedDecimals: 18, QuoteCurrency: "USD"},
	}

	tests := []struct {
		name        string
		feeds       []FeedConfig
		mock        *testutil.MockMulticaller
		wantErr     bool
		errContains string
		wantResults []FeedPriceResult
	}{
		{
			name:  "happy path - two feeds succeed",
			feeds: feeds,
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) != 2 {
						t.Fatalf("expected 2 calls, got %d", len(calls))
					}
					return []outbound.Result{
						{Success: true, ReturnData: packRoundData(t, big.NewInt(200000000000), big.NewInt(1000))},                                                               // $2000 with 8 decimals
						{Success: true, ReturnData: packRoundData(t, new(big.Int).Mul(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)), big.NewInt(1000))}, // $1 with 18 decimals
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Price: 1.0, Success: true},
			},
		},
		{
			name:  "feed reverts - AllowFailure skips it, latestAnswer also fails",
			feeds: feeds,
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					if len(calls) == 2 {
						// 1st call: latestRoundData
						return []outbound.Result{
							{Success: true, ReturnData: packRoundData(t, big.NewInt(200000000000), big.NewInt(1000))},
							{Success: false, ReturnData: nil}, // feed reverted
						}, nil
					}
					// 2nd call: latestAnswer retry for 1 failed feed
					return []outbound.Result{
						{Success: false},
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Success: false},
			},
		},
		{
			name:  "unpack error on Success true returns error",
			feeds: feeds[:1],
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: []byte{0xde, 0xad, 0xbe, 0xef}}, // bad data
					}, nil
				},
			},
			wantErr:     true,
			errContains: "unpacking latestRoundData",
		},
		{
			name:  "multicall error returns error",
			feeds: feeds,
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("RPC connection refused")
				},
			},
			wantErr:     true,
			errContains: "executing multicall at block",
		},
		{
			name:  "empty feeds returns nil",
			feeds: []FeedConfig{},
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					t.Fatal("Execute should not be called for empty feeds")
					return nil, nil
				},
			},
			wantResults: nil,
		},
		{
			name:  "result count mismatch returns error",
			feeds: feeds,
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100000000), big.NewInt(1000))},
					}, nil // 1 result for 2 feeds
				},
			},
			wantErr:     true,
			errContains: "expected 2 multicall results, got 1",
		},
		{
			name: "mixed decimals: 8 and 18",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
				{TokenID: 2, FeedAddress: feed2, FeedDecimals: 18, QuoteCurrency: "USD"},
			},
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					// 100_000_000 with 8 decimals = $1.00
					// 2500 * 10^18 with 18 decimals = $2500.00
					price18 := new(big.Int).Mul(big.NewInt(2500), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
					return []outbound.Result{
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
						{Success: true, ReturnData: packRoundData(t, price18, big.NewInt(1000))},
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Price: 1.0, Success: true},
				{TokenID: 2, Price: 2500.0, Success: true},
			},
		},
		{
			name: "zero feed address returns validation error",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: common.Address{}, FeedDecimals: 8, QuoteCurrency: "USD"},
			},
			mock:        &testutil.MockMulticaller{},
			wantErr:     true,
			errContains: "zero feed address",
		},
		{
			name: "zero feed decimals returns validation error",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: 0, QuoteCurrency: "USD"},
			},
			mock:        &testutil.MockMulticaller{},
			wantErr:     true,
			errContains: "invalid feed decimals",
		},
		{
			name: "negative feed decimals returns validation error",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: -1, QuoteCurrency: "USD"},
			},
			mock:        &testutil.MockMulticaller{},
			wantErr:     true,
			errContains: "invalid feed decimals",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := FetchFeedPrices(
				context.Background(),
				tt.mock,
				fABI,
				tt.feeds,
				blockNum,
				oracleTestBlockHash,
				testutil.DiscardLogger(),
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
				if got.TokenID != want.TokenID {
					t.Errorf("results[%d].TokenID = %d, want %d", i, got.TokenID, want.TokenID)
				}
				if got.Success != want.Success {
					t.Errorf("results[%d].Success = %v, want %v", i, got.Success, want.Success)
				}
				if want.Success && !floatEquals(got.Price, want.Price) {
					t.Errorf("results[%d].Price = %f, want %f", i, got.Price, want.Price)
				}
			}
		})
	}
}

func TestFetchFeedPrices_LatestAnswerFallback(t *testing.T) {
	fABI := feedABI(t)
	blockNum := int64(12345678)

	feed1 := common.HexToAddress("0x0000000000000000000000000000000000000AAA")
	feed2 := common.HexToAddress("0x0000000000000000000000000000000000000BBB")

	feeds := []FeedConfig{
		{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
		{TokenID: 2, FeedAddress: feed2, FeedDecimals: 8, QuoteCurrency: "USD"},
	}

	tests := []struct {
		name        string
		feeds       []FeedConfig
		callNum     int // tracks Execute invocations
		mock        func(t *testing.T) *testutil.MockMulticaller
		wantErr     bool
		errContains string
		wantResults []FeedPriceResult
	}{
		{
			name:  "latestRoundData fails, latestAnswer succeeds",
			feeds: feeds,
			mock: func(t *testing.T) *testutil.MockMulticaller {
				return newRoundDispatch(
					// 1st call: latestRoundData — feed1 ok, feed2 reverts
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{
							{Success: true, ReturnData: packRoundData(t, big.NewInt(200000000000), big.NewInt(1000))},
							{Success: false},
						}, nil
					},
					// 2nd call: latestAnswer for feed2 only
					func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						if len(calls) != 1 {
							t.Fatalf("latestAnswer retry: expected 1 call, got %d", len(calls))
						}
						if calls[0].Target != feed2 {
							t.Errorf("latestAnswer target = %v, want %v", calls[0].Target, feed2)
						}
						return []outbound.Result{
							{Success: true, ReturnData: packLatestAnswer(t, big.NewInt(117000000))}, // $1.17
						}, nil
					},
				)
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Price: 1.17, Success: true},
			},
		},
		{
			name:  "both latestRoundData and latestAnswer fail",
			feeds: feeds[:1],
			mock: func(t *testing.T) *testutil.MockMulticaller {
				return newRoundDispatch(
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{{Success: false}}, nil
					},
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{{Success: false}}, nil
					},
				)
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name:  "latestAnswer returns non-positive value",
			feeds: feeds[:1],
			mock: func(t *testing.T) *testutil.MockMulticaller {
				return newRoundDispatch(
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{{Success: false}}, nil
					},
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{
							{Success: true, ReturnData: packLatestAnswer(t, big.NewInt(0))},
						}, nil
					},
				)
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name:  "latestAnswer multicall error returns error",
			feeds: feeds[:1],
			mock: func(t *testing.T) *testutil.MockMulticaller {
				return newRoundDispatch(
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{{Success: false}}, nil
					},
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return nil, errors.New("RPC connection refused")
					},
				)
			},
			wantErr:     true,
			errContains: "executing latestAnswer multicall",
		},
		{
			name:  "zero answer retries with latestAnswer, also fails",
			feeds: feeds[:1],
			mock: func(t *testing.T) *testutil.MockMulticaller {
				return newRoundDispatch(
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{
							{Success: true, ReturnData: packRoundData(t, big.NewInt(0), big.NewInt(1000))},
						}, nil
					},
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{
							{Success: true, ReturnData: packLatestAnswer(t, big.NewInt(0))},
						}, nil
					},
				)
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name:  "negative answer retries with latestAnswer, recovers",
			feeds: feeds[:1],
			mock: func(t *testing.T) *testutil.MockMulticaller {
				return newRoundDispatch(
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{
							{Success: true, ReturnData: packRoundData(t, big.NewInt(-100), big.NewInt(1000))},
						}, nil
					},
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{
							{Success: true, ReturnData: packLatestAnswer(t, big.NewInt(100_000_000))},
						}, nil
					},
				)
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Price: 1.0, Success: true},
			},
		},
		{
			name:  "all feeds succeed on latestRoundData, no fallback needed",
			feeds: feeds,
			mock: func(t *testing.T) *testutil.MockMulticaller {
				return newRoundDispatch(
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						return []outbound.Result{
							{Success: true, ReturnData: packRoundData(t, big.NewInt(200000000000), big.NewInt(1000))},
							{Success: true, ReturnData: packRoundData(t, big.NewInt(100000000), big.NewInt(1000))},
						}, nil
					},
					// This should NOT be called
					func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
						t.Fatal("latestAnswer should not be called when all feeds succeed")
						return nil, nil
					},
				)
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Price: 1.0, Success: true},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := tt.mock(t)
			results, err := FetchFeedPrices(
				context.Background(),
				mock,
				fABI,
				tt.feeds,
				blockNum,
				oracleTestBlockHash,
				testutil.DiscardLogger(),
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

			if len(results) != len(tt.wantResults) {
				t.Fatalf("results length = %d, want %d", len(results), len(tt.wantResults))
			}
			for i, got := range results {
				want := tt.wantResults[i]
				if got.TokenID != want.TokenID {
					t.Errorf("results[%d].TokenID = %d, want %d", i, got.TokenID, want.TokenID)
				}
				if got.Success != want.Success {
					t.Errorf("results[%d].Success = %v, want %v", i, got.Success, want.Success)
				}
				if want.Success && !floatEquals(got.Price, want.Price) {
					t.Errorf("results[%d].Price = %f, want %f", i, got.Price, want.Price)
				}
			}
		})
	}
}

// newRoundDispatch returns a MockMulticaller that sends each successive
// Execute/ExecuteAtHash call to the next function in fns, so a test can script a
// multi-round interaction (e.g. latestRoundData then a latestAnswer retry). An
// unexpected extra call errors rather than dispatching off the end.
func newRoundDispatch(fns ...func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error)) *testutil.MockMulticaller {
	mc := testutil.NewMockMulticaller()
	var idx int
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if idx >= len(fns) {
			return nil, fmt.Errorf("unexpected multicall #%d", idx)
		}
		fn := fns[idx]
		idx++
		return fn(ctx, calls, blockNumber)
	}
	return mc
}

func TestFetchFeedPrices_VerifiesCallTargets(t *testing.T) {
	fABI := feedABI(t)

	feed1 := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	feed2 := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

	feeds := []FeedConfig{
		{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
		{TokenID: 2, FeedAddress: feed2, FeedDecimals: 8, QuoteCurrency: "USD"},
	}

	// Reads must be pinned to the block hash, not the number: after a reorg an
	// archive node answers eth_call-by-number with the new canonical feed value,
	// which can silently disagree with the reorged block being processed (VEC-471).
	mock := &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			t.Fatal("FetchFeedPrices must call ExecuteAtHash for a non-zero block hash, not Execute")
			return nil, nil
		},
		ExecuteAtHashFn: func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
			if len(calls) != 2 {
				t.Fatalf("expected 2 calls, got %d", len(calls))
			}
			// Each call targets its own feed address (not a shared oracle address)
			if calls[0].Target != feed1 {
				t.Errorf("call[0].Target = %v, want %v", calls[0].Target, feed1)
			}
			if calls[1].Target != feed2 {
				t.Errorf("call[1].Target = %v, want %v", calls[1].Target, feed2)
			}
			for i, call := range calls {
				if !call.AllowFailure {
					t.Errorf("call[%d].AllowFailure = false, want true", i)
				}
			}
			if blockHash != oracleTestBlockHash {
				t.Errorf("blockHash = %s, want %s", blockHash, oracleTestBlockHash)
			}
			return []outbound.Result{
				{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
				{Success: true, ReturnData: packRoundData(t, big.NewInt(200_000_000), big.NewInt(1000))},
			}, nil
		},
	}

	results, err := FetchFeedPrices(context.Background(), mock, fABI, feeds, 99, oracleTestBlockHash, testutil.DiscardLogger())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if !results[0].Success || !floatEquals(results[0].Price, 1.0) {
		t.Errorf("results[0] = %+v, want Success=true, Price=1.0", results[0])
	}
	if !results[1].Success || !floatEquals(results[1].Price, 2.0) {
		t.Errorf("results[1] = %+v, want Success=true, Price=2.0", results[1])
	}
}

func TestValidateFeedDecimals(t *testing.T) {
	fABI := feedABI(t)
	blockNum := int64(12345678)

	feed1 := common.HexToAddress("0x0000000000000000000000000000000000000AAA")
	feed2 := common.HexToAddress("0x0000000000000000000000000000000000000BBB")

	tests := []struct {
		name        string
		feeds       []FeedConfig
		mock        *testutil.MockMulticaller
		wantErr     bool
		errContains string
	}{
		{
			name: "matching decimals passes",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
				{TokenID: 2, FeedAddress: feed2, FeedDecimals: 18, QuoteCurrency: "USD"},
			},
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackDecimals(t, 8)},
						{Success: true, ReturnData: testutil.PackDecimals(t, 18)},
					}, nil
				},
			},
		},
		{
			name: "mismatching decimals returns error",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
			},
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackDecimals(t, 18)},
					}, nil
				},
			},
			wantErr:     true,
			errContains: "feed decimals mismatch",
		},
		{
			name: "decimals call reverts - warn and skip",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
			},
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: false},
					}, nil
				},
			},
		},
		{
			name:  "empty feeds is no-op",
			feeds: []FeedConfig{},
			mock:  &testutil.MockMulticaller{},
		},
		{
			name: "multicall error propagates",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
			},
			mock: &testutil.MockMulticaller{
				ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("RPC connection refused")
				},
			},
			wantErr:     true,
			errContains: "executing decimals() multicall",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateFeedDecimals(
				context.Background(),
				tt.mock,
				fABI,
				tt.feeds,
				blockNum,
				testutil.DiscardLogger(),
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
		})
	}
}
