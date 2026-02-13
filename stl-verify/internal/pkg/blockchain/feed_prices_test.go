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

func testFeedABI(t *testing.T) *abi.ABI {
	t.Helper()
	feedABI, err := abis.GetAggregatorV3ABI()
	if err != nil {
		t.Fatalf("loading AggregatorV3 ABI: %v", err)
	}
	return feedABI
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

func TestFetchFeedPrices(t *testing.T) {
	feedABI := testFeedABI(t)
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
		mock        *mockMulticaller
		wantErr     bool
		errContains string
		wantResults []FeedPriceResult
	}{
		{
			name:  "happy path - two feeds succeed",
			feeds: feeds,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
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
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
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
			name:  "answer <= 0 marks feed as failed",
			feeds: feeds[:1],
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: packRoundData(t, big.NewInt(0), big.NewInt(1000))}, // zero answer
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name:  "updatedAt == 0 marks feed as failed",
			feeds: feeds[:1],
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: packRoundData(t, big.NewInt(200000000000), big.NewInt(0))}, // updatedAt=0
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name:  "unpack error on Success true returns error",
			feeds: feeds[:1],
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
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
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return nil, errors.New("RPC connection refused")
				},
			},
			wantErr:     true,
			errContains: "executing multicall at block",
		},
		{
			name:  "empty feeds returns nil",
			feeds: []FeedConfig{},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					t.Fatal("Execute should not be called for empty feeds")
					return nil, nil
				},
			},
			wantResults: nil,
		},
		{
			name:  "result count mismatch returns error",
			feeds: feeds,
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: packRoundData(t, big.NewInt(100000000), big.NewInt(1000))},
					}, nil // 1 result for 2 feeds
				},
			},
			wantErr:     true,
			errContains: "expected 2 multicall results, got 1",
		},
		{
			name:  "negative answer marks feed as failed",
			feeds: feeds[:1],
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
					return []outbound.Result{
						{Success: true, ReturnData: packRoundData(t, big.NewInt(-100), big.NewInt(1000))},
					}, nil
				},
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name: "mixed decimals: 8 and 18",
			feeds: []FeedConfig{
				{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
				{TokenID: 2, FeedAddress: feed2, FeedDecimals: 18, QuoteCurrency: "USD"},
			},
			mock: &mockMulticaller{
				executeFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := FetchFeedPrices(
				context.Background(),
				tt.mock,
				feedABI,
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

func packLatestAnswer(t *testing.T, answer *big.Int) []byte {
	t.Helper()
	return testutil.PackLatestAnswer(t, answer)
}

func TestFetchFeedPrices_LatestAnswerFallback(t *testing.T) {
	feedABI := testFeedABI(t)
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
		mock        func(t *testing.T) *callCountMock
		wantErr     bool
		errContains string
		wantResults []FeedPriceResult
	}{
		{
			name:  "latestRoundData fails, latestAnswer succeeds",
			feeds: feeds,
			mock: func(t *testing.T) *callCountMock {
				return &callCountMock{
					executeFns: []func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error){
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
					},
				}
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Price: 1.17, Success: true},
			},
		},
		{
			name:  "both latestRoundData and latestAnswer fail",
			feeds: feeds[:1],
			mock: func(t *testing.T) *callCountMock {
				return &callCountMock{
					executeFns: []func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error){
						func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
							return []outbound.Result{{Success: false}}, nil
						},
						func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
							return []outbound.Result{{Success: false}}, nil
						},
					},
				}
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name:  "latestAnswer returns non-positive value",
			feeds: feeds[:1],
			mock: func(t *testing.T) *callCountMock {
				return &callCountMock{
					executeFns: []func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error){
						func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
							return []outbound.Result{{Success: false}}, nil
						},
						func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
							return []outbound.Result{
								{Success: true, ReturnData: packLatestAnswer(t, big.NewInt(0))},
							}, nil
						},
					},
				}
			},
			wantResults: []FeedPriceResult{
				{TokenID: 1, Success: false},
			},
		},
		{
			name:  "latestAnswer multicall error returns error",
			feeds: feeds[:1],
			mock: func(t *testing.T) *callCountMock {
				return &callCountMock{
					executeFns: []func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error){
						func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
							return []outbound.Result{{Success: false}}, nil
						},
						func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
							return nil, errors.New("RPC connection refused")
						},
					},
				}
			},
			wantErr:     true,
			errContains: "executing latestAnswer multicall",
		},
		{
			name:  "all feeds succeed on latestRoundData, no fallback needed",
			feeds: feeds,
			mock: func(t *testing.T) *callCountMock {
				return &callCountMock{
					executeFns: []func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error){
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
					},
				}
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
				feedABI,
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

// callCountMock is a mock multicaller that dispatches to different functions
// based on the call count, for testing multi-round interactions.
type callCountMock struct {
	executeFns []func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error)
	callIdx    int
}

func (m *callCountMock) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	if m.callIdx >= len(m.executeFns) {
		return nil, fmt.Errorf("unexpected Execute call #%d", m.callIdx)
	}
	fn := m.executeFns[m.callIdx]
	m.callIdx++
	return fn(ctx, calls, blockNumber)
}

func (m *callCountMock) Address() common.Address {
	return common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")
}

func TestFetchFeedPrices_VerifiesCallTargets(t *testing.T) {
	feedABI := testFeedABI(t)

	feed1 := common.HexToAddress("0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	feed2 := common.HexToAddress("0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB")

	feeds := []FeedConfig{
		{TokenID: 1, FeedAddress: feed1, FeedDecimals: 8, QuoteCurrency: "USD"},
		{TokenID: 2, FeedAddress: feed2, FeedDecimals: 8, QuoteCurrency: "USD"},
	}

	mock := &mockMulticaller{
		executeFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
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
			if blockNumber.Int64() != 99 {
				t.Errorf("blockNumber = %d, want 99", blockNumber.Int64())
			}
			return []outbound.Result{
				{Success: true, ReturnData: packRoundData(t, big.NewInt(100_000_000), big.NewInt(1000))},
				{Success: true, ReturnData: packRoundData(t, big.NewInt(200_000_000), big.NewInt(1000))},
			}, nil
		},
	}

	results, err := FetchFeedPrices(context.Background(), mock, feedABI, feeds, 99, testutil.DiscardLogger())
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
