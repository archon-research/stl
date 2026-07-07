package allocation_tracker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
)

func TestERC4626Source_Supports(t *testing.T) {
	src, err := NewERC4626Source(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		name      string
		tokenType string
		protocol  string
		want      bool
	}{
		{"erc4626 token type", "erc4626", "sky", true},
		{"plain erc20 token type", "erc20", "sky", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := src.Supports(tc.tokenType, tc.protocol); got != tc.want {
				t.Fatalf("Supports(%q, %q) = %v, want %v", tc.tokenType, tc.protocol, got, tc.want)
			}
		})
	}
}

func TestERC4626Source_FetchBalances_StoresShareBalance(t *testing.T) {
	contract := common.HexToAddress("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	expectedShares := new(big.Int).Mul(big.NewInt(12345), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	expectedAssets := new(big.Int).Add(expectedShares, big.NewInt(1000))

	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assetsData, err := src.vaultABI.Methods["convertToAssets"].Outputs.Pack(expectedAssets)
	if err != nil {
		t.Fatalf("pack convertToAssets output: %v", err)
	}
	mc.ExecuteFn = respondTwoRounds(t, src, expectedShares, &outbound.Result{Success: true, ReturnData: assetsData})

	entries := []*TokenEntry{{
		ContractAddress: contract,
		WalletAddress:   wallet,
		Star:            "spark",
		Chain:           "mainnet",
		Protocol:        "sky",
		TokenType:       "erc4626",
	}}

	results, err := src.FetchBalances(context.Background(), entries, 24584100)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	if mc.CallCount != 2 {
		t.Fatalf("expected exactly two multicall rounds, got %d", mc.CallCount)
	}

	got := results.Balances[entries[0].Key()]
	if got == nil {
		t.Fatal("expected result for entry")
	}
	if got.Balance.Cmp(expectedShares) != 0 {
		t.Fatalf("balance = %s, want %s", got.Balance, expectedShares)
	}
	if got.ScaledBalance == nil || got.ScaledBalance.Cmp(expectedShares) != 0 {
		t.Fatalf("scaled balance = %v, want %s", got.ScaledBalance, expectedShares)
	}
	if len(results.Supplies) != 0 {
		t.Fatalf("erc4626 source should not emit supply rows, got %d", len(results.Supplies))
	}
}

func TestERC4626Source_FetchBalances_FailedCallReturnsError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: false, ReturnData: nil}}, nil
	}

	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0xaaaa"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "erc4626",
	}}

	results, err := src.FetchBalances(context.Background(), entries, 100)
	if err == nil {
		t.Fatal("expected error for failed balanceOf call")
	}
	if results != nil {
		t.Fatal("expected nil results on failed balanceOf call")
	}
}

// respondTwoRounds returns an ExecuteFn serving round 1 (balanceOf) then
// round 2 (convertToAssets); pass &outbound.Result{Success: false} to simulate a revert.
// It asserts that round 2 pins the exact same block number as round 1.
func respondTwoRounds(t *testing.T, src *ERC4626Source, shares *big.Int, convertResult *outbound.Result) func(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
	t.Helper()
	round := 0
	var round1Block *big.Int
	return func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		round++
		switch round {
		case 1:
			round1Block = blockNumber
			ret, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(shares)
			if err != nil {
				t.Fatalf("pack balanceOf output: %v", err)
			}
			return []outbound.Result{{Success: true, ReturnData: ret}}, nil
		case 2:
			if blockNumber == nil {
				t.Fatal("round 2 must pin the same block")
			}
			if round1Block == nil || blockNumber.Cmp(round1Block) != 0 {
				t.Fatalf("round 2 block %v != round 1 block %v: both rounds must pin the same block", blockNumber, round1Block)
			}
			return []outbound.Result{*convertResult}, nil
		default:
			t.Fatalf("unexpected round %d", round)
			return nil, nil
		}
	}
}

func TestERC4626Source_FetchBalances_SetsUnderlyingValueFromConvertToAssets(t *testing.T) {
	shares := big.NewInt(1_000_000)
	assets := big.NewInt(1_023_000)
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	ret, err := src.vaultABI.Methods["convertToAssets"].Outputs.Pack(assets)
	if err != nil {
		t.Fatalf("pack convertToAssets output: %v", err)
	}
	mc.ExecuteFn = respondTwoRounds(t, src, shares, &outbound.Result{Success: true, ReturnData: ret})

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	got := results.Balances[entries[0].Key()]
	if got.UnderlyingValue == nil || got.UnderlyingValue.Cmp(assets) != 0 {
		t.Fatalf("UnderlyingValue = %v, want %s", got.UnderlyingValue, assets)
	}
	if got.Balance.Cmp(shares) != 0 || got.ScaledBalance.Cmp(shares) != 0 {
		t.Fatalf("balance/scaled must stay raw shares, got %s/%s", got.Balance, got.ScaledBalance)
	}
	if mc.CallCount != 2 {
		t.Fatalf("expected 2 multicall rounds, got %d", mc.CallCount)
	}
}

func TestERC4626Source_FetchBalances_ConvertRevertLeavesUnderlyingNil(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteFn = respondTwoRounds(t, src, big.NewInt(5), &outbound.Result{Success: false})

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("a reverting convertToAssets must not fail the fetch: %v", err)
	}
	got := results.Balances[entries[0].Key()]
	if got.UnderlyingValue != nil {
		t.Fatalf("UnderlyingValue = %v, want nil (never 0, never shares)", got.UnderlyingValue)
	}
	if got.Balance.Cmp(big.NewInt(5)) != 0 {
		t.Fatalf("balance must be unaffected, got %s", got.Balance)
	}
}

func TestERC4626Source_FetchBalances_ZeroSharesSkipConvertAndWriteZero(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		ret, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(0))
		if err != nil {
			t.Fatalf("pack balanceOf output: %v", err)
		}
		return []outbound.Result{{Success: true, ReturnData: ret}}, nil
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}
	if mc.CallCount != 1 {
		t.Fatalf("zero shares must skip round 2, got %d rounds", mc.CallCount)
	}
	got := results.Balances[entries[0].Key()]
	if got.UnderlyingValue == nil || got.UnderlyingValue.Sign() != 0 {
		t.Fatalf("UnderlyingValue = %v, want 0", got.UnderlyingValue)
	}
}

func TestERC4626Source_FetchBalances_TruncatedResultReturnsError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	round := 0
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		round++
		if round == 1 {
			ret, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(5))
			if err != nil {
				t.Fatalf("pack balanceOf output: %v", err)
			}
			return []outbound.Result{{Success: true, ReturnData: ret}}, nil
		}
		// Truncated: 0 results for 1 call.
		return []outbound.Result{}, nil
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	_, err = src.FetchBalances(context.Background(), entries, 100)
	if err == nil {
		t.Fatal("expected error for truncated round-2 result")
	}
	if !strings.Contains(err.Error(), "returned 0 results for 1 calls") {
		t.Fatalf("error %q does not mention count mismatch", err.Error())
	}
}

func TestERC4626Source_FetchBalances_ConvertTransportErrorFailsFetch(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	round := 0
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		round++
		if round == 1 {
			ret, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(5))
			if err != nil {
				t.Fatalf("pack balanceOf output: %v", err)
			}
			return []outbound.Result{{Success: true, ReturnData: ret}}, nil
		}
		return nil, fmt.Errorf("rpc down")
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	if _, err := src.FetchBalances(context.Background(), entries, 100); err == nil {
		t.Fatal("a transport-level round-2 failure must propagate so SQS retries the block")
	}
}
