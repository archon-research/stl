package allocation_tracker

import (
	"bytes"
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

	// The supply row is emitted for every vault regardless of a wallet's own
	// balance; a genuine totalSupply of 0 is a legitimate value, not a failure.
	tests := []struct {
		name        string
		totalSupply *big.Int
	}{
		{"non-zero total supply", new(big.Int).Mul(big.NewInt(99999), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))},
		{"zero total supply emits zero row", big.NewInt(0)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			assetsData, err := src.vaultABI.Methods["convertToAssets"].Outputs.Pack(expectedAssets)
			if err != nil {
				t.Fatalf("pack convertToAssets output: %v", err)
			}
			mc.ExecuteAtHashFn = respondTwoRounds(t, src, expectedShares, tc.totalSupply, &outbound.Result{Success: true, ReturnData: assetsData})

			entries := []*TokenEntry{{
				ContractAddress: contract,
				WalletAddress:   wallet,
				Star:            "spark",
				Chain:           "mainnet",
				Protocol:        "sky",
				TokenType:       "erc4626",
			}}

			results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
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

			if len(results.Supplies) != 1 {
				t.Fatalf("expected exactly one supply row, got %d", len(results.Supplies))
			}
			sup := results.Supplies[contract]
			if sup == nil {
				t.Fatalf("expected a supply row for the vault %s, got none", contract.Hex())
			}
			if sup.TotalSupply.Cmp(tc.totalSupply) != 0 {
				t.Fatalf("total supply = %s, want %s", sup.TotalSupply, tc.totalSupply)
			}
			if sup.ScaledTotalSupply != nil {
				t.Fatalf("scaled total supply = %v, want nil (vaults have no scaled supply)", sup.ScaledTotalSupply)
			}
		})
	}
}

func TestERC4626Source_FetchBalances_FailedCallReturnsError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			if bytes.Equal(c.CallData[:4], src.vaultABI.Methods["balanceOf"].ID) {
				results[i] = outbound.Result{Success: false, ReturnData: nil}
				continue
			}
			results[i] = outbound.Result{Success: true, ReturnData: padUint256(big.NewInt(1))}
		}
		return results, nil
	}

	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0xaaaa"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "erc4626",
	}}

	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err == nil {
		t.Fatal("expected error for failed balanceOf call")
	}
	if results != nil {
		t.Fatal("expected nil results on failed balanceOf call")
	}
}

// respondTwoRounds returns an ExecuteAtHashFn serving round 1 (balanceOf +
// totalSupply, dispatched by selector) then round 2 (convertToAssets); pass
// &outbound.Result{Success: false} to simulate a convertToAssets revert. It
// asserts both rounds pin the exact same block hash (testBlockHash) so the
// two-round snapshot stays reorg-correct (VEC-471): a reorg between rounds must
// never let round 2 read a different fork's state than round 1.
func respondTwoRounds(t *testing.T, src *ERC4626Source, shares, totalSupply *big.Int, convertResult *outbound.Result) func(context.Context, []outbound.Call, common.Hash) ([]outbound.Result, error) {
	t.Helper()
	round := 0
	return func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		round++
		if blockHash != testBlockHash {
			t.Fatalf("round %d blockHash = %v, want %v (state reads must pin to the block hash, not the number, so a reorg can't return the wrong fork's state)", round, blockHash, testBlockHash)
		}
		switch round {
		case 1:
			return packRound1(t, src, calls, shares, totalSupply), nil
		case 2:
			return []outbound.Result{*convertResult}, nil
		default:
			t.Fatalf("unexpected round %d", round)
			return nil, nil
		}
	}
}

// packRound1 packs the round-1 balanceOf + totalSupply calls by selector: every
// balanceOf call returns `shares`, every totalSupply call returns `totalSupply`.
func packRound1(t *testing.T, src *ERC4626Source, calls []outbound.Call, shares, totalSupply *big.Int) []outbound.Result {
	t.Helper()
	results, _ := packRound1Vaults(t, src, calls, shares, func(common.Address) *big.Int { return totalSupply })
	return results
}

// packRound1Vaults is packRound1 with per-vault totalSupply attribution: every
// balanceOf returns `shares`, and each totalSupply is answered by supplyFor
// keyed on Call.Target so distinct vaults return distinct supplies. It also
// returns how many totalSupply calls it served, so a caller can assert dedup.
func packRound1Vaults(t *testing.T, src *ERC4626Source, calls []outbound.Call, shares *big.Int, supplyFor func(target common.Address) *big.Int) ([]outbound.Result, int) {
	t.Helper()
	results := make([]outbound.Result, len(calls))
	supplyCalls := 0
	for i, c := range calls {
		sel := c.CallData[:4]
		switch {
		case bytes.Equal(sel, src.vaultABI.Methods["balanceOf"].ID):
			rd, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(shares)
			if err != nil {
				t.Fatalf("pack balanceOf output: %v", err)
			}
			results[i] = outbound.Result{Success: true, ReturnData: rd}
		case bytes.Equal(sel, src.vaultABI.Methods["totalSupply"].ID):
			supplyCalls++
			rd, err := src.vaultABI.Methods["totalSupply"].Outputs.Pack(supplyFor(c.Target))
			if err != nil {
				t.Fatalf("pack totalSupply output: %v", err)
			}
			results[i] = outbound.Result{Success: true, ReturnData: rd}
		default:
			t.Fatalf("round-1 unexpected selector %x", sel)
		}
	}
	return results, supplyCalls
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
	mc.ExecuteAtHashFn = respondTwoRounds(t, src, shares, big.NewInt(9_999_999), &outbound.Result{Success: true, ReturnData: ret})

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
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
	mc.ExecuteAtHashFn = respondTwoRounds(t, src, big.NewInt(5), big.NewInt(1000), &outbound.Result{Success: false})

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
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
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		return packRound1(t, src, calls, big.NewInt(0), big.NewInt(777)), nil
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
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
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		round++
		if round == 1 {
			return packRound1(t, src, calls, big.NewInt(5), big.NewInt(1000)), nil
		}
		// Truncated: 0 results for 1 call.
		return []outbound.Result{}, nil
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	_, err = src.FetchBalances(context.Background(), entries, testBlockHash)
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
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		round++
		if round == 1 {
			return packRound1(t, src, calls, big.NewInt(5), big.NewInt(1000)), nil
		}
		return nil, fmt.Errorf("rpc down")
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	if _, err := src.FetchBalances(context.Background(), entries, testBlockHash); err == nil {
		t.Fatal("a transport-level round-2 failure must propagate so SQS retries the block")
	}
}

// TestERC4626Source_FetchBalances_Round1LengthMismatchReturnsError asserts a
// round-1 multicall returning fewer results than calls fails the fetch rather
// than silently mis-aligning results to calls.
func TestERC4626Source_FetchBalances_Round1LengthMismatchReturnsError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		// One fewer result than calls (drop the last).
		full := packRound1(t, src, calls, big.NewInt(5), big.NewInt(1000))
		return full[:len(full)-1], nil
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err == nil {
		t.Fatal("expected error for round-1 length mismatch")
	}
	if !strings.Contains(err.Error(), "erc4626 round-1 multicall returned 1 results for 2 calls") {
		t.Fatalf("error %q does not carry the round-1 count-mismatch message", err.Error())
	}
	if results != nil {
		t.Fatal("expected nil results on round-1 length mismatch")
	}
}

// TestERC4626Source_FetchBalances_Round1TransportErrorFailsFetch asserts a
// transport-level round-1 failure propagates (wrapped) so SQS retries the block.
func TestERC4626Source_FetchBalances_Round1TransportErrorFailsFetch(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		return nil, fmt.Errorf("rpc down")
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err == nil {
		t.Fatal("a transport-level round-1 failure must propagate so SQS retries the block")
	}
	if !strings.Contains(err.Error(), "balanceOf multicall") {
		t.Fatalf("error %q does not wrap the balanceOf multicall failure", err.Error())
	}
	if !strings.Contains(err.Error(), "rpc down") {
		t.Fatalf("error %q does not wrap the transport error", err.Error())
	}
	if results != nil {
		t.Fatal("expected nil results on round-1 transport failure")
	}
}

// TestERC4626Source_FetchBalances_SupplyDedup verifies that two wallets holding
// the same vault produce exactly one totalSupply() call and one Supplies entry.
func TestERC4626Source_FetchBalances_SupplyDedup(t *testing.T) {
	vault := common.HexToAddress("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd")
	wallet1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	wallet2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		totalSupplyCalls := 0
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			switch {
			case bytes.Equal(c.CallData[:4], src.vaultABI.Methods["balanceOf"].ID):
				// Zero shares so no convertToAssets round is scheduled.
				rd, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(0))
				if err != nil {
					t.Fatalf("pack balanceOf output: %v", err)
				}
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(c.CallData[:4], src.vaultABI.Methods["totalSupply"].ID):
				totalSupplyCalls++
				results[i] = outbound.Result{Success: true, ReturnData: padUint256(big.NewInt(1))}
			default:
				t.Fatalf("unexpected selector %x", c.CallData[:4])
			}
		}
		if totalSupplyCalls != 1 {
			t.Errorf("totalSupply calls = %d, want 1 (deduped across wallets)", totalSupplyCalls)
		}
		return results, nil
	}

	entries := []*TokenEntry{
		{ContractAddress: vault, WalletAddress: wallet1, TokenType: "erc4626"},
		{ContractAddress: vault, WalletAddress: wallet2, TokenType: "erc4626"},
	}
	got, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	if len(got.Supplies) != 1 {
		t.Errorf("expected 1 supply entry (deduped across wallets), got %d", len(got.Supplies))
	}
}

// TestERC4626Source_FetchBalances_AttributesSupplyToItsVault asserts each
// distinct vault's totalSupply lands under its own contract address: two vaults
// sharing a wallet must not have their supplies swapped or collapsed. Asserting
// the value pairing (not just the count) is the point.
func TestERC4626Source_FetchBalances_AttributesSupplyToItsVault(t *testing.T) {
	vaultA := common.HexToAddress("0xaaaa000000000000000000000000000000000001")
	vaultB := common.HexToAddress("0xbbbb000000000000000000000000000000000002")
	walletW := common.HexToAddress("0x1111111111111111111111111111111111111111")
	walletX := common.HexToAddress("0x2222222222222222222222222222222222222222")

	wad := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	supplyA := new(big.Int).Mul(big.NewInt(7_000_000), wad)
	supplyB := big.NewInt(11)
	supplyByVault := map[common.Address]*big.Int{vaultA: supplyA, vaultB: supplyB}

	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	supplyCalls := 0
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		// Zero shares so no convertToAssets round is scheduled; only round 1 runs.
		results, n := packRound1Vaults(t, src, calls, big.NewInt(0), func(target common.Address) *big.Int {
			supply, ok := supplyByVault[target]
			if !ok {
				t.Fatalf("round-1 totalSupply for unexpected target %s", target.Hex())
			}
			return supply
		})
		supplyCalls += n
		return results, nil
	}

	// Wallet W holds both vaults; wallet X also holds vault A, keeping dedup in play.
	entries := []*TokenEntry{
		{ContractAddress: vaultA, WalletAddress: walletW, TokenType: "erc4626"},
		{ContractAddress: vaultA, WalletAddress: walletX, TokenType: "erc4626"},
		{ContractAddress: vaultB, WalletAddress: walletW, TokenType: "erc4626"},
	}
	got, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	if supplyCalls != 2 {
		t.Fatalf("totalSupply calls = %d, want 2 (one per distinct vault)", supplyCalls)
	}
	if len(got.Supplies) != 2 {
		t.Fatalf("Supplies entries = %d, want 2", len(got.Supplies))
	}
	supA := got.Supplies[vaultA]
	if supA == nil {
		t.Fatal("expected a supply row for vaultA")
	}
	if supA.TotalSupply.Cmp(supplyA) != 0 {
		t.Fatalf("Supplies[vaultA].TotalSupply = %s, want %s", supA.TotalSupply, supplyA)
	}
	supB := got.Supplies[vaultB]
	if supB == nil {
		t.Fatal("expected a supply row for vaultB")
	}
	if supB.TotalSupply.Cmp(supplyB) != 0 {
		t.Fatalf("Supplies[vaultB].TotalSupply = %s, want %s", supB.TotalSupply, supplyB)
	}
	if supA.ScaledTotalSupply != nil || supB.ScaledTotalSupply != nil {
		t.Fatalf("scaled total supply must be nil for vaults, got %v / %v", supA.ScaledTotalSupply, supB.ScaledTotalSupply)
	}
}

// TestERC4626Source_FetchBalances_FailedTotalSupplyReturnsError asserts a failed
// totalSupply sub-call bubbles up (no partial Supplies, no swallowed failure);
// see the decodeSupplies comment for why this source escalates where the aToken
// source drops the row.
func TestERC4626Source_FetchBalances_FailedTotalSupplyReturnsError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			switch {
			case bytes.Equal(c.CallData[:4], src.vaultABI.Methods["balanceOf"].ID):
				rd, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(0))
				if err != nil {
					t.Fatalf("pack balanceOf output: %v", err)
				}
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(c.CallData[:4], src.vaultABI.Methods["totalSupply"].ID):
				results[i] = outbound.Result{Success: false}
			default:
				t.Fatalf("unexpected selector %x", c.CallData[:4])
			}
		}
		return results, nil
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err == nil {
		t.Fatal("a failed totalSupply must fail the fetch")
	}
	if results != nil {
		t.Fatal("expected nil results when totalSupply fails")
	}
}

// TestERC4626Source_FetchBalances_UndecodableTotalSupplyReturnsError asserts a
// successful-but-undecodable totalSupply return bubbles up as an error.
func TestERC4626Source_FetchBalances_UndecodableTotalSupplyReturnsError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	mc.ExecuteAtHashFn = func(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			switch {
			case bytes.Equal(c.CallData[:4], src.vaultABI.Methods["balanceOf"].ID):
				rd, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(0))
				if err != nil {
					t.Fatalf("pack balanceOf output: %v", err)
				}
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(c.CallData[:4], src.vaultABI.Methods["totalSupply"].ID):
				// Truncated (<32 bytes): decodes to nil, must error.
				results[i] = outbound.Result{Success: true, ReturnData: []byte{0x01, 0x02}}
			default:
				t.Fatalf("unexpected selector %x", c.CallData[:4])
			}
		}
		return results, nil
	}

	entries := []*TokenEntry{{ContractAddress: common.HexToAddress("0xaaaa"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"}}
	results, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err == nil {
		t.Fatal("an undecodable totalSupply must fail the fetch")
	}
	if results != nil {
		t.Fatal("expected nil results when totalSupply is undecodable")
	}
}
