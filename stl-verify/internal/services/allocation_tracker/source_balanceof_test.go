package allocation_tracker

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestBalanceOfSource_Supports(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	src := NewBalanceOfSource(nil, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))

	tests := []struct {
		tokenType string
		protocol  string
		want      bool
	}{
		{"erc20", "", true},
		{"atoken", "sparklend", true},
		{"atoken", "aave", true},
		{"buidl", "", true},
		{"securitize", "", true},
		{"superstate", "", true},
		{"proxy", "", true},
		{"erc4626", "", false},
		{"curve", "", false},
		{"uni_v3_pool", "", false},
		{"anchorage", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.tokenType+"_"+tt.protocol, func(t *testing.T) {
			got := src.Supports(tt.tokenType, tt.protocol)
			if got != tt.want {
				t.Errorf("Supports(%q, %q) = %v, want %v", tt.tokenType, tt.protocol, got, tt.want)
			}
		})
	}
}

func TestBalanceOfSource_FetchBalances(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	contract := common.HexToAddress("0xe7df13b8e3d6740fe17cbe928c7334243d86c92f") // spUSDT
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")   // spark proxy

	expectedBalance := new(big.Int).Mul(big.NewInt(50000), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)) // 50000 USDT

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if len(calls) != 1 {
			t.Fatalf("expected 1 call, got %d", len(calls))
		}
		if calls[0].Target != contract {
			t.Errorf("expected target %s, got %s", contract.Hex(), calls[0].Target.Hex())
		}

		returnData, err := erc20ABI.Methods["balanceOf"].Outputs.Pack(expectedBalance)
		if err != nil {
			t.Fatalf("pack balanceOf output: %v", err)
		}
		return []outbound.Result{{Success: true, ReturnData: returnData}}, nil
	}

	src := NewBalanceOfSource(mc, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))

	entries := []*TokenEntry{
		{
			ContractAddress: contract,
			WalletAddress:   wallet,
			Star:            "spark",
			Chain:           "mainnet",
			Protocol:        "sparklend",
			TokenType:       "erc20",
		},
	}

	result, err := src.FetchBalances(context.Background(), entries, 24720000)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}

	key := entries[0].Key()
	bal, ok := result.Balances[key]
	if !ok {
		t.Fatal("expected result for entry, got none")
	}
	if bal.Balance.Cmp(expectedBalance) != 0 {
		t.Errorf("balance = %s, want %s", bal.Balance, expectedBalance)
	}
	if len(result.Supplies) != 0 {
		t.Errorf("erc20 entry should not produce supply rows, got %d", len(result.Supplies))
	}
}

func TestBalanceOfSource_FetchBalances_Empty(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	src := NewBalanceOfSource(nil, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))

	result, err := src.FetchBalances(context.Background(), nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Balances) != 0 {
		t.Errorf("expected empty results, got %d", len(result.Balances))
	}
}

func TestBalanceOfSource_FetchBalances_FailedCallStoresZero(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: false, ReturnData: nil}}, nil
	}

	src := NewBalanceOfSource(mc, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))

	entries := []*TokenEntry{
		{
			ContractAddress: common.HexToAddress("0xaaaa"),
			WalletAddress:   common.HexToAddress("0xbbbb"),
			TokenType:       "erc20",
		},
	}

	result, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bal, ok := result.Balances[entries[0].Key()]
	if !ok {
		t.Fatal("expected result for entry")
	}
	if bal.Balance.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("failed call should produce zero balance, got %s", bal.Balance)
	}
}

// padUint256 returns a 32-byte big-endian encoding for use as raw return data.
func padUint256(v *big.Int) []byte {
	return common.LeftPadBytes(v.Bytes(), 32)
}

// TestBalanceOfSource_FetchBalances_Atoken asserts the multicall contains exactly
// one balanceOf + scaledBalanceOf pair per atoken entry, plus exactly one
// totalSupply + scaledTotalSupply pair per unique contract, and that the result
// populates both the ScaledBalance and the Supplies map.
func TestBalanceOfSource_FetchBalances_Atoken(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	atoken := common.HexToAddress("0xe7df13b8e3d6740fe17cbe928c7334243d86c92f")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")

	bal := new(big.Int).Mul(big.NewInt(100), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	scaledBal := new(big.Int).Mul(big.NewInt(95), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	totalSupply := new(big.Int).Mul(big.NewInt(1000), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	scaledTotalSupply := new(big.Int).Mul(big.NewInt(980), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if len(calls) != 4 {
			t.Fatalf("expected 4 calls (balanceOf + scaledBalanceOf + totalSupply + scaledTotalSupply), got %d", len(calls))
		}

		// Selectors are the first 4 bytes of calldata. balanceOf selector is
		// known from the ABI.
		balSelector := erc20ABI.Methods["balanceOf"].ID

		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			if c.Target != atoken {
				t.Errorf("call %d target = %s, want %s", i, c.Target.Hex(), atoken.Hex())
			}
			sel := c.CallData[:4]
			switch {
			case bytes.Equal(sel, balSelector):
				rd, err := erc20ABI.Methods["balanceOf"].Outputs.Pack(bal)
				if err != nil {
					t.Fatalf("pack balanceOf: %v", err)
				}
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(sel, scaledBalanceOfSelector):
				results[i] = outbound.Result{Success: true, ReturnData: padUint256(scaledBal)}
			case bytes.Equal(sel, totalSupplySelector):
				results[i] = outbound.Result{Success: true, ReturnData: padUint256(totalSupply)}
			case bytes.Equal(sel, scaledTotalSupplySelector):
				results[i] = outbound.Result{Success: true, ReturnData: padUint256(scaledTotalSupply)}
			default:
				t.Fatalf("unexpected selector %x", sel)
			}
		}
		return results, nil
	}

	src := NewBalanceOfSource(mc, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))

	entries := []*TokenEntry{{
		ContractAddress: atoken,
		WalletAddress:   wallet,
		Star:            "spark",
		Chain:           "mainnet",
		Protocol:        "sparklend",
		TokenType:       "atoken",
	}}

	got, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}

	key := entries[0].Key()
	b, ok := got.Balances[key]
	if !ok {
		t.Fatal("missing balance for atoken entry")
	}
	if b.Balance.Cmp(bal) != 0 {
		t.Errorf("balance = %s, want %s", b.Balance, bal)
	}
	if b.ScaledBalance == nil || b.ScaledBalance.Cmp(scaledBal) != 0 {
		t.Errorf("scaled balance = %v, want %s", b.ScaledBalance, scaledBal)
	}

	sup, ok := got.Supplies[atoken]
	if !ok {
		t.Fatal("missing supply for atoken contract")
	}
	if sup.TotalSupply.Cmp(totalSupply) != 0 {
		t.Errorf("total supply = %s, want %s", sup.TotalSupply, totalSupply)
	}
	if sup.ScaledTotalSupply == nil || sup.ScaledTotalSupply.Cmp(scaledTotalSupply) != 0 {
		t.Errorf("scaled total supply = %v, want %s", sup.ScaledTotalSupply, scaledTotalSupply)
	}
}

// TestBalanceOfSource_FetchBalances_AtokenSupplyDedup verifies that when two
// wallets hold the same atoken contract, exactly one (totalSupply +
// scaledTotalSupply) pair is added to the multicall.
func TestBalanceOfSource_FetchBalances_AtokenSupplyDedup(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	atoken := common.HexToAddress("0xe7df13b8e3d6740fe17cbe928c7334243d86c92f")
	wallet1 := common.HexToAddress("0x1111111111111111111111111111111111111111")
	wallet2 := common.HexToAddress("0x2222222222222222222222222222222222222222")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		// 2 wallets × (balanceOf + scaledBalanceOf) = 4, plus 1 (totalSupply + scaledTotalSupply) = 2.
		if len(calls) != 6 {
			t.Fatalf("expected 6 calls, got %d", len(calls))
		}
		totalSupplyCount := 0
		scaledTotalSupplyCount := 0
		for _, c := range calls {
			sel := c.CallData[:4]
			if bytes.Equal(sel, totalSupplySelector) {
				totalSupplyCount++
			}
			if bytes.Equal(sel, scaledTotalSupplySelector) {
				scaledTotalSupplyCount++
			}
		}
		if totalSupplyCount != 1 {
			t.Errorf("totalSupply calls = %d, want 1 (deduped)", totalSupplyCount)
		}
		if scaledTotalSupplyCount != 1 {
			t.Errorf("scaledTotalSupply calls = %d, want 1 (deduped)", scaledTotalSupplyCount)
		}
		// Return all-success no-data so supply is still emitted.
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			sel := c.CallData[:4]
			if bytes.Equal(sel, erc20ABI.Methods["balanceOf"].ID) {
				rd, _ := erc20ABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(0))
				results[i] = outbound.Result{Success: true, ReturnData: rd}
				continue
			}
			results[i] = outbound.Result{Success: true, ReturnData: padUint256(big.NewInt(1))}
		}
		return results, nil
	}

	src := NewBalanceOfSource(mc, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entries := []*TokenEntry{
		{ContractAddress: atoken, WalletAddress: wallet1, TokenType: "atoken"},
		{ContractAddress: atoken, WalletAddress: wallet2, TokenType: "atoken"},
	}
	got, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	if len(got.Supplies) != 1 {
		t.Errorf("expected 1 supply entry (deduped across wallets), got %d", len(got.Supplies))
	}
}

// TestBalanceOfSource_TotalSupplyFailureDropsSupply asserts that when the
// totalSupply multicall entry fails, no supply row is emitted for that contract
// — but the balance row is still returned so that allocation_position stays
// consistent.
func TestBalanceOfSource_TotalSupplyFailureDropsSupply(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	atoken := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			sel := c.CallData[:4]
			switch {
			case bytes.Equal(sel, erc20ABI.Methods["balanceOf"].ID):
				rd, _ := erc20ABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(100))
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(sel, totalSupplySelector):
				// Simulate failure for totalSupply only.
				results[i] = outbound.Result{Success: false}
			default:
				results[i] = outbound.Result{Success: true, ReturnData: padUint256(big.NewInt(1))}
			}
		}
		return results, nil
	}

	src := NewBalanceOfSource(mc, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entries := []*TokenEntry{{ContractAddress: atoken, WalletAddress: wallet, TokenType: "atoken"}}
	got, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	if _, ok := got.Balances[entries[0].Key()]; !ok {
		t.Error("balance should still be present")
	}
	if _, ok := got.Supplies[atoken]; ok {
		t.Error("no supply row should be emitted when totalSupply failed")
	}
}

// TestBalanceOfSource_ScaledTotalSupplyFailureKeepsRowNil asserts that when only
// scaledTotalSupply fails, a supply row is still emitted with ScaledTotalSupply=nil.
func TestBalanceOfSource_ScaledTotalSupplyFailureKeepsRowNil(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	atoken := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, c := range calls {
			sel := c.CallData[:4]
			switch {
			case bytes.Equal(sel, erc20ABI.Methods["balanceOf"].ID):
				rd, _ := erc20ABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(100))
				results[i] = outbound.Result{Success: true, ReturnData: rd}
			case bytes.Equal(sel, scaledTotalSupplySelector):
				results[i] = outbound.Result{Success: false}
			default:
				results[i] = outbound.Result{Success: true, ReturnData: padUint256(big.NewInt(1234))}
			}
		}
		return results, nil
	}

	src := NewBalanceOfSource(mc, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entries := []*TokenEntry{{ContractAddress: atoken, WalletAddress: wallet, TokenType: "atoken"}}
	got, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	sup, ok := got.Supplies[atoken]
	if !ok {
		t.Fatal("expected supply row; only scaledTotalSupply failed")
	}
	if sup.TotalSupply.Cmp(big.NewInt(1234)) != 0 {
		t.Errorf("total supply = %s, want 1234", sup.TotalSupply)
	}
	if sup.ScaledTotalSupply != nil {
		t.Errorf("scaled total supply should be nil on failure, got %s", sup.ScaledTotalSupply)
	}
}

// TestBalanceOfSource_NoExtraCallsForNonAtoken verifies that erc20 entries do
// NOT pull scaledBalanceOf or the totalSupply pair.
func TestBalanceOfSource_NoExtraCallsForNonAtoken(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x1111")

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		for _, c := range calls {
			sel := c.CallData[:4]
			if bytes.Equal(sel, scaledBalanceOfSelector) {
				t.Error("scaledBalanceOf should not be called for non-atoken")
			}
			if bytes.Equal(sel, totalSupplySelector) {
				t.Error("totalSupply should not be called for non-atoken")
			}
			if bytes.Equal(sel, scaledTotalSupplySelector) {
				t.Error("scaledTotalSupply should not be called for non-atoken")
			}
		}
		rd, _ := erc20ABI.Methods["balanceOf"].Outputs.Pack(big.NewInt(1))
		return []outbound.Result{{Success: true, ReturnData: rd}}, nil
	}

	src := NewBalanceOfSource(mc, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entries := []*TokenEntry{{ContractAddress: usdc, WalletAddress: wallet, TokenType: "erc20"}}
	if _, err := src.FetchBalances(context.Background(), entries, 100); err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
}
