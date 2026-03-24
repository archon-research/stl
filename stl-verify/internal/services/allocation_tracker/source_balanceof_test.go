package allocation_tracker

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/ethereum/go-ethereum/common"
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
		t.Run(tt.tokenType, func(t *testing.T) {
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
			TokenType:       "atoken",
		},
	}

	results, err := src.FetchBalances(context.Background(), entries, 24720000)
	if err != nil {
		t.Fatalf("FetchBalances failed: %v", err)
	}

	key := entries[0].Key()
	bal, ok := results[key]
	if !ok {
		t.Fatal("expected result for entry, got none")
	}
	if bal.Balance.Cmp(expectedBalance) != 0 {
		t.Errorf("balance = %s, want %s", bal.Balance, expectedBalance)
	}
}

func TestBalanceOfSource_FetchBalances_Empty(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

	src := NewBalanceOfSource(nil, erc20ABI, slog.New(slog.NewTextHandler(io.Discard, nil)))

	results, err := src.FetchBalances(context.Background(), nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty results, got %d", len(results))
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

	results, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bal, ok := results[entries[0].Key()]
	if !ok {
		t.Fatal("expected result for entry")
	}
	if bal.Balance.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("failed call should produce zero balance, got %s", bal.Balance)
	}
}
