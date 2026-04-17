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

func TestERC4626Source_Supports(t *testing.T) {
	src, err := NewERC4626Source(nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !src.Supports("erc4626", "sky") {
		t.Fatal("expected erc4626 source to support erc4626 tokens")
	}
	if src.Supports("erc20", "sky") {
		t.Fatal("erc4626 source should not support plain erc20 tokens")
	}
}

func TestERC4626Source_FetchBalances_StoresShareBalance(t *testing.T) {
	contract := common.HexToAddress("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	expectedShares := new(big.Int).Mul(big.NewInt(12345), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))

	mc := testutil.NewMockMulticaller()
	src, err := NewERC4626Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if len(calls) != 1 {
			t.Fatalf("expected 1 call, got %d", len(calls))
		}
		if blockNumber == nil || blockNumber.Cmp(big.NewInt(24584100)) != 0 {
			t.Fatalf("blockNumber = %v, want 24584100", blockNumber)
		}

		returnData, err := src.vaultABI.Methods["balanceOf"].Outputs.Pack(expectedShares)
		if err != nil {
			t.Fatalf("pack balanceOf output: %v", err)
		}
		return []outbound.Result{{Success: true, ReturnData: returnData}}, nil
	}

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
	if mc.CallCount != 1 {
		t.Fatalf("expected exactly one multicall round, got %d", mc.CallCount)
	}

	got := results[entries[0].Key()]
	if got == nil {
		t.Fatal("expected result for entry")
	}
	if got.Balance.Cmp(expectedShares) != 0 {
		t.Fatalf("balance = %s, want %s", got.Balance, expectedShares)
	}
	if got.ScaledBalance == nil || got.ScaledBalance.Cmp(expectedShares) != 0 {
		t.Fatalf("scaled balance = %v, want %s", got.ScaledBalance, expectedShares)
	}
}

func TestERC4626Source_FetchBalances_FailedCallStoresZero(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("failed to load ERC20 ABI: %v", err)
	}

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
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := results[entries[0].Key()]
	if got == nil {
		t.Fatal("expected result for entry")
	}
	if got.Balance.Cmp(big.NewInt(0)) != 0 {
		t.Fatalf("balance = %s, want 0", got.Balance)
	}
	if got.ScaledBalance == nil || got.ScaledBalance.Cmp(big.NewInt(0)) != 0 {
		t.Fatalf("scaled balance = %v, want 0", got.ScaledBalance)
	}

	// Keep a local ERC20 ABI reference so the test fails fast if the shared ABI
	// helpers disappear; the source relies on standard balanceOf semantics.
	_ = erc20ABI
}
