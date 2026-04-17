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

func TestCurveSource_Supports(t *testing.T) {
	curveABI, err := abis.GetCurvePoolABI()
	if err != nil {
		t.Fatalf("failed to load curve ABI: %v", err)
	}

	src := NewCurveSource(nil, curveABI, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !src.Supports("curve", "curve") {
		t.Fatal("expected curve source to support curve token type")
	}
	if src.Supports("erc20", "curve") {
		t.Fatal("curve source should not support erc20 token type")
	}
}

func TestCurveSource_FetchBalances_StoresLPBalance(t *testing.T) {
	curveABI, err := abis.GetCurvePoolABI()
	if err != nil {
		t.Fatalf("failed to load curve ABI: %v", err)
	}

	contract := common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	expectedShares, _ := new(big.Int).SetString("1759386773255205923032", 10)

	mc := testutil.NewMockMulticaller()
	src := NewCurveSource(mc, curveABI, slog.New(slog.NewTextHandler(io.Discard, nil)))
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if len(calls) != 1 {
			t.Fatalf("expected 1 call, got %d", len(calls))
		}
		if blockNumber == nil || blockNumber.Cmp(big.NewInt(24584100)) != 0 {
			t.Fatalf("blockNumber = %v, want 24584100", blockNumber)
		}

		returnData, err := src.poolABI.Methods["balanceOf"].Outputs.Pack(expectedShares)
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
		Protocol:        "curve",
		TokenType:       "curve",
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

func TestCurveSource_FetchBalances_FailedCallStoresZero(t *testing.T) {
	curveABI, err := abis.GetCurvePoolABI()
	if err != nil {
		t.Fatalf("failed to load curve ABI: %v", err)
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		return []outbound.Result{{Success: false, ReturnData: nil}}, nil
	}

	src := NewCurveSource(mc, curveABI, slog.New(slog.NewTextHandler(io.Discard, nil)))
	entries := []*TokenEntry{{
		ContractAddress: common.HexToAddress("0xaaaa"),
		WalletAddress:   common.HexToAddress("0xbbbb"),
		TokenType:       "curve",
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
}
