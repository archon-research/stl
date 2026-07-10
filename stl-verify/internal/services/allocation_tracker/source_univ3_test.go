package allocation_tracker

import (
	"context"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// UniV3Source adapter tests
// ---------------------------------------------------------------------------

func TestUniV3Source_Name(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src.Name() != "uni-v3" {
		t.Errorf("Name() = %q, want %q", src.Name(), "uni-v3")
	}
}

func TestUniV3Source_Supports(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	tests := []struct {
		tokenType string
		protocol  string
		want      bool
	}{
		{"uni_v3_pool", "uniswap", true},
		{"uni_v3_lp", "uniswap", true},
		{"uni_v3_pool", "", true},
		{"uni_v3_lp", "", true},
		{"erc20", "uniswap", false},
		{"erc4626", "", false},
		{"curve", "", false},
	}

	for _, tt := range tests {
		got := src.Supports(tt.tokenType, tt.protocol)
		if got != tt.want {
			t.Errorf("Supports(%q, %q) = %v, want %v", tt.tokenType, tt.protocol, got, tt.want)
		}
	}
}

func TestUniV3Source_FetchBalances_EmptyEntries(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := src.FetchBalances(context.Background(), nil, testBlockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Balances) != 0 {
		t.Errorf("expected empty result, got %d", len(result.Balances))
	}
}

func TestUniV3Source_FetchBalances_UnknownChain(t *testing.T) {
	src, err := NewUniV3Source(testutil.NewMockMulticaller(), slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := []*TokenEntry{
		{
			ContractAddress: common.HexToAddress("0x1111"),
			WalletAddress:   common.HexToAddress("0x2222"),
			Star:            "spark",
			Chain:           "unknown-chain",
			TokenType:       "uni_v3_pool",
		},
	}

	result, err := src.FetchBalances(context.Background(), entries, testBlockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Unknown chain is skipped with a warning, not an error.
	if len(result.Balances) != 0 {
		t.Errorf("expected empty result for unknown chain, got %d", len(result.Balances))
	}
}

// TestUniV3Source_FetchBalances_PinsToBlockHash asserts the NFT-position and
// pool-state reads are pinned to the block hash, not the block number: after a
// reorg an archive node answers eth_call-by-number with the new canonical
// state, which can silently disagree with the reorged (older-version) data
// this fetch is being made for.
func TestUniV3Source_FetchBalances_PinsToBlockHash(t *testing.T) {
	// An empty-but-successful result batch sized to the call count lets the test
	// assert the reader pins state reads to the block hash rather than the block
	// number, without modelling exact multicall shapes.
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return make([]outbound.Result, len(calls)), nil
	}
	src, err := NewUniV3Source(mc, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries := []*TokenEntry{
		{
			ContractAddress: common.HexToAddress("0x1111"),
			WalletAddress:   common.HexToAddress("0x2222"),
			Star:            "spark",
			Chain:           "mainnet",
			TokenType:       "uni_v3_pool",
		},
	}

	if _, err := src.FetchBalances(context.Background(), entries, testBlockHash); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mc.Invocations) == 0 {
		t.Fatal("expected at least one multicall")
	}
	last := mc.Invocations[len(mc.Invocations)-1]
	if !last.ViaHash {
		t.Fatal("multicaller invoked via the number path, want the hash-pinned path")
	}
	if last.BlockHash != testBlockHash {
		t.Fatalf("multicall block hash = %s, want %s", last.BlockHash, testBlockHash)
	}
}
