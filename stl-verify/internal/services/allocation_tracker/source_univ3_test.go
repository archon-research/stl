package allocation_tracker

import (
	"context"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// fakeMulticaller returns pre-configured results for testing.
type fakeMulticaller struct {
	results []outbound.Result
	err     error
}

func (f *fakeMulticaller) Execute(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.results, nil
}

func (f *fakeMulticaller) ExecuteAtHash(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.results, nil
}

func (f *fakeMulticaller) Address() common.Address {
	return common.Address{}
}

// ---------------------------------------------------------------------------
// UniV3Source adapter tests
// ---------------------------------------------------------------------------

func TestUniV3Source_Name(t *testing.T) {
	src, err := NewUniV3Source(&fakeMulticaller{}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if src.Name() != "uni-v3" {
		t.Errorf("Name() = %q, want %q", src.Name(), "uni-v3")
	}
}

func TestUniV3Source_Supports(t *testing.T) {
	src, err := NewUniV3Source(&fakeMulticaller{}, slog.Default())
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
	src, err := NewUniV3Source(&fakeMulticaller{}, slog.Default())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := src.FetchBalances(context.Background(), nil, 100, testBlockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Balances) != 0 {
		t.Errorf("expected empty result, got %d", len(result.Balances))
	}
}

func TestUniV3Source_FetchBalances_UnknownChain(t *testing.T) {
	src, err := NewUniV3Source(&fakeMulticaller{}, slog.Default())
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

	result, err := src.FetchBalances(context.Background(), entries, 100, testBlockHash)
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
	mc := &hashRecordingFakeMulticaller{}
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

	if _, err := src.FetchBalances(context.Background(), entries, 100, testBlockHash); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mc.callCount == 0 {
		t.Fatal("expected at least one multicall")
	}
	if mc.executedVia != "hash" {
		t.Fatalf("multicaller invoked via %q, want the hash-pinned path", mc.executedVia)
	}
	if mc.gotHash != testBlockHash {
		t.Fatalf("multicall block hash = %s, want %s", mc.gotHash, testBlockHash)
	}
}

// hashRecordingFakeMulticaller is a test double for outbound.Multicaller that
// records the block hash it was called with via ExecuteAtHash and returns an
// empty-but-successful result batch big enough for any call count, so tests
// can assert the reader pins state reads to the block hash rather than the
// block number, without needing to model exact multicall shapes.
type hashRecordingFakeMulticaller struct {
	callCount   int
	gotHash     common.Hash
	executedVia string // "hash" or "number", whichever method was actually called
}

func (m *hashRecordingFakeMulticaller) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	m.callCount++
	m.executedVia = "number"
	return make([]outbound.Result, len(calls)), nil
}

func (m *hashRecordingFakeMulticaller) ExecuteAtHash(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	m.callCount++
	m.executedVia = "hash"
	m.gotHash = blockHash
	return make([]outbound.Result, len(calls)), nil
}

func (m *hashRecordingFakeMulticaller) Address() common.Address {
	return common.Address{}
}
