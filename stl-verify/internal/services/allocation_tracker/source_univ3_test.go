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

	result, err := src.FetchBalances(context.Background(), nil, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty result, got %d", len(result))
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

	result, err := src.FetchBalances(context.Background(), entries, 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Unknown chain is skipped with a warning, not an error.
	if len(result) != 0 {
		t.Errorf("expected empty result for unknown chain, got %d", len(result))
	}
}
