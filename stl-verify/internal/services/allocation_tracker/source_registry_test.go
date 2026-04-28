package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// mockSource is a mock PositionSource for testing the registry.
type mockSource struct {
	name       string
	tokenTypes map[string]bool
	result     *FetchResult
	err        error
	called     int
}

func (m *mockSource) Name() string { return m.name }

func (m *mockSource) Supports(tokenType, protocol string) bool {
	return m.tokenTypes[tokenType]
}

func (m *mockSource) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (*FetchResult, error) {
	m.called++
	if m.err != nil {
		return nil, m.err
	}
	if m.result == nil {
		return NewFetchResult(), nil
	}
	return m.result, nil
}

func TestSourceRegistry_Route(t *testing.T) {
	logger := slog.Default()
	registry := NewSourceRegistry(logger)

	erc20Source := &mockSource{name: "erc20", tokenTypes: map[string]bool{"erc20": true}}
	erc4626Source := &mockSource{name: "erc4626", tokenTypes: map[string]bool{"erc4626": true}}

	registry.Register(erc20Source)
	registry.Register(erc4626Source)

	tests := []struct {
		tokenType string
		wantName  string
	}{
		{"erc20", "erc20"},
		{"erc4626", "erc4626"},
		{"unknown", ""},
	}

	for _, tt := range tests {
		entry := &TokenEntry{TokenType: tt.tokenType}
		source := registry.Route(entry)
		if tt.wantName == "" {
			if source != nil {
				t.Errorf("Route(%q) should return nil, got %s", tt.tokenType, source.Name())
			}
		} else {
			if source == nil {
				t.Errorf("Route(%q) returned nil, want %s", tt.tokenType, tt.wantName)
			} else if source.Name() != tt.wantName {
				t.Errorf("Route(%q) = %s, want %s", tt.tokenType, source.Name(), tt.wantName)
			}
		}
	}
}

func TestSourceRegistry_FetchAll_GroupsBySource(t *testing.T) {
	logger := slog.Default()
	registry := NewSourceRegistry(logger)

	contract1 := common.HexToAddress("0x1111")
	wallet1 := common.HexToAddress("0xaaaa")
	contract2 := common.HexToAddress("0x2222")
	wallet2 := common.HexToAddress("0xbbbb")

	key1 := EntryKey{ContractAddress: contract1, WalletAddress: wallet1}
	key2 := EntryKey{ContractAddress: contract2, WalletAddress: wallet2}

	result1 := NewFetchResult()
	result1.Balances[key1] = &PositionBalance{Balance: big.NewInt(100)}
	result1.Supplies[contract1] = &PoolSupply{TotalSupply: big.NewInt(999)}

	result2 := NewFetchResult()
	result2.Balances[key2] = &PositionBalance{Balance: big.NewInt(200)}

	erc20Source := &mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		result:     result1,
	}
	erc4626Source := &mockSource{
		name:       "erc4626",
		tokenTypes: map[string]bool{"erc4626": true},
		result:     result2,
	}

	registry.Register(erc20Source)
	registry.Register(erc4626Source)

	entries := []*TokenEntry{
		{ContractAddress: contract1, WalletAddress: wallet1, TokenType: "erc20"},
		{ContractAddress: contract2, WalletAddress: wallet2, TokenType: "erc4626"},
	}

	results, err := registry.FetchAll(context.Background(), entries, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results.Balances) != 2 {
		t.Fatalf("expected 2 balance entries, got %d", len(results.Balances))
	}

	if results.Balances[key1].Balance.Cmp(big.NewInt(100)) != 0 {
		t.Errorf("erc20 balance = %s, want 100", results.Balances[key1].Balance.String())
	}
	if results.Balances[key2].Balance.Cmp(big.NewInt(200)) != 0 {
		t.Errorf("erc4626 balance = %s, want 200", results.Balances[key2].Balance.String())
	}

	if len(results.Supplies) != 1 {
		t.Fatalf("expected 1 supply entry, got %d", len(results.Supplies))
	}
	if results.Supplies[contract1].TotalSupply.Cmp(big.NewInt(999)) != 0 {
		t.Errorf("total supply = %s, want 999", results.Supplies[contract1].TotalSupply)
	}

	if erc20Source.called != 1 {
		t.Errorf("erc20 source called %d times, want 1", erc20Source.called)
	}
	if erc4626Source.called != 1 {
		t.Errorf("erc4626 source called %d times, want 1", erc4626Source.called)
	}
}

func TestSourceRegistry_FetchAll_SkipsUnsupported(t *testing.T) {
	logger := slog.Default()
	registry := NewSourceRegistry(logger)

	src := &mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		result:     NewFetchResult(),
	}
	registry.Register(src)

	entries := []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1111"), WalletAddress: common.HexToAddress("0xaaaa"), TokenType: "unknown_type"},
	}

	results, err := registry.FetchAll(context.Background(), entries, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(results.Balances) != 0 {
		t.Errorf("unsupported entries should be skipped, got %d results", len(results.Balances))
	}
	if src.called != 0 {
		t.Errorf("source should not be called for unsupported entry")
	}
}

func TestSourceRegistry_FetchAll_PartialFailure(t *testing.T) {
	logger := slog.Default()
	registry := NewSourceRegistry(logger)

	contract1 := common.HexToAddress("0x1111")
	wallet1 := common.HexToAddress("0xaaaa")
	key1 := EntryKey{ContractAddress: contract1, WalletAddress: wallet1}

	okRes := NewFetchResult()
	okRes.Balances[key1] = &PositionBalance{Balance: big.NewInt(100)}

	goodSource := &mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		result:     okRes,
	}
	badSource := &mockSource{
		name:       "erc4626",
		tokenTypes: map[string]bool{"erc4626": true},
		err:        fmt.Errorf("rpc timeout"),
	}

	registry.Register(goodSource)
	registry.Register(badSource)

	entries := []*TokenEntry{
		{ContractAddress: contract1, WalletAddress: wallet1, TokenType: "erc20"},
		{ContractAddress: common.HexToAddress("0x2222"), WalletAddress: common.HexToAddress("0xbbbb"), TokenType: "erc4626"},
	}

	results, err := registry.FetchAll(context.Background(), entries, 0)

	// Should return partial results + error
	if err == nil {
		t.Error("expected error for partial failure")
	}
	if len(results.Balances) != 1 {
		t.Errorf("expected 1 partial balance, got %d", len(results.Balances))
	}
	if results.Balances[key1].Balance.Cmp(big.NewInt(100)) != 0 {
		t.Errorf("good source result should still be present")
	}
}
