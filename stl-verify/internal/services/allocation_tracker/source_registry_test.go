package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// recordingHandler captures slog records for assertions. Tests using it call the
// registry sequentially, so it needs no locking.
type recordingHandler struct{ records []slog.Record }

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) countWarn(substr string) int {
	n := 0
	for _, r := range h.records {
		if r.Level == slog.LevelWarn && strings.Contains(r.Message, substr) {
			n++
		}
	}
	return n
}

// mockSource is a mock PositionSource for testing the registry.
type mockSource struct {
	name       string
	tokenTypes map[string]bool
	result     *FetchResult
	err        error
	returnNil  bool // return (nil, nil) — a contract violation the registry must surface
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
	if m.returnNil {
		return nil, nil
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

// TestSourceRegistry_FetchAll_NilResultIsError covers the (nil, nil) contract
// violation: a source returning no result and no error would otherwise silently drop
// its whole group, so FetchAll must surface it as a fetch failure rather than swallow it.
func TestSourceRegistry_FetchAll_NilResultIsError(t *testing.T) {
	registry := NewSourceRegistry(slog.Default())
	registry.Register(&mockSource{
		name:       "erc20",
		tokenTypes: map[string]bool{"erc20": true},
		returnNil:  true,
	})

	entries := []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1111"), WalletAddress: common.HexToAddress("0xaaaa"), TokenType: "erc20"},
	}

	_, err := registry.FetchAll(context.Background(), entries, 0)
	if err == nil || !strings.Contains(err.Error(), "returned nil result without error") {
		t.Fatalf("want nil-result error, got %v", err)
	}
}

func TestSourceRegistry_FetchAll_WarnsOnceForStubRouted(t *testing.T) {
	h := &recordingHandler{}
	logger := slog.New(h)
	registry := NewSourceRegistry(logger)
	stub := NewStubSource("psm3", "psm3", logger)
	registry.Register(stub)

	// Two psm3 entries (distinct contracts) fetched over two sweeps: the stub
	// matches and records nothing, so the warning must fire exactly once.
	entries := []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1"), WalletAddress: common.HexToAddress("0xa"), TokenType: "psm3", Protocol: "psm3"},
		{ContractAddress: common.HexToAddress("0x2"), WalletAddress: common.HexToAddress("0xb"), TokenType: "psm3", Protocol: "psm3"},
	}
	for i := range 2 {
		if _, err := registry.FetchAll(context.Background(), entries, int64(i)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if got := h.countWarn("not-yet-implemented stub"); got != 1 {
		t.Errorf("stub warning fired %d times, want exactly 1 (deduped per type/protocol)", got)
	}
}

// placeholderMockSource is a mockSource that also satisfies placeholderSource, so
// the registry treats it like a StubSource while still letting us observe calls.
type placeholderMockSource struct{ mockSource }

func (m *placeholderMockSource) isPlaceholder() {}

func TestSourceRegistry_FetchAll_StubStillFetchedAfterWarn(t *testing.T) {
	h := &recordingHandler{}
	registry := NewSourceRegistry(slog.New(h))
	stub := &placeholderMockSource{mockSource{name: "psm3", tokenTypes: map[string]bool{"psm3": true}}}
	registry.Register(stub)

	entries := []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1"), WalletAddress: common.HexToAddress("0xa"), TokenType: "psm3", Protocol: "psm3"},
	}
	if _, err := registry.FetchAll(context.Background(), entries, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := h.countWarn("not-yet-implemented stub"); got != 1 {
		t.Errorf("stub warning fired %d times, want 1", got)
	}
	if stub.called != 1 {
		t.Errorf("stub fetched %d times, want 1 (warned but still run)", stub.called)
	}
}

func TestSourceRegistry_FetchAll_SkipSourceDoesNotWarn(t *testing.T) {
	h := &recordingHandler{}
	logger := slog.New(h)
	registry := NewSourceRegistry(logger)
	registry.Register(NewSkipSource("anchorage-skip", "anchorage", nil, logger))

	entries := []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1"), WalletAddress: common.HexToAddress("0xa"), TokenType: "anchorage", Protocol: "anchorage"},
	}
	if _, err := registry.FetchAll(context.Background(), entries, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A SkipSource's work is intentionally done elsewhere, so it must not warn.
	if got := h.countWarn("not-yet-implemented stub"); got != 0 {
		t.Errorf("skip source emitted %d stub warnings, want 0", got)
	}
	if got := h.countWarn("unsupported entry skipped"); got != 0 {
		t.Errorf("skip source emitted %d unsupported warnings, want 0", got)
	}
}

func TestSourceRegistry_FetchAll_WarnsOnceForUnsupported(t *testing.T) {
	h := &recordingHandler{}
	registry := NewSourceRegistry(slog.New(h))

	entries := []*TokenEntry{
		{ContractAddress: common.HexToAddress("0x1"), WalletAddress: common.HexToAddress("0xa"), TokenType: "mystery", Protocol: "x"},
		{ContractAddress: common.HexToAddress("0x2"), WalletAddress: common.HexToAddress("0xb"), TokenType: "mystery", Protocol: "x"},
	}
	for i := range 2 {
		if _, err := registry.FetchAll(context.Background(), entries, int64(i)); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if got := h.countWarn("unsupported entry skipped"); got != 1 {
		t.Errorf("unsupported warning fired %d times, want exactly 1", got)
	}
}
