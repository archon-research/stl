package allocation_tracker

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/ethereum/go-ethereum/common"
)

// TestNewService_ValidatesScopedEntriesAndProxies exercises the entry/proxy validation
// through the public NewService constructor (the validation itself is unexported; this is
// its public entry point). The fetch/handler dependencies are nil because validation runs
// before they are used.
func TestNewService_ValidatesScopedEntriesAndProxies(t *testing.T) {
	entry := func(contract, wallet, chain string) *TokenEntry {
		return &TokenEntry{
			ContractAddress: common.HexToAddress(contract),
			WalletAddress:   common.HexToAddress(wallet),
			Chain:           chain,
		}
	}
	proxy := func(addr, chain string) ProxyConfig {
		return ProxyConfig{Address: common.HexToAddress(addr), Chain: chain}
	}
	okEntries := []*TokenEntry{entry("0x01", "0x0a", "mainnet")}
	okProxies := []ProxyConfig{proxy("0x0a", "mainnet")}

	tests := []struct {
		name    string
		entries []*TokenEntry
		proxies []ProxyConfig
		chainID int64
		wantErr string // substring; "" means no error expected
	}{
		{"valid", okEntries, okProxies, 1, ""},
		{"unknown chain id", okEntries, okProxies, 9_999_999, "unknown chain ID"},
		{"nil entry", []*TokenEntry{nil}, okProxies, 1, "is nil"},
		{"entry chain mismatch", []*TokenEntry{entry("0x01", "0x0a", "base")}, okProxies, 1, "want mainnet"},
		{
			"duplicate entry",
			[]*TokenEntry{entry("0x01", "0x0a", "mainnet"), entry("0x01", "0x0a", "mainnet")},
			okProxies, 1, "duplicate token entry",
		},
		{"proxy chain mismatch", okEntries, []ProxyConfig{proxy("0x0a", "base")}, 1, "want mainnet"},
		{
			"duplicate proxy",
			okEntries,
			[]ProxyConfig{proxy("0x0a", "mainnet"), proxy("0x0a", "mainnet")},
			1, "duplicate proxy address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(
				Config{ChainID: tt.chainID, Logger: quietLogger()},
				nil, nil, nil, tt.entries, nil, tt.proxies,
			)
			switch {
			case tt.wantErr == "":
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			case err == nil:
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			case !strings.Contains(err.Error(), tt.wantErr):
				t.Errorf("error = %q, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestProxiesFromAlmProxy(t *testing.T) {
	p := func(addr, role string) axis_synome_contract.ProxyConfig {
		return axis_synome_contract.ProxyConfig{Address: addr, Role: role}
	}

	t.Run("flattens, tags role, and sorts by star", func(t *testing.T) {
		in := map[string]map[string][]axis_synome_contract.ProxyConfig{
			"spark": {"mainnet": {
				p("0x1601843c5e9bc251a3272907010afa41fa18347e", "alm"),
				p("0x3300f198988e4c9c63f75df86de36421f06af8c4", "subproxy"),
			}},
			"grove": {"mainnet": {p("0x491edfb0b8b608044e227225c715981a30f3a44e", "alm")}},
		}
		got, err := proxiesFromAlmProxy(in)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("got %d proxies, want 3", len(got))
		}
		if got[0].Star != "grove" {
			t.Errorf("first proxy star = %q, want grove (sorted by star)", got[0].Star)
		}
		if got[0].Role != "alm" {
			t.Errorf("role not threaded through: %q", got[0].Role)
		}
	})

	t.Run("rejects duplicate (chain, address) across stars", func(t *testing.T) {
		dup := "0x00000000000000000000000000000000000000aa"
		in := map[string]map[string][]axis_synome_contract.ProxyConfig{
			"spark": {"mainnet": {p(dup, "alm")}},
			"grove": {"mainnet": {p(dup, "alm")}},
		}
		if _, err := proxiesFromAlmProxy(in); err == nil || !strings.Contains(err.Error(), "duplicate proxy address") {
			t.Fatalf("want duplicate-proxy error, got %v", err)
		}
	})

	t.Run("rejects empty", func(t *testing.T) {
		if _, err := proxiesFromAlmProxy(map[string]map[string][]axis_synome_contract.ProxyConfig{}); err == nil ||
			!strings.Contains(err.Error(), "no ALM proxies") {
			t.Fatalf("want empty error, got %v", err)
		}
	})
}
