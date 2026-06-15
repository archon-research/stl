package allocation_tracker

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
)

// TestEntriesAndProxiesForChainID covers the worker's startup resolution and its empty-set
// error branches (formerly inline in main.go and untested).
func TestEntriesAndProxiesForChainID(t *testing.T) {
	// Valid: Ethereum mainnet (chain id 1) has both entries and proxies.
	entries, proxies, err := EntriesAndProxiesForChainID(mustDefaultContract(t), 1)
	if err != nil {
		t.Fatalf("mainnet: unexpected error: %v", err)
	}
	if len(entries) == 0 || len(proxies) == 0 {
		t.Fatalf("mainnet: got %d entries, %d proxies; want both > 0", len(entries), len(proxies))
	}

	// Unknown chain id resolves to no entries -> clear error.
	if _, _, err := EntriesAndProxiesForChainID(mustDefaultContract(t), 9_999_999); err == nil ||
		!strings.Contains(err.Error(), "no token entries for chain ID") {
		t.Fatalf("unknown chain: want 'no token entries' error, got %v", err)
	}

	// Entries on a chain that has no proxy for it -> 'no proxies for chain ID'.
	synthetic := &axis_synome_contract.Contract{
		Version:             "v1",
		AxisSynomeGitCommit: "test",
		AxisSynome: axis_synome_contract.AxisSynomeModel{Spec: axis_synome_contract.SpecModel{Entities: &axis_synome_contract.EntitiesModel{
			AssetsByPrime: axis_synome_contract.AssetsByPrimeModel{ASSETSByPrime: map[string][]axis_synome_contract.TokenEntry{
				"spark": {{
					ContractAddress: "0x0000000000000000000000000000000000000001",
					WalletAddress:   "0x000000000000000000000000000000000000000a",
					Chain:           "mainnet",
					TokenType:       "erc20",
					Star:            "spark",
				}},
			}},
			AlmProxies: axis_synome_contract.AlmProxiesModel{AlmProxy: map[string]map[string][]axis_synome_contract.ProxyConfig{
				// A proxy exists, but on a different chain than the entry.
				"spark": {"base": {{Star: "spark", Chain: "base", Address: "0x000000000000000000000000000000000000000b", Role: "alm"}}},
			}},
		}}},
	}
	if _, _, err := EntriesAndProxiesForChainID(synthetic, 1); err == nil ||
		!strings.Contains(err.Error(), "no proxies for chain ID") {
		t.Fatalf("entries-without-proxy: want 'no proxies for chain ID' error, got %v", err)
	}
}

// contractWithEntry builds a minimal synthetic contract holding a single token entry
// under the given ASSETS_BY_PRIME key, for exercising tokenEntriesFromContract's
// load-time rejection branches.
func contractWithEntry(key string, entry axis_synome_contract.TokenEntry) *axis_synome_contract.Contract {
	return &axis_synome_contract.Contract{
		Version:             "v1",
		AxisSynomeGitCommit: "test",
		AxisSynome: axis_synome_contract.AxisSynomeModel{Spec: axis_synome_contract.SpecModel{Entities: &axis_synome_contract.EntitiesModel{
			AssetsByPrime: axis_synome_contract.AssetsByPrimeModel{ASSETSByPrime: map[string][]axis_synome_contract.TokenEntry{
				key: {entry},
			}},
		}}},
	}
}

// TestTokenEntriesFromContract_Rejects covers the converter's two load-time guardrails:
// an entry whose own Star disagrees with its ASSETS_BY_PRIME key (silent mis-attribution),
// and an entry on a chain that is neither configurable nor acknowledged (silently dropped
// forever by entriesForChainID). Both must fail loudly at load.
func TestTokenEntriesFromContract_Rejects(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		entry   axis_synome_contract.TokenEntry
		wantErr string
	}{
		{
			name: "star disagrees with ASSETS_BY_PRIME key",
			key:  "spark",
			entry: axis_synome_contract.TokenEntry{
				ContractAddress: "0x0000000000000000000000000000000000000001",
				WalletAddress:   "0x000000000000000000000000000000000000000a",
				Chain:           "mainnet",
				TokenType:       "erc20",
				Star:            "grove",
			},
			wantErr: "does not match its ASSETS_BY_PRIME key",
		},
		{
			name: "unrecognised chain",
			key:  "spark",
			entry: axis_synome_contract.TokenEntry{
				ContractAddress: "0x0000000000000000000000000000000000000001",
				WalletAddress:   "0x000000000000000000000000000000000000000a",
				Chain:           "narnia",
				TokenType:       "erc20",
				Star:            "spark",
			},
			wantErr: "unrecognised chain",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tokenEntriesFromContract(contractWithEntry(tt.key, tt.entry))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("want error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}
