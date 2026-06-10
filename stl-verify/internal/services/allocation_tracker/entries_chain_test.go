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
		AxisSynome: axis_synome_contract.AxisSynomeModel{Spec: axis_synome_contract.SpecModel{ASC: axis_synome_contract.ASCModel{Entities: axis_synome_contract.EntitiesModel{
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
		}}}},
	}
	if _, _, err := EntriesAndProxiesForChainID(synthetic, 1); err == nil ||
		!strings.Contains(err.Error(), "no proxies for chain ID") {
		t.Fatalf("entries-without-proxy: want 'no proxies for chain ID' error, got %v", err)
	}
}
