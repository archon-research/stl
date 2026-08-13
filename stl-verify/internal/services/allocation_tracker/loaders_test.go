package allocation_tracker

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
)

// mustDefaultContract loads the committed contract, failing the test on error.
func mustDefaultContract(t *testing.T) *axis_synome_contract.Contract {
	t.Helper()
	contract, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		t.Fatalf("load default axis-synome contract: %v", err)
	}
	return contract
}

// defaultEntries loads the committed contract and converts it to token entries, failing
// the test on error. Replaces the former DefaultTokenEntries() panic wrapper.
func defaultEntries(t *testing.T) []*TokenEntry {
	t.Helper()
	entries, err := tokenEntriesFromContract(mustDefaultContract(t))
	if err != nil {
		t.Fatalf("token entries from contract: %v", err)
	}
	return entries
}

// defaultProxies loads the committed contract and converts it to proxies, failing the
// test on error. Replaces the former DefaultProxies() panic wrapper.
func defaultProxies(t *testing.T) []ProxyConfig {
	t.Helper()
	proxies, err := proxiesFromContract(mustDefaultContract(t))
	if err != nil {
		t.Fatalf("proxies from contract: %v", err)
	}
	return proxies
}
