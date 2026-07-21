package allocation_tracker

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// contractChainNames collects the distinct chains carried by the committed contract's
// token entries and ALM proxies. These are the chains that must each be served by a
// deployed tracker or acknowledged; any that is neither is silently dropped by
// entriesForChainID/proxiesForChainID.
func contractChainNames(t *testing.T) []string {
	t.Helper()
	seen := make(map[string]bool)
	var chains []string
	add := func(chain string) {
		if seen[chain] {
			return
		}
		seen[chain] = true
		chains = append(chains, chain)
	}
	for _, e := range defaultEntries(t) {
		add(e.Chain)
	}
	for _, p := range defaultProxies(t) {
		add(p.Chain)
	}
	return chains
}

// servedTrackerChainNames resolves the deployed allocation-tracker CHAIN_IDs (parsed from
// the prod ConfigMaps) into internal chain names, the vocabulary the contract uses.
func servedTrackerChainNames(t *testing.T) map[string]bool {
	t.Helper()
	ids, err := deployedTrackerChainIDs()
	if err != nil {
		t.Fatalf("deployed tracker chain IDs: %v", err)
	}
	served := make(map[string]bool, len(ids))
	for _, id := range ids {
		name, err := entity.ChainName(id)
		if err != nil {
			t.Fatalf("deployed tracker on unknown chain ID %d: %v", id, err)
		}
		served[name] = true
	}
	return served
}

// TestDeployedTrackerChainIDs pins the parse of the prod ConfigMaps: exactly one
// allocation-tracker is deployed today, on Ethereum mainnet (chain id 1). Deploying a
// tracker on a new chain (or removing mainnet's) is a deliberate change that must update
// this expectation.
func TestDeployedTrackerChainIDs(t *testing.T) {
	ids, err := deployedTrackerChainIDs()
	if err != nil {
		t.Fatalf("deployed tracker chain IDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("deployed tracker chain IDs = %v, want [1]", ids)
	}
}

// TestEveryContractChainIsServedOrAcknowledged is the deployment-level guardrail this PR
// adds: the committed contract carries entries (~$465M today) on chains that no deployed
// allocation-tracker serves, and EntriesAndProxiesForChainID silently drops them. It fails
// CI when a contract chain is neither served by a deployed tracker nor listed in
// acknowledgedUnservedByTrackerChains — forcing a deploy or an explicit acknowledgement
// instead of a silent hole.
func TestEveryContractChainIsServedOrAcknowledged(t *testing.T) {
	if err := validateContractChainsServed(
		servedTrackerChainNames(t),
		contractChainNames(t),
		acknowledgedUnservedByTrackerChains,
	); err != nil {
		t.Errorf("contract chains are not all served-or-acknowledged: %v", err)
	}
}

// TestValidateContractChainsServed covers the pure guardrail's branches: a served chain
// passes, an acknowledged-but-unserved chain passes (the point of the set), an unserved and
// unacknowledged chain fails, a chain that became served while still acknowledged is flagged
// stale, and both faults are reported together.
func TestValidateContractChainsServed(t *testing.T) {
	tests := []struct {
		name           string
		served         map[string]bool
		contractChains []string
		acknowledged   map[string]bool
		want           []string // substrings; empty means no error expected
	}{
		{
			name:           "served chain passes",
			served:         map[string]bool{"mainnet": true},
			contractChains: []string{"mainnet"},
		},
		{
			name:           "acknowledged unserved chain passes",
			contractChains: []string{"base"},
			acknowledged:   map[string]bool{"base": true},
		},
		{
			name:           "unserved and unacknowledged chain fails",
			contractChains: []string{"base"},
			want:           []string{"no deployed allocation-tracker serves", "acknowledgedUnservedByTrackerChains"},
		},
		{
			name:           "served chain still acknowledged is stale",
			served:         map[string]bool{"base": true},
			contractChains: []string{"base"},
			acknowledged:   map[string]bool{"base": true},
			want:           []string{"still listed in", "acknowledgedUnservedByTrackerChains"},
		},
		{
			name:           "unserved and stale faults reported together",
			served:         map[string]bool{"base": true},
			contractChains: []string{"base", "plume"},
			acknowledged:   map[string]bool{"base": true},
			want:           []string{"no deployed allocation-tracker serves", "still listed in"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateContractChainsServed(tt.served, tt.contractChains, tt.acknowledged)
			if len(tt.want) == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %v, got nil", tt.want)
			}
			for _, sub := range tt.want {
				if !strings.Contains(err.Error(), sub) {
					t.Errorf("error %q missing substring %q", err.Error(), sub)
				}
			}
		})
	}
}
