package allocation_tracker

import (
	"strings"
	"testing"
)

// contractChainNames collects the distinct chains carried by the committed contract's token
// entries and ALM proxies. These are the chains that must each be served or acknowledged; any
// that is neither is silently dropped by entriesForChainID/proxiesForChainID.
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

// TestEveryContractChainIsServedOrAcknowledged is the deployment-level guardrail: the
// committed contract carries entries (~$465M today) on chains that no deployed
// allocation-tracker serves, and EntriesAndProxiesForChainID silently drops them. It fails CI
// when a contract chain is in neither servedTrackerChains nor the union of the two acknowledged
// sets, forcing a deploy-and-declare or an explicit acknowledgement instead of a silent hole.
// It runs against the real declarations, so leaving a new contract chain undeclared turns it red.
func TestEveryContractChainIsServedOrAcknowledged(t *testing.T) {
	if err := validateContractChainsServed(
		servedTrackerChains,
		contractChainNames(t),
		allAcknowledgedUnservedChains(),
	); err != nil {
		t.Errorf("contract chains are not all served-or-acknowledged: %v", err)
	}
}

// TestAcknowledgedSetsAreDisjoint enforces the dedup invariant: a chain is acknowledged as
// unserved in exactly one place. Because validateContractChainsServed only ever sees the
// union (allAcknowledgedUnservedChains), a chain double-listed across the two sets would be
// invisible there — so disjointness is checked directly over the two declarations.
func TestAcknowledgedSetsAreDisjoint(t *testing.T) {
	for chain := range acknowledgedUnservedByTrackerChains {
		if acknowledgedUnservedChains[chain] {
			t.Errorf("chain %q is in both acknowledgedUnservedChains (vocabulary-level) and "+
				"acknowledgedUnservedByTrackerChains (deployment-level); keep it in exactly one — "+
				"vocabulary-unknown chains belong only in acknowledgedUnservedChains", chain)
		}
	}
}

// TestValidateContractChainsServed covers the pure guardrail's three rule branches
// independently and combined: a served or acknowledged chain passes; an unserved and
// unacknowledged chain fails; a chain that is both served and acknowledged is flagged stale;
// a served chain absent from entity.ChainIDToName is unresolvable; and multiple faults are
// reported together.
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
			want:           []string{"no deployed allocation-tracker serves", "servedTrackerChains"},
		},
		{
			name:           "served and acknowledged chain is stale",
			served:         map[string]bool{"base": true},
			contractChains: []string{"base"},
			acknowledged:   map[string]bool{"base": true},
			want:           []string{"listed in both", "remove it from acknowledgedUnservedByTrackerChains"},
		},
		{
			name:           "served chain absent from ChainIDToName is unresolvable",
			served:         map[string]bool{"narnia": true},
			contractChains: []string{"narnia"},
			want:           []string{"absent from entity.ChainIDToName"},
		},
		{
			name:           "dropped and stale faults reported together",
			served:         map[string]bool{"base": true},
			contractChains: []string{"base", "plume"},
			acknowledged:   map[string]bool{"base": true},
			want:           []string{"no deployed allocation-tracker serves", "listed in both"},
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

// TestAssertServedTrackerChain covers the startup assertion both directions: a declared served
// chain boots, an undeclared one fails hard with an instruction to update servedTrackerChains.
func TestAssertServedTrackerChain(t *testing.T) {
	tests := []struct {
		name    string
		chain   string
		wantErr bool
	}{
		{"declared served chain boots", "mainnet", false},
		{"acknowledged-but-unserved chain fails", "optimism", true},
		{"unknown chain fails", "narnia", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := AssertServedTrackerChain(tt.chain)
			switch {
			case tt.wantErr && err == nil:
				t.Fatalf("chain %q: expected error, got nil", tt.chain)
			case !tt.wantErr && err != nil:
				t.Fatalf("chain %q: unexpected error: %v", tt.chain, err)
			case tt.wantErr && !strings.Contains(err.Error(), "servedTrackerChains"):
				t.Errorf("chain %q: error %q should name servedTrackerChains", tt.chain, err)
			}
		})
	}
}
