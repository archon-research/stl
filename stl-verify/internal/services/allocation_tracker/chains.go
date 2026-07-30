package allocation_tracker

import (
	"errors"
	"fmt"
	"sort"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// acknowledgedUnservedChains are chains intentionally present in the axis-synome
// contract but not yet wired into entity.ChainIDToName, so no allocation tracker is
// configured for them today (their entries are filtered out by entriesForChainID for
// every configured chain). They are allowed past load-time validation; a genuinely new
// chain must be either enabled (added to ChainIDToName) or acknowledged here.
//
// This is what keeps a typo'd chain string (e.g. "avalanche" vs "avalanche-c") in a
// future regeneration from silently dropping a financial position forever: such a chain
// is in neither set, so load fails loudly.
var acknowledgedUnservedChains = map[string]bool{
	"monad":  true, // VEC-315: enablement pending (chain id 143 absent from ChainIDToName)
	"plasma": true, // not yet served
	"plume":  true, // centrifuge_feeder only; not yet served
}

// servedTrackerChains, acknowledgedUnservedByTrackerChains and acknowledgedUnservedChains
// (above) partition the chains the axis-synome contract can carry: each chain belongs to
// exactly ONE of them, and the served-chain guardrail (validateContractChainsServed) fails CI
// if a contract chain belongs to none — turning a regeneration that adds a chain into a
// deliberate choice instead of a silent drop by entriesForChainID.
//
//   - servedTrackerChains                 — a deployed tracker instance indexes it.
//   - acknowledgedUnservedByTrackerChains — the code recognises it (it is in
//     entity.ChainIDToName) but no tracker is deployed for it yet.
//   - acknowledgedUnservedChains          — the code does not recognise the chain string at
//     all (absent from entity.ChainIDToName), so it is unserved a fortiori.
//
// The guardrail's notion of "acknowledged unserved" is the union of the two acknowledged sets
// (allAcknowledgedUnservedChains): a not-in-vocabulary chain is unserved a fortiori, so it is
// NOT duplicated into the deployment-level set. TestAcknowledgedSetsAreDisjoint keeps the two
// acknowledged sets disjoint so no chain is double-listed.
//
// servedTrackerChains is not derived from the k8s manifests — it is kept honest from the other
// side by AssertServedTrackerChain, which every tracker instance runs at boot: an instance
// whose CHAIN_ID chain is not declared here crash-loops immediately, so a real deployment
// cannot exist without its entry. Accepted residual: the reverse — deleting a deployment while
// leaving its chain listed here — is NOT detectable in CI (nothing observes the manifests); the
// declaration and the manifests must travel in the same PR, a convention enforced by review.
var servedTrackerChains = map[string]bool{
	"mainnet":     true, // prime-allocation-indexer (CHAIN_ID 1)
	"avalanche-c": true, // avalanche-allocation-tracker (CHAIN_ID 43114, VEC-499)
	"base":        true, // base-allocation-tracker (CHAIN_ID 8453, VEC-499)
}

// acknowledgedUnservedByTrackerChains lists vocabulary-known contract chains (in
// entity.ChainIDToName) that a tracker could index but none is deployed for yet: their entries
// are knowingly dropped by entriesForChainID. A chain moves from here into servedTrackerChains
// in the same PR that deploys its tracker; the staleness rule in validateContractChainsServed
// fails CI if a chain ends up both served and acknowledged. Vocabulary-UNKNOWN unserved chains
// live in acknowledgedUnservedChains, not here (see the partition note above).
var acknowledgedUnservedByTrackerChains = map[string]bool{
	"arbitrum": true, // no allocation-tracker deployed yet
	"optimism": true, // no allocation-tracker deployed yet
	"unichain": true, // no allocation-tracker deployed yet
}

// allAcknowledgedUnservedChains is the set of chains it is acceptable for no tracker to serve:
// the union of the vocabulary-unknown acknowledgements (acknowledgedUnservedChains, unserved a
// fortiori) and the vocabulary-known-but-undeployed ones (acknowledgedUnservedByTrackerChains).
// validateContractChainsServed takes this as its `acknowledged` argument.
func allAcknowledgedUnservedChains() map[string]bool {
	merged := make(map[string]bool, len(acknowledgedUnservedChains)+len(acknowledgedUnservedByTrackerChains))
	for chain := range acknowledgedUnservedChains {
		merged[chain] = true
	}
	for chain := range acknowledgedUnservedByTrackerChains {
		merged[chain] = true
	}
	return merged
}

// chainIsConfigurable reports whether a chain resolves to a CHAIN_ID in
// entity.ChainIDToName, i.e. a tracker could be configured to index it.
func chainIsConfigurable(chain string) bool {
	for _, name := range entity.ChainIDToName {
		if name == chain {
			return true
		}
	}
	return false
}

// chainIsKnown reports whether a chain is either configurable (present in
// entity.ChainIDToName, so a tracker can index it) or an acknowledged not-yet-served
// chain.
func chainIsKnown(chain string) bool {
	return acknowledgedUnservedChains[chain] || chainIsConfigurable(chain)
}

// validateChainVocabulary fails if any loaded position sits on an empty or unrecognised
// chain. entriesForChainID / proxiesForChainID silently drop entries whose chain matches
// no configured chainID, so this is the trust boundary that turns "silently untracked
// forever" into a loud load-time failure. chainCounts maps chain -> number of positions.
func validateChainVocabulary(what string, chainCounts map[string]int) error {
	var unknown []string
	for chain, n := range chainCounts {
		if chain == "" || !chainIsKnown(chain) {
			unknown = append(unknown, fmt.Sprintf("%q (%d %s)", chain, n, what))
		}
	}
	if len(unknown) == 0 {
		return nil
	}
	sort.Strings(unknown)
	return fmt.Errorf(
		"axis-synome contract has %s on unrecognised chain(s) %v: add the chain to "+
			"entity.ChainIDToName to serve it, or to acknowledgedUnservedChains if intentionally pending",
		what, unknown,
	)
}

// validateContractChainsServed is the deployment-level guardrail over the served/acknowledged
// declarations. It enforces three rules, each with an instructive error naming the exact edit:
//   - every contract chain is served or acknowledged (else its positions are silently dropped);
//   - no chain is both served and acknowledged (a served chain is no longer unserved);
//   - every served chain resolves in entity.ChainIDToName (else a tracker cannot get a CHAIN_ID).
//
// It is pure: served, contractChains and acknowledged are supplied by the caller.
func validateContractChainsServed(served map[string]bool, contractChains []string, acknowledged map[string]bool) error {
	var errs []error

	var dropped []string
	seen := make(map[string]bool)
	for _, chain := range contractChains {
		if seen[chain] {
			continue
		}
		seen[chain] = true
		if served[chain] || acknowledged[chain] {
			continue
		}
		dropped = append(dropped, chain)
	}
	if len(dropped) > 0 {
		sort.Strings(dropped)
		errs = append(errs, fmt.Errorf(
			"axis-synome contract has entries on chain(s) %v that no deployed allocation-tracker serves: "+
				"deploy a tracker and add the chain to servedTrackerChains, or add it to "+
				"acknowledgedUnservedByTrackerChains if it is intentionally not served yet",
			dropped,
		))
	}

	var stale []string
	for chain := range acknowledged {
		if served[chain] {
			stale = append(stale, chain)
		}
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		errs = append(errs, fmt.Errorf(
			"chain(s) %v are listed in both servedTrackerChains and acknowledgedUnservedByTrackerChains: "+
				"a served chain is no longer unserved — remove it from acknowledgedUnservedByTrackerChains",
			stale,
		))
	}

	var unresolvable []string
	for chain := range served {
		if !chainIsConfigurable(chain) {
			unresolvable = append(unresolvable, chain)
		}
	}
	if len(unresolvable) > 0 {
		sort.Strings(unresolvable)
		errs = append(errs, fmt.Errorf(
			"servedTrackerChains lists chain(s) %v absent from entity.ChainIDToName: a tracker cannot "+
				"resolve a CHAIN_ID for them — add them to entity.ChainIDToName or remove them from servedTrackerChains",
			unresolvable,
		))
	}

	return errors.Join(errs...)
}

// AssertServedTrackerChain is the env-var half of the guardrail: every allocation-tracker
// instance calls it at boot with its own resolved chain. It fails hard when the chain is not
// declared in servedTrackerChains, so a tracker deployed without updating the declaration
// crash-loops immediately (rather than the served-chain guardrail silently believing that
// chain is still unserved). This is what keeps servedTrackerChains honest from the deployment
// side without parsing the k8s manifests.
func AssertServedTrackerChain(chain string) error {
	if servedTrackerChains[chain] {
		return nil
	}
	return fmt.Errorf(
		"allocation-tracker booted on chain %q, which is not declared in servedTrackerChains: add %q to "+
			"servedTrackerChains in internal/services/allocation_tracker/chains.go so the served-chain guardrail "+
			"stays honest",
		chain, chain,
	)
}
