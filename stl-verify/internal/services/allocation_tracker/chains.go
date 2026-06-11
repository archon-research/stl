package allocation_tracker

import (
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

// chainIsKnown reports whether a chain is either configurable (present in
// entity.ChainIDToName, so a tracker can index it) or an acknowledged not-yet-served
// chain.
func chainIsKnown(chain string) bool {
	if acknowledgedUnservedChains[chain] {
		return true
	}
	for _, name := range entity.ChainIDToName {
		if name == chain {
			return true
		}
	}
	return false
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
