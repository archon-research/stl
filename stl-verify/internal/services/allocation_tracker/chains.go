package allocation_tracker

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"gopkg.in/yaml.v3"
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

// acknowledgedUnservedByTrackerChains are chains present in the axis-synome contract that no
// deployed allocation-tracker serves: no ConfigMap in k8s/overlays/prod/configmaps.yaml
// stands up a tracker on their CHAIN_ID, so entriesForChainID silently drops their entries.
//
// This set is DEPLOYMENT-level and is distinct from acknowledgedUnservedChains above, which
// is VOCABULARY-level. acknowledgedUnservedChains answers "does the tracker code even
// recognise this chain string?" (is it in entity.ChainIDToName) and gates
// validateChainVocabulary. This set answers "is a tracker actually deployed for this chain?"
// and gates validateContractChainsServed. The two overlap: a chain the code does not
// recognise (monad/plasma/plume) is unserved a fortiori, so it appears in both; a chain the
// code recognises but has not deployed a tracker for (base, arbitrum, ...) appears only here.
//
// validateContractChainsServed fails CI when a contract chain is in neither the served set
// nor here, and again once a chain here becomes served (staleness), so an entry can never be
// silently dropped and the set cannot rot as trackers come online.
var acknowledgedUnservedByTrackerChains = map[string]bool{
	// Recognised chains (in entity.ChainIDToName) a tracker could index, none deployed yet.
	"avalanche-c": true, // VEC-499 tracker instance in flight
	"base":        true, // VEC-499 tracker instance in flight
	"arbitrum":    true, // no allocation-tracker deployed yet
	"optimism":    true, // no allocation-tracker deployed yet
	"unichain":    true, // no allocation-tracker deployed yet
	// Chains absent from entity.ChainIDToName (also in acknowledgedUnservedChains), unserved
	// a fortiori until enabled.
	"monad":  true, // VEC-315: enablement pending
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

// deployedTrackerChainIDs reports the CHAIN_IDs that a deployed allocation-tracker serves,
// read from the committed prod ConfigMaps (k8s/overlays/prod/configmaps.yaml). It collects
// the CHAIN_ID of every ConfigMap whose name ends with "allocation-tracker-config" — the
// flat multi-doc overlay needs no kustomize render. It fails loudly if the file is missing
// or holds no such ConfigMap, so the guardrail can never silently treat every chain as
// unserved. Today it returns exactly [1] (mainnet).
func deployedTrackerChainIDs() ([]int64, error) {
	path, err := prodConfigMapsPath()
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open prod configmaps %q: %w", path, err)
	}
	defer file.Close()

	type configMap struct {
		Kind     string `yaml:"kind"`
		Metadata struct {
			Name string `yaml:"name"`
		} `yaml:"metadata"`
		Data map[string]string `yaml:"data"`
	}

	var ids []int64
	decoder := yaml.NewDecoder(file)
	for {
		var doc configMap
		if err := decoder.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode prod configmaps %q: %w", path, err)
		}
		if doc.Kind != "ConfigMap" || !strings.HasSuffix(doc.Metadata.Name, "allocation-tracker-config") {
			continue
		}
		raw, ok := doc.Data["CHAIN_ID"]
		if !ok {
			return nil, fmt.Errorf("allocation-tracker ConfigMap %q in %q has no CHAIN_ID", doc.Metadata.Name, path)
		}
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("allocation-tracker ConfigMap %q has non-numeric CHAIN_ID %q: %w", doc.Metadata.Name, raw, err)
		}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("no allocation-tracker-config ConfigMap found in %q", path)
	}
	slices.Sort(ids)
	return ids, nil
}

func prodConfigMapsPath() (string, error) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("resolve prod configmaps path: runtime caller unavailable")
	}
	// chains.go lives at stl-verify/internal/services/allocation_tracker; the k8s overlays
	// sit at the repo root, four directories up.
	return filepath.Clean(filepath.Join(
		filepath.Dir(currentFile), "..", "..", "..", "..",
		"k8s", "overlays", "prod", "configmaps.yaml",
	)), nil
}

// validateContractChainsServed is the deployment-level guardrail (see
// acknowledgedUnservedByTrackerChains). It fails when a contract chain is neither served by
// a deployed tracker nor acknowledged (its positions would be silently dropped), and again
// when a chain is both served and still acknowledged (a stale acknowledgement that would
// mask a future unserve). Error messages name the exact edit to make. It is pure: served,
// contractChains and acknowledged are supplied by the caller.
func validateContractChainsServed(served map[string]bool, contractChains []string, acknowledged map[string]bool) error {
	var errs []error

	var unserved []string
	seen := make(map[string]bool)
	for _, chain := range contractChains {
		if seen[chain] {
			continue
		}
		seen[chain] = true
		if served[chain] || acknowledged[chain] {
			continue
		}
		unserved = append(unserved, chain)
	}
	if len(unserved) > 0 {
		sort.Strings(unserved)
		errs = append(errs, fmt.Errorf(
			"axis-synome contract has entries on chain(s) %v that no deployed allocation-tracker serves: "+
				"deploy a tracker (add an allocation-tracker-config ConfigMap carrying its CHAIN_ID to "+
				"k8s/overlays/prod/configmaps.yaml), or add the chain to acknowledgedUnservedByTrackerChains if "+
				"it is intentionally not served yet",
			unserved,
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
			"chain(s) %v are now served by a deployed allocation-tracker but are still listed in "+
				"acknowledgedUnservedByTrackerChains: remove them from the set now that a tracker serves them",
			stale,
		))
	}

	return errors.Join(errs...)
}
