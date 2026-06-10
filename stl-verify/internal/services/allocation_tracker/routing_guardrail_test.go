package allocation_tracker

import (
	"io"
	"log/slog"
	"testing"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stubRoutedAllowlist is the set of token_types that are allowed to route to a
// not-yet-implemented StubSource (i.e. are knowingly untracked). Anything else that
// routes to a stub is a silently-dropped position and fails the guardrail below.
var stubRoutedAllowlist = map[string]bool{
	"psm3":              true,
	"centrifuge_feeder": true,
	"galaxy_clo":        true,
}

// TestEveryContractEntryRoutes is the guardrail for this PR's headline risk: a
// regenerated axis-synome contract introduces a new/renamed token_type that no source
// handles, so positions are silently untracked with only a runtime Warn. It loads the
// real committed contract, routes every entry through the real production registry, and
// fails if any entry routes to nothing or to a stub outside the allowlist.
//
// Route/Supports need no live multicaller, so the registry is built with a nil one.
func TestEveryContractEntryRoutes(t *testing.T) {
	entries, err := LoadDefaultTokenEntries()
	if err != nil {
		t.Fatalf("load default token entries: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no token entries loaded from the committed contract")
	}

	registry, err := BuildSourceRegistry(nil, quietLogger())
	if err != nil {
		t.Fatalf("build source registry: %v", err)
	}

	for _, e := range entries {
		source := registry.Route(e)
		if source == nil {
			t.Errorf("contract entry routes to NO source (silently untracked): chain=%s token_type=%q protocol=%q contract=%s — register a source for this token_type",
				e.Chain, e.TokenType, e.Protocol, e.ContractAddress.Hex())
			continue
		}
		if _, isStub := source.(placeholderSource); isStub && !stubRoutedAllowlist[e.TokenType] {
			t.Errorf("contract entry routes to a not-yet-implemented stub but token_type %q is not in the allowlist {psm3, centrifuge_feeder, galaxy_clo}: chain=%s protocol=%q contract=%s — implement a real source or add the token_type to stubRoutedAllowlist",
				e.TokenType, e.Chain, e.Protocol, e.ContractAddress.Hex())
		}
	}
}

// TestEveryContractChainIsConfigurableOrAcknowledged closes B1 at CI time. Loading the
// entries and proxies runs the load-boundary chain-vocabulary validation
// (validateChainVocabulary), which fails if a regeneration introduces an entry on a
// chain that is neither in entity.ChainIDToName (so a tracker can index it) nor in the
// acknowledged not-yet-served allowlist — the gap where EntriesForChainID/
// ProxiesForChainID would silently drop the position. The explicit chainIsKnown loop
// documents the same invariant against the shared allowlist.
func TestEveryContractChainIsConfigurableOrAcknowledged(t *testing.T) {
	entries, err := LoadDefaultTokenEntries()
	if err != nil {
		t.Fatalf("load default token entries (chain-vocabulary validation): %v", err)
	}
	if _, err := LoadDefaultProxies(); err != nil {
		t.Fatalf("load default proxies (chain-vocabulary validation): %v", err)
	}

	seen := make(map[string]bool)
	for _, e := range entries {
		if seen[e.Chain] {
			continue
		}
		seen[e.Chain] = true
		if !chainIsKnown(e.Chain) {
			t.Errorf("contract chain %q is neither configurable (entity.ChainIDToName) nor acknowledged (acknowledgedUnservedChains); enable it or acknowledge it", e.Chain)
		}
	}
}
