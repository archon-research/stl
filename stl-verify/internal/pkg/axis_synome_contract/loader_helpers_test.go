package axis_synome_contract

import (
	"strings"
	"testing"
)

func TestUniquePaths(t *testing.T) {
	got := uniquePaths([]string{"/a", "/b", "/a", "/c", "/b"})
	want := []string{"/a", "/b", "/c"}
	if len(got) != len(want) {
		t.Fatalf("uniquePaths = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("uniquePaths = %v, want %v (order preserved, deduped)", got, want)
		}
	}
}

func TestResolveDefaultPath_NotFound(t *testing.T) {
	_, err := resolveDefaultPath("does/not/exist/nope.json")
	if err == nil {
		t.Fatal("expected error for a missing default file")
	}
	if !strings.Contains(err.Error(), "not found; tried:") {
		t.Errorf("error = %q, want it to report the tried paths", err)
	}
}

func TestResolveDefaultPath_FindsCommittedContract(t *testing.T) {
	// From the package source dir the caller-relative candidate resolves to the
	// committed contract under stl-verify/contracts/axis-synome/.
	got, err := resolveDefaultPath(DefaultContractPath)
	if err != nil {
		t.Fatalf("resolveDefaultPath(%q) error = %v", DefaultContractPath, err)
	}
	if !strings.HasSuffix(got, DefaultContractPath) {
		t.Errorf("resolved %q does not end with %q", got, DefaultContractPath)
	}
}

func TestLoadDefault_ReturnsPopulatedBundle(t *testing.T) {
	bundle, err := LoadDefault()
	if err != nil {
		t.Fatalf("LoadDefault() error = %v", err)
	}
	if bundle.Contract.Version == "" {
		t.Error("contract version should be non-empty")
	}
	if len(bundle.Contract.GetAlmProxies()) == 0 {
		t.Error("expected at least one ALM proxy star")
	}
	if len(bundle.Contract.GetAssetsByPrime()) == 0 {
		t.Error("expected at least one assets-by-prime star")
	}
	if len(bundle.Schema) == 0 {
		t.Error("schema should be loaded into the bundle")
	}
}
