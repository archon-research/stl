package axis_synome_contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadContract_OK(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")

	contract := map[string]any{
		"version":                "v1",
		"axis_synome_git_commit": "deadbeef",
		"axis_synome": map[string]any{
			"spec": map[string]any{
				"entities": map[string]any{
					"assets_by_prime": map[string]any{
						"ASSETS_BY_PRIME": map[string]any{
							"spark": []map[string]any{
								{
									"contract_address": "0x1111111111111111111111111111111111111111",
									"wallet_address":   "0x2222222222222222222222222222222222222222",
									"asset_address":    nil,
									"star":             "spark",
									"chain":            "mainnet",
									"protocol":         "aave-v3",
									"allocation_type":  "allocation",
									"token_type":       "atoken",
								},
							},
						},
					},
					"alm_proxies": map[string]any{
						"AlmProxy": map[string]any{
							"spark": map[string]any{
								"mainnet": []map[string]any{
									{
										"star":    "spark",
										"chain":   "mainnet",
										"address": "0x3333333333333333333333333333333333333333",
										"role":    "alm",
									},
									{
										"star":    "spark",
										"chain":   "mainnet",
										"address": "0x4444444444444444444444444444444444444444",
										"role":    "subproxy",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mustWriteJSON(t, contractPath, contract)

	loaded, err := LoadContract(contractPath)
	if err != nil {
		t.Fatalf("LoadContract() error = %v", err)
	}

	if loaded.Version != "v1" {
		t.Fatalf("version = %q, want %q", loaded.Version, "v1")
	}
	if loaded.AxisSynomeGitCommit != "deadbeef" {
		t.Fatalf("axis_synome_git_commit = %q, want %q", loaded.AxisSynomeGitCommit, "deadbeef")
	}

	sparkMainnet := loaded.AxisSynome.Spec.Entities.AlmProxies.AlmProxy["spark"]["mainnet"]
	if len(sparkMainnet) != 2 {
		t.Fatalf("spark/mainnet proxies = %d, want 2", len(sparkMainnet))
	}
	if got := sparkMainnet[0].Address; got != "0x3333333333333333333333333333333333333333" {
		t.Fatalf("address = %q, want %q", got, "0x3333333333333333333333333333333333333333")
	}
	if got := sparkMainnet[0].Role; got != "alm" {
		t.Fatalf("role = %q, want %q", got, "alm")
	}
}

// TestLoadDefaultContract_RealEntriesArePopulated guards against a silent field
// *drop* in the real contract that decoding cannot catch. A renamed field trips
// strict decode (DisallowUnknownFields) and bad addresses trip validateAddresses,
// but a dropped non-address field decodes to an empty string with no error, and
// the workers would then index mislabelled positions. The sentinel gate swaps
// the real file in before this runs, so pinning per-field invariants here turns
// that class of drift into a red. The checked>0 assertions keep the per-entry
// loops from passing vacuously if a map is somehow empty.
func TestLoadDefaultContract_RealEntriesArePopulated(t *testing.T) {
	t.Parallel()

	contract, err := LoadDefaultContract()
	if err != nil {
		t.Fatalf("LoadDefaultContract() error = %v", err)
	}

	entriesChecked := 0
	for star, entries := range contract.GetAssetsByPrime() {
		for i, entry := range entries {
			entriesChecked++
			if entry.Star != star {
				t.Errorf("star=%s index=%d: entry.Star = %q, want it to match its map key", star, i, entry.Star)
			}
			if entry.Chain == "" {
				t.Errorf("star=%s index=%d: empty chain", star, i)
			}
			if entry.AllocationType == "" {
				t.Errorf("star=%s index=%d: empty allocation_type", star, i)
			}
			if entry.TokenType == "" {
				t.Errorf("star=%s index=%d: empty token_type", star, i)
			}
		}
	}
	if entriesChecked == 0 {
		t.Fatal("no token entries examined; real contract has no ASSETS_BY_PRIME rows")
	}

	proxiesChecked := 0
	for star, byChain := range contract.GetAlmProxies() {
		for chain, proxies := range byChain {
			for i, proxy := range proxies {
				proxiesChecked++
				if proxy.Star != star || proxy.Chain != chain {
					t.Errorf("proxy star=%s chain=%s index=%d: fields (%q,%q) do not match their map keys", star, chain, i, proxy.Star, proxy.Chain)
				}
				if proxy.Role == "" {
					t.Errorf("proxy star=%s chain=%s index=%d: empty role", star, chain, i)
				}
			}
		}
	}
	if proxiesChecked == 0 {
		t.Fatal("no proxies examined; real contract has no AlmProxy rows")
	}
}

func TestLoadContract_EmptyEntitiesFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")

	// Structurally valid, correct shape, but no entries: strict decode and
	// address validation both pass, so only the loader's non-empty guard can
	// reject it.
	payload := map[string]any{
		"version":                "v1",
		"axis_synome_git_commit": "deadbeef",
		"axis_synome": map[string]any{
			"spec": map[string]any{
				"entities": map[string]any{
					"assets_by_prime": map[string]any{"ASSETS_BY_PRIME": map[string]any{}},
					"alm_proxies":     map[string]any{"AlmProxy": map[string]any{}},
				},
			},
		},
	}
	mustWriteJSON(t, contractPath, payload)

	_, err := LoadContract(contractPath)
	if err == nil {
		t.Fatal("expected empty-contract rejection, got nil")
	}
	if !strings.Contains(err.Error(), "no assets_by_prime entries") {
		t.Fatalf("expected assets_by_prime emptiness error, got: %v", err)
	}
}

func TestLoadContract_UnknownFieldFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")

	payload := map[string]any{
		"version":                "v1",
		"axis_synome_git_commit": "deadbeef",
		"axis_synome": map[string]any{
			"spec": map[string]any{
				"entities": map[string]any{
					"assets_by_prime": map[string]any{"ASSETS_BY_PRIME": map[string]any{}},
					"alm_proxies":     map[string]any{"AlmProxy": map[string]any{}},
				},
			},
		},
		"extra_field": "nope",
	}
	mustWriteJSON(t, contractPath, payload)

	_, err := LoadContract(contractPath)
	if err == nil {
		t.Fatal("expected strict decode error, got nil")
	}
}

func TestLoadContract_InvalidAddressFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")

	payload := map[string]any{
		"version":                "v1",
		"axis_synome_git_commit": "deadbeef",
		"axis_synome": map[string]any{
			"spec": map[string]any{
				"entities": map[string]any{
					"assets_by_prime": map[string]any{
						"ASSETS_BY_PRIME": map[string]any{
							"spark": []map[string]any{
								{
									"contract_address": "0x1",
									"wallet_address":   "0x2222222222222222222222222222222222222222",
									"asset_address":    nil,
									"star":             "spark",
									"chain":            "mainnet",
									"protocol":         "aave-v3",
									"allocation_type":  "allocation",
									"token_type":       "atoken",
								},
							},
						},
					},
					"alm_proxies": map[string]any{
						"AlmProxy": map[string]any{
							"spark": map[string]any{
								"mainnet": []map[string]any{
									{
										"star":    "spark",
										"chain":   "mainnet",
										"address": "0x3333333333333333333333333333333333333333",
										"role":    "alm",
									},
									{
										"star":    "spark",
										"chain":   "mainnet",
										"address": "0x4444444444444444444444444444444444444444",
										"role":    "subproxy",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	mustWriteJSON(t, contractPath, payload)

	_, err := LoadContract(contractPath)
	if err == nil {
		t.Fatal("expected address validation error, got nil")
	}
}

func TestLoadContract_TrailingJSONFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")

	payload := `{
  "version": "v1",
  "axis_synome_git_commit": "deadbeef",
  "axis_synome": {
    "spec": {
      "entities": {
        "assets_by_prime": { "ASSETS_BY_PRIME": {} },
        "alm_proxies": { "AlmProxy": {} }
      }
    }
  }
}{"extra":true}`

	if err := os.WriteFile(contractPath, []byte(payload), 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}

	_, err := LoadContract(contractPath)
	if err == nil {
		t.Fatal("expected trailing JSON error, got nil")
	}
	if !strings.Contains(err.Error(), "unexpected trailing JSON content") {
		t.Fatalf("expected trailing JSON error, got: %v", err)
	}
}

func mustWriteJSON(t *testing.T, path string, payload any) {
	t.Helper()

	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		t.Fatalf("json.MarshalIndent() error = %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("os.WriteFile() error = %v", err)
	}
}
