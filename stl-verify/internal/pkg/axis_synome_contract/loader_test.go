package axis_synome_contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoad_OK(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	contractPath := filepath.Join(dir, "contract.json")
	schemaPath := filepath.Join(dir, "contract.schema.json")

	contract := map[string]any{
		"version":                "v1",
		"axis_synome_git_commit": "deadbeef",
		"axis_synome": map[string]any{
			"spec": map[string]any{
				"asc": map[string]any{
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
										"created_at_block": nil,
									},
								},
							},
						},
						"alm_proxies": map[string]any{
							"AlmProxy": map[string]any{
								"spark": map[string]any{
									"mainnet": map[string]any{
										"star":    "spark",
										"chain":   "mainnet",
										"address": "0x3333333333333333333333333333333333333333",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	schema := map[string]any{"type": "object", "$defs": map[string]any{}}

	mustWriteJSON(t, contractPath, contract)
	mustWriteJSON(t, schemaPath, schema)

	bundle, err := Load(contractPath, schemaPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if bundle.Contract.Version != "v1" {
		t.Fatalf("version = %q, want %q", bundle.Contract.Version, "v1")
	}
	if bundle.Contract.AxisSynomeGitCommit != "deadbeef" {
		t.Fatalf("axis_synome_git_commit = %q, want %q", bundle.Contract.AxisSynomeGitCommit, "deadbeef")
	}

	got := bundle.Contract.AxisSynome.Spec.ASC.Entities.AlmProxies.AlmProxy["spark"]["mainnet"].Address
	if got != "0x3333333333333333333333333333333333333333" {
		t.Fatalf("address = %q, want %q", got, "0x3333333333333333333333333333333333333333")
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
				"asc": map[string]any{
					"entities": map[string]any{
						"assets_by_prime": map[string]any{"ASSETS_BY_PRIME": map[string]any{}},
						"alm_proxies":     map[string]any{"AlmProxy": map[string]any{}},
					},
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
				"asc": map[string]any{
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
										"created_at_block": nil,
									},
								},
							},
						},
						"alm_proxies": map[string]any{
							"AlmProxy": map[string]any{
								"spark": map[string]any{
									"mainnet": map[string]any{
										"star":    "spark",
										"chain":   "mainnet",
										"address": "0x3333333333333333333333333333333333333333",
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
      "asc": {
        "entities": {
          "assets_by_prime": { "ASSETS_BY_PRIME": {} },
          "alm_proxies": { "AlmProxy": {} }
        }
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
