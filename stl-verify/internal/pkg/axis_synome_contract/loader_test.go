package axis_synome_contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testEntitiesPayload is the entities subtree shared by the legacy
// ({spec:{asc:{entities}}}) and current ({spec:{entities}}) shape tests.
func testEntitiesPayload() map[string]any {
	return map[string]any{
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
	}
}

func testContractPayload(spec map[string]any) map[string]any {
	return map[string]any{
		"version":                "v1",
		"axis_synome_git_commit": "deadbeef",
		"axis_synome":            map[string]any{"spec": spec},
	}
}

func TestLoad_OK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		spec map[string]any
	}{
		{
			name: "legacy asc shape",
			spec: map[string]any{"asc": map[string]any{"entities": testEntitiesPayload()}},
		},
		{
			name: "current entities shape",
			spec: map[string]any{"entities": testEntitiesPayload()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			contractPath := filepath.Join(dir, "contract.json")

			mustWriteJSON(t, contractPath, testContractPayload(tt.spec))

			contract, err := LoadContract(contractPath)
			if err != nil {
				t.Fatalf("LoadContract() error = %v", err)
			}

			if contract.Version != "v1" {
				t.Fatalf("version = %q, want %q", contract.Version, "v1")
			}
			if contract.AxisSynomeGitCommit != "deadbeef" {
				t.Fatalf("axis_synome_git_commit = %q, want %q", contract.AxisSynomeGitCommit, "deadbeef")
			}

			sparkMainnet := contract.GetAlmProxies()["spark"]["mainnet"]
			if len(sparkMainnet) != 2 {
				t.Fatalf("spark/mainnet proxies = %d, want 2", len(sparkMainnet))
			}
			if got := sparkMainnet[0].Address; got != "0x3333333333333333333333333333333333333333" {
				t.Fatalf("address = %q, want %q", got, "0x3333333333333333333333333333333333333333")
			}
			if got := sparkMainnet[0].Role; got != "alm" {
				t.Fatalf("role = %q, want %q", got, "alm")
			}

			sparkAssets := contract.GetAssetsByPrime()["spark"]
			if len(sparkAssets) != 1 {
				t.Fatalf("spark assets = %d, want 1", len(sparkAssets))
			}
		})
	}
}

func TestLoadContract_SpecShapeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		spec    map[string]any
		wantErr string
	}{
		{
			name: "both shapes fails",
			spec: map[string]any{
				"asc":      map[string]any{"entities": testEntitiesPayload()},
				"entities": testEntitiesPayload(),
			},
			wantErr: "both the legacy asc shape and the current entities shape",
		},
		{
			name:    "neither shape fails",
			spec:    map[string]any{},
			wantErr: "neither the legacy asc shape nor the current entities shape",
		},
		{
			// JSON null leaves the pointer nil: previously decoded to a
			// zero-value ASCModel and loaded silently empty, now rejected.
			name:    "explicit null asc fails",
			spec:    map[string]any{"asc": nil},
			wantErr: "neither the legacy asc shape nor the current entities shape",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			contractPath := filepath.Join(dir, "contract.json")
			mustWriteJSON(t, contractPath, testContractPayload(tt.spec))

			_, err := LoadContract(contractPath)
			if err == nil {
				t.Fatal("expected spec shape error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestLoadContract_UnknownFieldFails(t *testing.T) {
	t.Parallel()

	withExtraTopLevel := testContractPayload(
		map[string]any{"asc": map[string]any{"entities": testEntitiesPayload()}},
	)
	withExtraTopLevel["extra_field"] = "nope"

	entitiesWithExtra := testEntitiesPayload()
	entitiesWithExtra["bogus"] = 1

	tests := []struct {
		name    string
		payload map[string]any
	}{
		{
			name:    "unknown top-level field",
			payload: withExtraTopLevel,
		},
		{
			name: "typo'd key under spec",
			payload: testContractPayload(
				map[string]any{"entites": map[string]any{}},
			),
		},
		{
			name: "unknown field inside new-shape entities",
			payload: testContractPayload(
				map[string]any{"entities": entitiesWithExtra},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			contractPath := filepath.Join(dir, "contract.json")
			mustWriteJSON(t, contractPath, tt.payload)

			_, err := LoadContract(contractPath)
			if err == nil {
				t.Fatal("expected strict decode error, got nil")
			}
		})
	}
}

func TestLoadContract_InvalidAddressFails(t *testing.T) {
	t.Parallel()

	// One bad contract_address inside the shared entities payload, exercised
	// through both contract shapes so address validation provably traverses
	// each.
	invalidEntities := func() map[string]any {
		entities := testEntitiesPayload()
		assetsByPrime := entities["assets_by_prime"].(map[string]any)
		assets := assetsByPrime["ASSETS_BY_PRIME"].(map[string]any)
		assets["spark"].([]map[string]any)[0]["contract_address"] = "0x1"
		return entities
	}

	tests := []struct {
		name string
		spec map[string]any
	}{
		{
			name: "legacy asc shape",
			spec: map[string]any{"asc": map[string]any{"entities": invalidEntities()}},
		},
		{
			name: "current entities shape",
			spec: map[string]any{"entities": invalidEntities()},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			contractPath := filepath.Join(dir, "contract.json")
			mustWriteJSON(t, contractPath, testContractPayload(tt.spec))

			_, err := LoadContract(contractPath)
			if err == nil {
				t.Fatal("expected address validation error, got nil")
			}
			if !strings.Contains(err.Error(), "invalid ethereum address") {
				t.Fatalf("error = %v, want address validation error", err)
			}
		})
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

func TestAccessors_ZeroValueContractReturnNil(t *testing.T) {
	t.Parallel()

	var contract Contract
	if got := contract.GetAlmProxies(); got != nil {
		t.Fatalf("GetAlmProxies() = %v, want nil", got)
	}
	if got := contract.GetAssetsByPrime(); got != nil {
		t.Fatalf("GetAssetsByPrime() = %v, want nil", got)
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
