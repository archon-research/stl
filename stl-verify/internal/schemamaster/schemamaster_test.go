package schemamaster

import "testing"

// TestLoad confirms the embedded config parses and is non-trivial.
func TestLoad(t *testing.T) {
	r, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(r.Canonical) == 0 {
		t.Fatal("no canonical columns loaded")
	}
	if len(r.Tables) == 0 {
		t.Fatal("no governed tables loaded")
	}
	// spot-check a couple of well-known entries.
	if got := r.Canonical["block_timestamp"]; got.Type != "timestamptz" || got.Semantics != "event_time" {
		t.Errorf("block_timestamp = %+v, want timestamptz/event_time", got)
	}
	if got := r.Canonical["chain_id"]; got.Type != "int4" {
		t.Errorf("chain_id type = %q, want int4", got.Type)
	}
}

// TestClassify exercises the per-column decision against real register entries.
func TestClassify(t *testing.T) {
	r, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	cases := []struct {
		name string
		col  Column
		want Verdict
	}{
		{"conforming type", Column{"chain", "chain_id", "integer"}, Conforms},
		{"varchar equals text", Column{"token", "symbol", "character varying"}, Conforms},
		{"registered rename", Column{"morpho_market_state", "timestamp", "timestamp with time zone"}, ConformsTransform},
		{"registered cast", Column{"sparklend_reserve_data", "last_update_timestamp", "bigint"}, ConformsTransform},
		{"accepted type deviation", Column{"build_registry", "id", "integer"}, ConformsAccepted},
		{"unregistered new column", Column{"chain", "blocknum", "bigint"}, Unregistered},
		{"type drift", Column{"chain", "chain_id", "bigint"}, TypeDrift},
		{"rename with drifted type", Column{"morpho_market_state", "timestamp", "bigint"}, TypeDrift},
		{"cast with wrong source type", Column{"sparklend_reserve_data", "last_update_timestamp", "integer"}, TypeDrift},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, detail := r.Classify(tc.col); got != tc.want {
				t.Errorf("Classify(%+v) = %q (%s), want %q", tc.col, got, detail, tc.want)
			}
		})
	}
}

// TestNormalizeType covers the type equivalences the check relies on.
func TestNormalizeType(t *testing.T) {
	for in, want := range map[string]string{
		"timestamp with time zone": "timestamptz",
		"bigint":                   "int8",
		"integer":                  "int4",
		"smallint":                 "int2",
		"boolean":                  "bool",
		"character varying":        "text",
		"double precision":         "float8",
		"numeric":                  "numeric",
		"bytea":                    "bytea",
	} {
		if got := NormalizeType(in); got != want {
			t.Errorf("NormalizeType(%q) = %q, want %q", in, got, want)
		}
	}
}
