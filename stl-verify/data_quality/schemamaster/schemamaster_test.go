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
	if len(r.Fills) == 0 {
		t.Error("no fills loaded")
	}
	if len(r.RequiredKeys) == 0 {
		t.Error("no required_keys loaded")
	}
	if !r.Canonical["build_id"].NotNull {
		t.Error("build_id should be not_null in the register")
	}
	if !r.Canonical["processing_version"].NotNull {
		t.Error("processing_version should be not_null in the register")
	}
	if len(r.NullableExempt) == 0 {
		t.Error("no nullable_exempt entries loaded")
	}
	// transform_config binds to the Register struct (a structural typo in the key
	// would otherwise be silently dropped by json.Unmarshal).
	if len(r.TransformConfig) == 0 {
		t.Error("no transform_config entries loaded")
	}
	if got := r.TransformConfig["cex_orderbook_snapshots"].Key; len(got) == 0 {
		t.Error("cex_orderbook_snapshots transform_config key missing or empty")
	}
}

// TestNullability exercises the nullability pass: a column whose canonical is not_null must not be
// NULL-able unless it is in nullable_exempt. Filters by Kind so unrelated verdicts don't interfere.
func TestNullability(t *testing.T) {
	reg := &Register{}
	reg.Canonical = map[string]Canonical{"build_id": {Type: "int4", NotNull: true}, "note": {Type: "text"}}
	reg.Tables = map[string]TableMeta{"t": {Type: "raw_pipeline"}, "ex": {Type: "raw_pipeline"}}
	reg.NullableExempt = []NullableExempt{{Table: "ex", Column: "build_id"}}
	live := []Column{
		{Table: "t", Name: "build_id", DataType: "integer", Nullable: true},
		{Table: "ex", Name: "build_id", DataType: "integer", Nullable: true},
		{Table: "t", Name: "build_id", DataType: "integer", Nullable: false},
		{Table: "t", Name: "note", DataType: "text", Nullable: true},
	}
	var flagged []string
	for _, v := range reg.Check(live) {
		if v.Kind == "unexpected_nullable" {
			flagged = append(flagged, v.Table+"."+v.Column)
		}
	}
	if len(flagged) != 1 || flagged[0] != "t.build_id" {
		t.Errorf("unexpected_nullable = %v, want [t.build_id]", flagged)
	}
}

// TestRequiredKeys exercises the required-key pass in isolation: a raw_pipeline table must resolve
// the key natively, via a transform, or via a fill; exempt tables and other table types are skipped.
// Filters by Kind so unrelated per-column verdicts from the synthetic register don't interfere.
func TestRequiredKeys(t *testing.T) {
	reg := &Register{}
	reg.Tables = map[string]TableMeta{"native": {Type: "raw_pipeline"}, "filled": {Type: "raw_pipeline"}, "xformed": {Type: "raw_pipeline"}, "missing": {Type: "raw_pipeline"}, "exempt": {Type: "raw_pipeline"}, "cfg": {Type: "config"}}
	reg.Transforms = []Transform{{Table: "xformed", Column: "ts", Canonical: "chain_id", Action: "rename"}}
	reg.Fills = []Fill{{Table: "filled", Column: "chain_id"}}
	reg.RequiredKeys = []RequiredKey{{Name: "chain", AnyOf: []string{"chain_id"}, AppliesTo: []string{"raw_pipeline"}, Exempt: []string{"exempt"}}}
	live := []Column{
		{Table: "native", Name: "chain_id", DataType: "integer"},
		{Table: "filled", Name: "other", DataType: "integer"},
		{Table: "xformed", Name: "ts", DataType: "integer"},
		{Table: "missing", Name: "other", DataType: "integer"},
		{Table: "exempt", Name: "other", DataType: "integer"},
		{Table: "cfg", Name: "other", DataType: "integer"},
	}
	flagged := map[string]bool{}
	for _, v := range reg.Check(live) {
		if v.Kind == "missing_required_key" {
			flagged[v.Table] = true
		}
	}
	if !flagged["missing"] {
		t.Error("expected 'missing' to be flagged missing_required_key")
	}
	for _, ok := range []string{"native", "filled", "xformed", "exempt", "cfg"} {
		if flagged[ok] {
			t.Errorf("%s should not be flagged (resolved natively/fill/transform, or exempt/other type)", ok)
		}
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
		{"conforming type", Column{"chain", "chain_id", "integer", false}, Conforms},
		{"varchar equals text", Column{"token", "symbol", "character varying", false}, Conforms},
		{"parameterized varchar conforms", Column{"token", "symbol", "character varying(256)", false}, Conforms},
		{"registered rename", Column{"morpho_market_state", "timestamp", "timestamp with time zone", false}, ConformsTransform},
		{"registered cast", Column{"sparklend_reserve_data", "last_update_timestamp", "bigint", false}, ConformsTransform},
		{"accepted type deviation", Column{"build_registry", "id", "integer", false}, ConformsAccepted},
		{"unregistered new column", Column{"chain", "blocknum", "bigint", false}, Unregistered},
		{"type drift", Column{"chain", "chain_id", "bigint", false}, TypeDrift},
		{"rename with drifted type", Column{"morpho_market_state", "timestamp", "bigint", false}, TypeDrift},
		{"cast with wrong source type", Column{"sparklend_reserve_data", "last_update_timestamp", "integer", false}, TypeDrift},
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
		// parameterized forms: information_schema emits the base type (the length/precision lives
		// in a separate column), but NormalizeType strips a trailing modifier defensively.
		"character varying(256)": "text",
		"numeric(10,2)":          "numeric",
	} {
		if got := NormalizeType(in); got != want {
			t.Errorf("NormalizeType(%q) = %q, want %q", in, got, want)
		}
	}
}
