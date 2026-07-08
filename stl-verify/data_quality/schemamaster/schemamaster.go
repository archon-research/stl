// Package schemamaster loads the canonical column register (schema_master.json) and
// checks a live schema, read from information_schema, against it.
//
// The register is CONFIG, not database state. It records what columns SHOULD be
// (canonical names, types, semantics) and which deviations are sanctioned. A column
// that already conforms appears nowhere here: conformance is verified against
// information_schema at check time. See README.md for the section-by-section intent.
package schemamaster

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
)

//go:embed schema_master.json
var configBytes []byte

// Canonical is one entry in the rulebook: the invariant type plus default class/semantics.
// NotNull marks a column that must be declared NOT NULL wherever it appears (with sanctioned
// exceptions in NullableExempt).
type Canonical struct {
	Type      string `json:"type"`
	Class     string `json:"class"`
	Semantics string `json:"semantics"`
	NotNull   bool   `json:"not_null"`
}

// TableMeta is per-table governance (all optional for now).
type TableMeta struct {
	Type  string `json:"type"`
	Owner string `json:"owner"`
}

// Transform is a column the transformation layer rewrites: rename / cast / fill.
// GuardMin/GuardMax are the plausibility bounds for an epoch (int8 -> timestamptz) cast: values
// outside the range are NULLed rather than cast. They are policy read by the transform materializer
// and the runtime cast check; the conformance check here does not use them.
type Transform struct {
	Table     string `json:"table"`
	Column    string `json:"column"`
	Canonical string `json:"canonical"`
	Action    string `json:"action"`
	From      string `json:"from"`
	GuardMin  *int64 `json:"guard_min"`
	GuardMax  *int64 `json:"guard_max"`
}

// Override is a sanctioned TYPE exemption: a column deliberately kept at accepted_type rather
// than its canonical type (e.g. an infra surrogate key). Semantics/class overrides and derived-
// column formulas are not part of the conformance check; they live with the semantic-layer checks
// that consume them.
type Override struct {
	Table        string `json:"table"`
	Column       string `json:"column"`
	Canonical    string `json:"canonical"`
	AcceptedType string `json:"accepted_type"`
	Reason       string `json:"reason"`
}

// Fill declares how a governed table obtains a canonical key it lacks natively: the transform layer
// derives the key from another table rather than the column existing in the raw row. The conformance
// check reads only Table+Column (a declared fill satisfies a required key);
// the remaining fields describe the mechanism for the transform generator: a single-hop FK join
// (Parent/Key/Ref), an optional second hop (ThenParent/ThenKey/ThenRef), a literal (Const), or the
// block-time dimension (BlockTime).
type Fill struct {
	Table      string `json:"table"`
	Column     string `json:"column"`
	Parent     string `json:"parent"`
	Key        string `json:"key"`
	Ref        string `json:"ref"`
	ThenParent string `json:"then_parent"`
	ThenKey    string `json:"then_key"`
	ThenRef    string `json:"then_ref"`
	Const      *int   `json:"const"`
	BlockTime  bool   `json:"block_time"`
}

// RequiredKey asserts that every governed table whose type is in AppliesTo resolves one of AnyOf
// (as a native column, a transform target, or a fill), unless the table is listed in Exempt.
type RequiredKey struct {
	Name      string   `json:"name"`
	AnyOf     []string `json:"any_of"`
	AppliesTo []string `json:"applies_to"`
	Exempt    []string `json:"exempt"`
}

// NullableExempt sanctions a specific column staying NULL-able despite a not_null canonical
// (e.g. build_id on tables retrofitted before the auditability convention, which TigerData's
// tiered-chunk restriction blocks from a SET NOT NULL).
type NullableExempt struct {
	Table  string `json:"table"`
	Column string `json:"column"`
	Reason string `json:"reason"`
}

// Register is the whole schema_master config.
type Register struct {
	IgnoreTables   []string             `json:"ignore_tables"`
	Canonical      map[string]Canonical `json:"canonical"`
	Tables         map[string]TableMeta `json:"tables"`
	Transforms     []Transform          `json:"transforms"`
	Overrides      []Override           `json:"overrides"`
	Fills          []Fill               `json:"fills"`
	RequiredKeys   []RequiredKey        `json:"required_keys"`
	NullableExempt []NullableExempt     `json:"nullable_exempt"`
}

// producesCanonical reports whether a transform for table renames or casts a column to canonicalCol.
func (r *Register) producesCanonical(table, canonicalCol string) bool {
	for _, t := range r.Transforms {
		if t.Table == table && t.Canonical == canonicalCol {
			return true
		}
	}
	return false
}

// hasFill reports whether a fill declares canonicalCol for table.
func (r *Register) hasFill(table, canonicalCol string) bool {
	for _, f := range r.Fills {
		if f.Table == table && f.Column == canonicalCol {
			return true
		}
	}
	return false
}

// Load parses the embedded schema_master.json.
func Load() (*Register, error) {
	var r Register
	if err := json.Unmarshal(configBytes, &r); err != nil {
		return nil, fmt.Errorf("parsing schema_master.json: %w", err)
	}
	return &r, nil
}

// Column is a live column as reported by information_schema.
type Column struct {
	Table    string
	Name     string
	DataType string // raw information_schema.data_type
	Nullable bool   // information_schema.is_nullable = 'YES'
}

// Verdict is the per-column classification.
type Verdict string

const (
	Conforms          Verdict = "conforms"
	ConformsTransform Verdict = "conforms (transform)"
	ConformsAccepted  Verdict = "conforms (accepted)"
	TypeDrift         Verdict = "type_drift"
	Unregistered      Verdict = "unregistered_column"
)

// Violation is a single failing finding.
type Violation struct {
	Table  string
	Column string
	Kind   string
	Detail string
}

// NormalizeType folds information_schema data_type names to the canonical short tokens
// used in the register. text and varchar (no length) are treated as identical, as they
// are in Postgres.
//
// information_schema.data_type reports the base type without a length/precision modifier
// (e.g. "character varying", "numeric"); the modifier lives in separate columns
// (character_maximum_length, numeric_precision). A trailing modifier is stripped defensively
// so that a value like "character varying(256)" or "numeric(10,2)" from any other caller
// still folds to its base token.
func NormalizeType(dataType string) string {
	if i := strings.IndexByte(dataType, '('); i >= 0 && strings.HasSuffix(dataType, ")") {
		dataType = strings.TrimSpace(dataType[:i])
	}
	switch dataType {
	case "timestamp with time zone":
		return "timestamptz"
	case "bigint":
		return "int8"
	case "integer":
		return "int4"
	case "smallint":
		return "int2"
	case "boolean":
		return "bool"
	case "character varying":
		return "text"
	case "double precision":
		return "float8"
	default:
		return dataType
	}
}

// Classify decides a single live column against the register, independent of coverage.
func (r *Register) Classify(c Column) (Verdict, string) {
	live := NormalizeType(c.DataType)
	for _, t := range r.Transforms {
		if t.Table == c.Table && t.Column == c.Name {
			// A transform sanctions a name/type change but must not let the column drift:
			// a rename-only column must already be the canonical type; a cast's live type
			// must be the declared source (`from`).
			target, known := r.Canonical[t.Canonical]
			if !known {
				return TypeDrift, fmt.Sprintf("transform references unknown canonical %q", t.Canonical)
			}
			switch t.Action {
			case "rename":
				if live != target.Type {
					return TypeDrift, fmt.Sprintf("live %s, canonical %s (rename to %s)", live, target.Type, t.Canonical)
				}
			case "cast":
				if from := NormalizeType(t.From); live != from {
					return TypeDrift, fmt.Sprintf("live %s, transform source %s", live, from)
				}
			default:
				return TypeDrift, fmt.Sprintf("unknown transform action %q", t.Action)
			}
			return ConformsTransform, t.Action
		}
	}
	canon, known := r.Canonical[c.Name]
	if !known {
		return Unregistered, fmt.Sprintf("%q is not a canonical column and has no transform", c.Name)
	}
	if live == canon.Type {
		return Conforms, ""
	}
	for _, o := range r.Overrides {
		if o.Table == c.Table && o.Column == c.Name && o.AcceptedType == live {
			return ConformsAccepted, o.Reason
		}
	}
	return TypeDrift, fmt.Sprintf("live %s, canonical %s", live, canon.Type)
}

// Check diffs the whole live schema against the register and returns every violation.
// An empty result means the live schema fully conforms.
func (r *Register) Check(live []Column) []Violation {
	ignore := make(map[string]bool, len(r.IgnoreTables))
	for _, t := range r.IgnoreTables {
		ignore[t] = true
	}
	liveCols := make(map[[2]string]bool, len(live))
	liveTables := make(map[string]bool)
	for _, c := range live {
		liveCols[[2]string{c.Table, c.Name}] = true
		liveTables[c.Table] = true
	}

	var vs []Violation

	// 1. per-column conformance, governed tables only.
	for _, c := range live {
		if ignore[c.Table] {
			continue
		}
		if _, governed := r.Tables[c.Table]; !governed {
			continue // reported by the table-coverage pass below
		}
		switch v, detail := r.Classify(c); v {
		case Unregistered, TypeDrift:
			vs = append(vs, Violation{c.Table, c.Name, string(v), detail})
		}
	}

	// 2. table coverage.
	for t := range r.Tables {
		if ignore[t] {
			continue
		}
		if !liveTables[t] {
			vs = append(vs, Violation{t, "", "orphan_table", "in register, not in the live schema"})
		}
	}
	for t := range liveTables {
		if ignore[t] {
			continue
		}
		if _, ok := r.Tables[t]; !ok {
			vs = append(vs, Violation{t, "", "unregistered_table", "in the live schema, not in the register"})
		}
	}

	// 3. orphaned register entries (reference a column that no longer exists).
	orphan := func(table, column, kind string) {
		if !liveCols[[2]string{table, column}] {
			vs = append(vs, Violation{table, column, kind, "register entry references a column that is not live"})
		}
	}
	for _, t := range r.Transforms {
		orphan(t.Table, t.Column, "orphan_transform")
	}
	for _, o := range r.Overrides {
		orphan(o.Table, o.Column, "orphan_override")
	}

	// 4. required keys: a governed table of an applicable type must resolve each requirement as a
	// native column, a transform target, or a fill, unless exempt. Catches a table that lands
	// without chain_id / protocol_id / an observation time and has no declared way to obtain it.
	for t, meta := range r.Tables {
		if ignore[t] || !liveTables[t] {
			continue
		}
		for _, req := range r.RequiredKeys {
			if !slices.Contains(req.AppliesTo, meta.Type) || slices.Contains(req.Exempt, t) {
				continue
			}
			resolved := false
			for _, col := range req.AnyOf {
				if liveCols[[2]string{t, col}] || r.producesCanonical(t, col) || r.hasFill(t, col) {
					resolved = true
					break
				}
			}
			if !resolved {
				vs = append(vs, Violation{t, "", "missing_required_key",
					fmt.Sprintf("%s: needs one of %v (native column, transform, or fill)", req.Name, req.AnyOf)})
			}
		}
	}

	// 5. nullability: a column whose canonical is not_null must not be NULL-able, unless sanctioned.
	nullExempt := make(map[[2]string]bool, len(r.NullableExempt))
	for _, e := range r.NullableExempt {
		nullExempt[[2]string{e.Table, e.Column}] = true
	}
	for _, c := range live {
		if ignore[c.Table] {
			continue
		}
		if _, governed := r.Tables[c.Table]; !governed {
			continue
		}
		if canon, known := r.Canonical[c.Name]; known && canon.NotNull && c.Nullable && !nullExempt[[2]string{c.Table, c.Name}] {
			vs = append(vs, Violation{c.Table, c.Name, "unexpected_nullable",
				fmt.Sprintf("%s is not_null in the register but the column is NULL-able", c.Name)})
		}
	}

	sort.Slice(vs, func(i, j int) bool {
		if vs[i].Table != vs[j].Table {
			return vs[i].Table < vs[j].Table
		}
		if vs[i].Column != vs[j].Column {
			return vs[i].Column < vs[j].Column
		}
		return vs[i].Kind < vs[j].Kind
	})
	return vs
}
