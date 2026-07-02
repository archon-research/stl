// Package schemamaster loads the canonical column register (schema_master.yaml) and
// checks a live schema, read from information_schema, against it.
//
// The register is CONFIG, not database state. It records what columns SHOULD be
// (canonical names, types, semantics) and which deviations are sanctioned. A column
// that already conforms appears nowhere here: conformance is verified against
// information_schema at check time. See docs/schema-framework.md.
package schemamaster

import (
	_ "embed"
	"fmt"
	"sort"

	"gopkg.in/yaml.v3"
)

//go:embed schema_master.yaml
var configBytes []byte

// Canonical is one entry in the rulebook: the invariant type plus default class/semantics.
type Canonical struct {
	Type      string `yaml:"type"`
	Class     string `yaml:"class"`
	Semantics string `yaml:"semantics"`
}

// TableMeta is per-table governance (all optional for now).
type TableMeta struct {
	Type  string `yaml:"type"`
	Owner string `yaml:"owner"`
}

// Transform is a column the transformation layer rewrites: rename / cast / fill.
type Transform struct {
	Table     string `yaml:"table"`
	Column    string `yaml:"column"`
	Canonical string `yaml:"canonical"`
	Action    string `yaml:"action"`
	From      string `yaml:"from"`
}

// Override is a sanctioned TYPE exemption: a column deliberately kept at accepted_type rather
// than its canonical type (e.g. an infra surrogate key). Semantics/class overrides and derived-
// column formulas are not part of the conformance check; they live with the L3 / semantic checks
// that consume them.
type Override struct {
	Table        string `yaml:"table"`
	Column       string `yaml:"column"`
	Canonical    string `yaml:"canonical"`
	AcceptedType string `yaml:"accepted_type"`
	Reason       string `yaml:"reason"`
}

// Register is the whole schema_master config.
type Register struct {
	IgnoreTables []string             `yaml:"ignore_tables"`
	Canonical    map[string]Canonical `yaml:"canonical"`
	Tables       map[string]TableMeta `yaml:"tables"`
	Transforms   []Transform          `yaml:"transforms"`
	Overrides    []Override           `yaml:"overrides"`
}

// Load parses the embedded schema_master.yaml.
func Load() (*Register, error) {
	var r Register
	if err := yaml.Unmarshal(configBytes, &r); err != nil {
		return nil, fmt.Errorf("parsing schema_master.yaml: %w", err)
	}
	return &r, nil
}

// Column is a live column as reported by information_schema.
type Column struct {
	Table    string
	Name     string
	DataType string // raw information_schema.data_type
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
func NormalizeType(dataType string) string {
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
