package schemamaster

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ReferenceRead is an append-on-change reference table (ADR-0006 §4) plus the two objects
// its conversion creates. Which reference rows a reader used decides which data rows it
// used, so the read has to be pinned to a recorded date rather than to the wall clock.
type ReferenceRead struct {
	Table       string `json:"table"`
	CurrentView string `json:"current_view"`
	AsOf        string `json:"as_of"`
	Reason      string `json:"reason"`
}

// SQLSource is one scanned file of application SQL: repository adapters embed their SQL as
// string literals, so the lint reads the file body rather than parsing statements.
type SQLSource struct {
	Path string
	Body string
}

// wallClock matches the expressions that make a read's answer depend on when it runs.
// localtimestamp/current_timestamp are included because they are the same hazard spelled
// differently, and a reviewer should not have to know which spellings the lint knows.
var wallClock = regexp.MustCompile(`(?i)\b(now\s*\(\s*\)|current_date|current_timestamp|localtimestamp)`)

// CheckCalculationSQL lints calculation and writer SQL against ADR-0006 §4. Three ways a
// read of a converted reference table goes wrong, in the order they cost us:
//
//   - the raw table, unpinned: it holds every version now, so an EXISTS over it matches a
//     row that was retired years ago as readily as the live one;
//   - the _current view: bounded on the wall clock, so the same query changes answer when a
//     future-dated version becomes effective — operational reads only;
//   - the _as_of function handed a wall-clock expression: pinned in form only, and a replay
//     cannot pass back the date the original read used.
//
// The wall-clock rule is deliberately scoped to the effective_at argument, not to every
// now() in the file: a staleness or age computation over observation data is legitimate,
// and banning it outright would need an allowlist long enough to hide a real finding.
func (r *Register) CheckCalculationSQL(sources []SQLSource) []Violation {
	var vs []Violation
	for _, src := range sources {
		code := SQLSource{Path: src.Path, Body: stripLineComments(src.Body)}
		for _, ref := range r.ReferenceReads {
			vs = append(vs, ref.check(code)...)
		}
	}
	sort.Slice(vs, func(i, j int) bool {
		if vs[i].Table != vs[j].Table {
			return vs[i].Table < vs[j].Table
		}
		return vs[i].Kind < vs[j].Kind
	})
	return vs
}

// check reports every way src reads ref without a pinned effective date. Violation.Table
// carries the source path and Violation.Column the reference table, so a finding reads as
// "<file>: <kind>" the way the schema findings read as "<table>.<column>: <kind>".
func (ref ReferenceRead) check(src SQLSource) []Violation {
	var vs []Violation
	finding := func(kind, detail string) {
		vs = append(vs, Violation{src.Path, ref.Table, kind, detail})
	}

	if strings.Contains(src.Body, ref.CurrentView) {
		finding("current_view_in_calculation_sql", fmt.Sprintf(
			"reads %s; that view is bounded on the wall clock, so use %s(<recorded effective_at>) here (ADR-0006 §4)",
			ref.CurrentView, ref.AsOf))
	}
	for _, position := range unpinnedReads(ref.Table, src.Body) {
		finding("unpinned_reference_read", fmt.Sprintf(
			"%s reads the raw append-on-change table %s, which holds every version; read %s(<recorded effective_at>) instead (ADR-0006 §4)",
			position, ref.Table, ref.AsOf))
	}
	for _, arg := range asOfArguments(ref.AsOf, src.Body) {
		if wallClock.MatchString(arg) {
			finding("wall_clock_effective_at", fmt.Sprintf(
				"%s(%s) pins the read to the wall clock; pass the run's recorded effective_at as a parameter (ADR-0006 §4)",
				ref.AsOf, strings.TrimSpace(arg)))
		}
	}
	return vs
}

// stripLineComments blanks out line comments before matching, so documenting the rule —
// naming the _current view in the comment that explains why not to use it — is not itself
// a violation. Covers the three comment markers the scanned sources use: SQL `--` inside
// the embedded query text, plus Python `#` and Go `//` around it.
func stripLineComments(body string) string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		if cut := commentStart(line); cut >= 0 {
			lines[i] = line[:cut]
		}
	}
	return strings.Join(lines, "\n")
}

// commentStart returns the index where line's comment begins, or -1 if it has none.
func commentStart(line string) int {
	first := -1
	for _, marker := range []string{"--", "#", "//"} {
		if i := strings.Index(line, marker); i >= 0 && (first < 0 || i < first) {
			first = i
		}
	}
	return first
}

// unpinnedReads returns the FROM/JOIN clauses that read table directly. Only read
// positions count: appending a version is an INSERT into the raw table and is how a change
// is recorded. RE2 has no lookahead, so the identifier's trailing characters are captured
// and checked instead — that is also what separates `oracle_asset` from `oracle_asset_as_of`.
func unpinnedReads(table, body string) []string {
	pattern := regexp.MustCompile(`(?i)\b(from|join)\s+` + regexp.QuoteMeta(table) + `([0-9a-z_]*)`)
	var positions []string
	for _, m := range pattern.FindAllStringSubmatch(body, -1) {
		if m[2] != "" {
			continue // a longer identifier: the _as_of / _current / _versions object
		}
		positions = append(positions, strings.ToUpper(m[1]))
	}
	return positions
}

// asOfArguments returns the argument text of every asOf call in body, tracking nesting so a
// cast or a function call inside the argument does not truncate it.
func asOfArguments(asOf, body string) []string {
	var args []string
	for _, start := range callSites(asOf, body) {
		depth := 0
		for i := start; i < len(body); i++ {
			switch body[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					args = append(args, body[start+1:i])
				}
			}
			if depth == 0 {
				break
			}
		}
	}
	return args
}

// callSites returns the index of the opening parenthesis of every asOf(...) call.
func callSites(asOf, body string) []int {
	var sites []int
	for offset := 0; ; {
		i := strings.Index(body[offset:], asOf+"(")
		if i < 0 {
			return sites
		}
		sites = append(sites, offset+i+len(asOf))
		offset += i + len(asOf) + 1
	}
}

// LoadSQLSources reads every Go and Python file under the given roots. The whole file is
// one source: repository SQL lives in string literals (and f-strings), so a statement-level
// parse would buy precision the lint's rules do not need.
func LoadSQLSources(roots ...string) ([]SQLSource, error) {
	var sources []SQLSource
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			switch filepath.Ext(path) {
			case ".go", ".py":
			default:
				return nil
			}
			body, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			sources = append(sources, SQLSource{Path: path, Body: string(body)})
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scanning %s: %w", root, err)
		}
	}
	return sources, nil
}
