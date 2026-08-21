package schemamaster

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// ReferenceRead is an append-on-change reference table (ADR-0006 §4) plus the objects its
// conversion creates. Which reference rows a reader used decides which data rows it used.
type ReferenceRead struct {
	Table       string `json:"table"`
	CurrentView string `json:"current_view"`
	AsOf        string `json:"as_of"`
	Reason      string `json:"reason"`
}

// WallClockExempt sanctions one file for wall-clock use alongside a reference read (a
// staleness computation). Never excuses a wall-clock effective_at, which is checked separately.
type WallClockExempt struct {
	Path   string `json:"path"`
	Reason string `json:"reason"`
}

// SQLSource is one scanned file of application SQL: repository adapters embed their SQL as
// string literals, so the lint reads the file body rather than parsing statements.
type SQLSource struct {
	Path string
	Body string
}

// wallClock matches every spelling of "when it runs", so the rule does not depend on a
// reviewer knowing which ones the lint knows.
var wallClock = regexp.MustCompile(`(?i)\b(now\s*\(\s*\)|current_date|current_timestamp|localtimestamp)`)

// CheckCalculationSQL lints calculation and writer SQL against ADR-0006 §4: a read of a
// converted reference table must go through <table>_as_of(<recorded effective_at>), and a
// source that reads one must not consult the wall clock at all.
//
// Sources are files, not statements: repository SQL lives in string literals, so a file
// that reads a reference table is treated as calculation SQL throughout, with sanctioned
// exceptions named in wall_clock_exempt.
func (r *Register) CheckCalculationSQL(sources []SQLSource) []Violation {
	var vs []Violation
	for _, src := range sources {
		code := SQLSource{Path: src.Path, Body: stripLineComments(src.Body)}
		for _, ref := range r.ReferenceReads {
			vs = append(vs, ref.check(code, r.wallClockExempt(src.Path))...)
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

// wallClockExempt reports whether path is sanctioned for wall-clock use. Suffix match: the
// register records repo-relative paths, the scan yields absolute ones.
func (r *Register) wallClockExempt(path string) bool {
	for _, e := range r.WallClockExempt {
		if strings.HasSuffix(filepath.ToSlash(path), e.Path) {
			return true
		}
	}
	return false
}

// check reports every way src reads ref without a pinned effective date. Violation.Table
// carries the source path and Column the reference table, matching "<table>.<column>: <kind>".
func (ref ReferenceRead) check(src SQLSource, wallClockAllowed bool) []Violation {
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
	args := asOfArguments(ref.AsOf, src.Body)
	for _, arg := range args {
		if wallClock.MatchString(src.Body[arg.from:arg.to]) {
			finding("wall_clock_effective_at", fmt.Sprintf(
				"%s(%s) pins the read to the wall clock; pass the run's recorded effective_at as a parameter (ADR-0006 §4)",
				ref.AsOf, strings.TrimSpace(src.Body[arg.from:arg.to])))
		}
	}
	if wallClockAllowed || !ref.isReadBy(src.Body) {
		return vs
	}
	for _, m := range wallClock.FindAllStringIndex(src.Body, -1) {
		if within(m[0], args) {
			continue // reported as wall_clock_effective_at above
		}
		finding("wall_clock_in_reference_read_sql", fmt.Sprintf(
			"%q sits in SQL that reads %s; calculation SQL must take every date as a recorded parameter (ADR-0006 §4). Sanction a legitimate staleness computation in schema_master.json wall_clock_exempt",
			strings.TrimSpace(src.Body[m[0]:m[1]]), ref.Table))
	}
	return vs
}

// isReadBy reports whether body reads ref at all, in any of its three forms.
func (ref ReferenceRead) isReadBy(body string) bool {
	return strings.Contains(body, ref.AsOf) ||
		strings.Contains(body, ref.CurrentView) ||
		len(unpinnedReads(ref.Table, body)) > 0
}

// within reports whether index i falls inside one of the argument spans.
func within(i int, spans []span) bool {
	for _, s := range spans {
		if i >= s.from && i < s.to {
			return true
		}
	}
	return false
}

// stripLineComments drops line comments (SQL `--`, Python `#`, Go `//`) before matching, so
// naming a banned object in the comment explaining the ban is not itself a violation.
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

// unpinnedReads returns the FROM/JOIN clauses that read table directly; an INSERT is how a
// version is appended, so it is not one. RE2 has no lookahead, so the trailing characters are
// captured and checked instead — that is what separates the table from its _as_of object.
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

// span is a half-open [from, to) range of body: one asOf call's argument text.
type span struct{ from, to int }

// asOfArguments returns the argument span of every asOf call in body, tracking nesting so a
// cast or a function call inside the argument does not truncate it.
func asOfArguments(asOf, body string) []span {
	var args []span
	for _, open := range callSites(asOf, body) {
		depth := 0
		for i := open; i < len(body); i++ {
			switch body[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					args = append(args, span{open + 1, i})
				}
			}
			if depth == 0 {
				break
			}
		}
	}
	return args
}

// callSites returns the index of the opening parenthesis of every asOf(...) call. The
// identifier and the parenthesis may be separated by whitespace, which is valid SQL.
func callSites(asOf, body string) []int {
	pattern := regexp.MustCompile(`(?i)` + regexp.QuoteMeta(asOf) + `\s*\(`)
	var sites []int
	for _, m := range pattern.FindAllStringIndex(body, -1) {
		sites = append(sites, m[1]-1)
	}
	return sites
}

// LoadSQLSources reads every Go and Python file under the given roots, whole: repository SQL
// lives in string literals, so a statement-level parse buys precision these rules don't need.
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
