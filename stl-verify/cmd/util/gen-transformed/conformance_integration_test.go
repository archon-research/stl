//go:build integration

package main

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const migrationsDir = "../../../db/migrations/"

// TestRegenDiff is the drift gate: it migrates a fresh DB, reads the raw schema of
// every generated table from information_schema, regenerates each committed
// migration from the register + that schema, and asserts it matches the committed
// file after normalisation (SQL comments and whitespace stripped). It fails if the
// register or a raw table changed without regenerating, or if the CTAS / _run /
// _bootstrap projections of a table were hand-edited out of sync.
func TestRegenDiff(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	reg, err := schemamaster.Load()
	if err != nil {
		t.Fatalf("load register: %v", err)
	}
	raw, err := FetchRawSchemas(context.Background(), pool, AllTables())
	if err != nil {
		t.Fatalf("fetch raw schemas: %v", err)
	}

	for _, spec := range MigrationSpecs() {
		t.Run(spec.file, func(t *testing.T) {
			got, err := Generate(spec, reg, raw)
			if err != nil {
				t.Fatalf("generate: %v", err)
			}
			want, err := os.ReadFile(migrationsDir + spec.file)
			if err != nil {
				t.Fatalf("read committed migration: %v", err)
			}
			assertSameSQL(t, got, string(want))
		})
	}
}

// assertSameSQL compares the generated and committed SQL after normalisation and,
// on a mismatch, reports the first diverging offset with surrounding context.
func assertSameSQL(t *testing.T, got, want string) {
	t.Helper()

	ng, nw := normalizeSQL(got), normalizeSQL(want)
	if ng == nw {
		return
	}
	i := 0
	for i < len(ng) && i < len(nw) && ng[i] == nw[i] {
		i++
	}
	lo := max(i-100, 0)
	t.Fatalf("generated migration diverges from the committed file (normalised) at offset %d.\n"+
		"Regenerate with `gen-transformed` after changing the register or a raw table.\n"+
		"  context: %q\n  committed: %q\n  generated: %q",
		i, nw[lo:min(i, len(nw))], nw[i:min(i+120, len(nw))], ng[i:min(i+120, len(ng))])
}

// normalizeSQL strips line comments, COMMENT ON statements (their prose is not
// generated), and collapses whitespace, so the gate compares SQL structure rather
// than the migration's hand-written commentary and formatting.
func normalizeSQL(s string) string {
	var keep []string
	for _, ln := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(ln), "--") {
			continue
		}
		keep = append(keep, ln)
	}
	s = strings.Join(keep, "\n")
	s = regexp.MustCompile(`(?s)COMMENT ON \w+ [^;]*?\s+IS\s+'(?:[^']|'')*';`).ReplaceAllString(s, "")
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}
