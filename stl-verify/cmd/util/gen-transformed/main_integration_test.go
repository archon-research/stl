//go:build integration

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestRun_EmitsTheRequestedMigration covers the entry point's wiring: -migration
// selects one spec, and the SQL written out is that migration and no other. The
// content itself is the regen-diff test's subject; what matters here is that the
// flag reaches the generator and the result reaches the writer.
func TestRun_EmitsTheRequestedMigration(t *testing.T) {
	_, dsn, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()
	t.Setenv("DATABASE_URL", dsn)

	specs := MigrationSpecs()
	want, other := specs[len(specs)-1], specs[0]

	var out bytes.Buffer
	if err := run(context.Background(), []string{"-migration", want.file}, &out); err != nil {
		t.Fatalf("run: %v", err)
	}

	sql := out.String()
	if !strings.Contains(sql, "VALUES ('"+want.file+"')") {
		t.Errorf("output does not self-register as %q:\n%s", want.file, sql)
	}
	if strings.Contains(sql, "VALUES ('"+other.file+"')") {
		t.Errorf("output also emitted %q; -migration must select exactly one", other.file)
	}
}
