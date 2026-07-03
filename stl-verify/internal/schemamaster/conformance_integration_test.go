//go:build integration

package schemamaster_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/schemamaster"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestSchemaConformance is the gate: it migrates a fresh DB, reads every live base-table
// column from information_schema, and fails if any governed column is unregistered or has
// drifted from the register. Generalizes the hardcoded build_id/processing_version existence
// checks in db/migrator/migrator_integration_test.go into a data-driven sweep.
func TestSchemaConformance(t *testing.T) {
	pool, _, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	reg, err := schemamaster.Load()
	if err != nil {
		t.Fatalf("load register: %v", err)
	}

	rows, err := pool.Query(context.Background(), `
		SELECT c.table_name, c.column_name, c.data_type, c.is_nullable
		FROM information_schema.columns c
		JOIN information_schema.tables t
		  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		WHERE c.table_schema = 'public' AND t.table_type = 'BASE TABLE'`)
	if err != nil {
		t.Fatalf("query information_schema: %v", err)
	}
	defer rows.Close()

	var live []schemamaster.Column
	for rows.Next() {
		var col schemamaster.Column
		var isNullable string
		if err := rows.Scan(&col.Table, &col.Name, &col.DataType, &isNullable); err != nil {
			t.Fatalf("scan: %v", err)
		}
		col.Nullable = isNullable == "YES"
		live = append(live, col)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(live) == 0 {
		t.Fatal("no live columns found — migrations did not apply?")
	}

	violations := reg.Check(live)
	for _, v := range violations {
		t.Errorf("%s.%s: %s — %s", v.Table, v.Column, v.Kind, v.Detail)
	}
	if len(violations) > 0 {
		t.Fatalf("%d schema conformance violation(s); register (schema_master.yaml) and the live schema disagree", len(violations))
	}
	t.Logf("schema conformance: %d live columns checked, all conform or registered", len(live))
}
