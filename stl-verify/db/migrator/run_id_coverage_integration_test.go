//go:build integration

package migrator_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// governedTableTypes are the schemamaster types whose rows a calculation or a served data
// point can read, so every row needs its writer run (ADR-0006 §1, §2).
var governedTableTypes = map[string]bool{"raw_pipeline": true, "dimension": true, "config": true}

// TestGovernedTablesCarryRunID re-derives the governed set from schema_master.json and asserts
// each table has a nullable BIGINT run_id. Driven by the register rather than a list, so a
// governed table added without the column fails here instead of silently writing rows nobody
// can attribute to a run.
func TestGovernedTablesCarryRunID(t *testing.T) {
	ctx := context.Background()
	pool, cleanup := setupPostgres(ctx, t)
	defer cleanup()
	if err := migrator.New(pool, getMigrationsPath()).ApplyAll(ctx); err != nil {
		t.Fatalf("migrations failed: %v", err)
	}

	reg, err := schemamaster.Load()
	if err != nil {
		t.Fatalf("load register: %v", err)
	}

	checked := 0
	for table, meta := range reg.Tables {
		if !governedTableTypes[meta.Type] {
			continue
		}
		checked++
		t.Run(table, func(t *testing.T) {
			var dataType, isNullable string
			err := pool.QueryRow(ctx, `
				SELECT data_type, is_nullable FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = $1 AND column_name = 'run_id'`, table,
			).Scan(&dataType, &isNullable)
			if errors.Is(err, pgx.ErrNoRows) {
				t.Fatalf("%s is governed (%s) but has no run_id column; add it in a migration (ADR-0006 §2)", table, meta.Type)
			}
			if err != nil {
				t.Fatalf("read run_id column of %s: %v", table, err)
			}
			if dataType != "bigint" {
				t.Errorf("%s.run_id is %s, want bigint (writer_run.id is BIGSERIAL)", table, dataType)
			}
			if isNullable != "YES" {
				t.Errorf("%s.run_id is NOT NULL; it must be nullable, NULL meaning written before run tracking", table)
			}
		})
	}
	if checked == 0 {
		t.Fatal("no governed table found in the register; the type vocabulary or the register changed under this test")
	}
}
