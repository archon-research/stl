// Package main regenerates the bucket-1 transformation-layer migration from the
// schema_master register and the live raw schema, and prints it to stdout. It is
// the human-facing entry point to the generator; the regen-diff CI test
// (internal/transformgen) asserts the committed migration still matches what this
// produces (normalised), so the register and the migration cannot drift.
//
// Regenerate to a SCRATCH file, then diff and reconcile into the committed
// migration by hand:
//
//	DATABASE_URL=postgres://... gen-transformed > gen.sql
//	diff gen.sql db/migrations/20260706_140000_create_transformed_bucket1.sql
//
// Do NOT redirect over the committed migration. The generator emits only the
// handful of static COMMENT ON statements, not the ~190 per-table COMMENT ON
// catalogue entries carried in the committed file, and the regen-diff gate
// strips comments from both sides before comparing, so it cannot see them go
// missing. Redirecting over the committed file would silently drop that column
// and table metadata while still passing the gate. Apply generated structural
// changes into the committed migration by hand, keeping the COMMENT ON block.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/transformgen"
)

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "gen-transformed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	dsn := env.Get("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/stl_verify?sslmode=disable")
	pool, err := postgres.PoolOpener(postgres.DefaultDBConfig(dsn))(ctx)
	if err != nil {
		return fmt.Errorf("opening database: %w", err)
	}
	defer pool.Close()

	reg, err := schemamaster.Load()
	if err != nil {
		return fmt.Errorf("loading register: %w", err)
	}
	raw, err := transformgen.FetchRawSchemas(ctx, pool, transformgen.Bucket1Tables())
	if err != nil {
		return fmt.Errorf("fetching raw schemas: %w", err)
	}
	sql, err := transformgen.GenerateBucket1(reg, raw)
	if err != nil {
		return fmt.Errorf("generating: %w", err)
	}
	_, err = fmt.Print(sql)
	return err
}
