// Package main regenerates one transformation-layer migration from the
// schema_master register and the live raw schema, and prints it to stdout. It is
// the human-facing entry point to the generator; the regen-diff CI test asserts
// every committed migration still matches what this produces (normalised), so the
// register and the migrations cannot drift.
//
// Regenerate to a SCRATCH file, then diff and reconcile into the committed
// migration by hand:
//
//	DATABASE_URL=postgres://... gen-transformed -migration 20260706_140000_create_transformed_bucket1.sql > gen.sql
//	diff gen.sql db/migrations/20260706_140000_create_transformed_bucket1.sql
//
// Do NOT redirect over the committed migration. The generator emits only the
// handful of static COMMENT ON statements, not the per-table COMMENT ON catalogue
// entries carried in the committed files, and the regen-diff gate strips comments
// from both sides before comparing, so it cannot see them go missing. Redirecting
// over the committed file would silently drop that column and table metadata while
// still passing the gate. Apply generated structural changes into the committed
// migration by hand, keeping the COMMENT ON block.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/archon-research/stl/stl-verify/data_quality/schemamaster"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "gen-transformed:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("gen-transformed", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	file := fs.String("migration", "", "committed migration file to regenerate (required)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	spec, err := SpecFor(*file)
	if err != nil {
		return err
	}

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
	raw, err := FetchRawSchemas(ctx, pool, spec.tables)
	if err != nil {
		return fmt.Errorf("fetching raw schemas: %w", err)
	}
	sql, err := Generate(spec, reg, raw)
	if err != nil {
		return fmt.Errorf("generating: %w", err)
	}
	_, err = io.WriteString(out, sql)
	return err
}
