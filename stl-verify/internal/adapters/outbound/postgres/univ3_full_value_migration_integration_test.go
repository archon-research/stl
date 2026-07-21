//go:build integration

package postgres

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const univ3FullValueSchemaName = "test_univ3_full_value"

// The AUSD/USDC Uniswap V3 pool contract whose token row the migration renames.
const univ3PoolAddressHex = "bafead7c60ea473758ed6c6021505e8bbd7e8e5d"

const univ3FullValueMigrationFile = "20260713_140000_univ3_full_position_value.sql"

// 20260714_170000 renames the same row from 'AUSDUSDC-UNIV3' to skyeco's
// 'UNIV3-LP-AUSD-USDC'; it runs after univ3FullValueMigrationFile, so the two
// in sequence produce the skyeco end-state.
const univ3SkyecoRenameMigrationFile = "20260714_170000_rename_univ3_symbol_skyeco.sql"

var univ3FullValuePool *pgxpool.Pool

func init() {
	registerTestFileSetup(univ3FullValueSchemaName, func() {
		univ3FullValuePool = testutil.SetupSchemaForMain(sharedDSN, univ3FullValueSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, univ3FullValuePool, univ3FullValueSchemaName)
	})
}

// TestAllocationPositionCatalogueComments verifies the rewritten catalogue
// comments describe what the writers actually persist (these comments are the
// catalogue's source of truth): uni_v3 rows carry the full position value
// with explicit zero rows on exit, balance no longer claims "never 0"
// (curve/erc4626/uni_v3 write explicit zeros), scaled_balance no longer
// claims aToken-only population (curve/erc4626 store share counts), and the
// NULL-pricing note no longer claims an unconditional balance-based fallback
// (allowlisted underlying-value tokens surface NULL amount_usd).
func TestAllocationPositionCatalogueComments(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		column   string
		want     []string
		rejected []string
	}{
		{
			column: "underlying_value",
			want: []string{
				"uni_v3_pool/uni_v3_lp: full position value",
				"uncollected fees",
				"explicit 0",
				"NULL amount_usd",
			},
			rejected: []string{
				"curve/uni_v3",
				"consumers fall back to balance-based pricing",
			},
		},
		{
			column: "balance",
			want:   []string{"uni_v3", "explicit 0"},
			rejected: []string{
				"Populated with the real balance (not 0)",
			},
		},
		{
			column: "scaled_balance",
			want:   []string{"uni_v3", "share count"},
			rejected: []string{
				"Populated only for aTokens",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.column, func(t *testing.T) {
			var comment string
			err := univ3FullValuePool.QueryRow(ctx, `
				SELECT col_description(a.attrelid, a.attnum)
				FROM pg_attribute a
				WHERE a.attrelid = 'allocation_position'::regclass
				  AND a.attname = $1`, tc.column).Scan(&comment)
			if err != nil {
				t.Fatalf("read %s comment: %v", tc.column, err)
			}
			for _, fragment := range tc.want {
				if !strings.Contains(comment, fragment) {
					t.Errorf("%s comment %q should contain %q", tc.column, comment, fragment)
				}
			}
			for _, fragment := range tc.rejected {
				if strings.Contains(comment, fragment) {
					t.Errorf("%s comment %q still carries the false claim %q", tc.column, comment, fragment)
				}
			}
		})
	}
}

// TestUniV3PoolTokenRenameMigration exercises the two rename migrations in
// production order against a seeded impostor row: 20260713_140000 renames the
// old hint-asset symbol to 'AUSDUSDC-UNIV3', then 20260714_170000 renames that
// to skyeco's 'UNIV3-LP-AUSD-USDC', which is the end-state the token row must
// carry. The fresh-DB 0-row path of both already ran green during schema setup
// (the row does not exist then); this proves the 1-row rename through the
// sequence and that re-running the second migration stays green.
func TestUniV3PoolTokenRenameMigration(t *testing.T) {
	ctx := context.Background()

	fullValueSQL := readMigrationFile(t, univ3FullValueMigrationFile)
	skyecoRenameSQL := readMigrationFile(t, univ3SkyecoRenameMigrationFile)

	if _, err := univ3FullValuePool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT (chain_id) DO NOTHING`,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	// Seed the impostor row the old ingestion wrote: the POOL address labeled
	// with the hint asset's symbol.
	if _, err := univ3FullValuePool.Exec(ctx, `
		INSERT INTO token (chain_id, address, symbol, decimals, metadata, updated_at)
		VALUES (1, decode($1, 'hex'), 'USDC', 6, '{}', NOW())
		ON CONFLICT (chain_id, address) DO UPDATE SET symbol = 'USDC'`,
		univ3PoolAddressHex,
	); err != nil {
		t.Fatalf("seed impostor token row: %v", err)
	}

	if _, err := univ3FullValuePool.Exec(ctx, fullValueSQL); err != nil {
		t.Fatalf("re-execute full-value migration against impostor row: %v", err)
	}
	if _, err := univ3FullValuePool.Exec(ctx, skyecoRenameSQL); err != nil {
		t.Fatalf("re-execute skyeco-rename migration: %v", err)
	}

	var symbol string
	var updatedAtAfterRename string
	if err := univ3FullValuePool.QueryRow(ctx, `
		SELECT symbol, updated_at::text FROM token
		WHERE chain_id = 1 AND address = decode($1, 'hex')`,
		univ3PoolAddressHex,
	).Scan(&symbol, &updatedAtAfterRename); err != nil {
		t.Fatalf("read renamed token row: %v", err)
	}
	if symbol != "UNIV3-LP-AUSD-USDC" {
		t.Fatalf("pool token symbol = %q, want UNIV3-LP-AUSD-USDC", symbol)
	}

	// Idempotency: re-running the skyeco rename must change nothing (the symbol
	// filter makes the UPDATE a 0-row no-op, so updated_at must not be
	// rewritten) and raise nothing. The full-value migration is NOT re-run here
	// because in production the migrator applies each file exactly once, in
	// order; running it after the skyeco rename would revert the symbol.
	if _, err := univ3FullValuePool.Exec(ctx, skyecoRenameSQL); err != nil {
		t.Fatalf("re-execute skyeco-rename migration a second time: %v", err)
	}
	var updatedAtAfterRerun string
	if err := univ3FullValuePool.QueryRow(ctx, `
		SELECT updated_at::text FROM token
		WHERE chain_id = 1 AND address = decode($1, 'hex')`,
		univ3PoolAddressHex,
	).Scan(&updatedAtAfterRerun); err != nil {
		t.Fatalf("read token row after rerun: %v", err)
	}
	if updatedAtAfterRerun != updatedAtAfterRename {
		t.Fatalf("updated_at changed on re-run: %s -> %s (UPDATE should be a no-op)",
			updatedAtAfterRename, updatedAtAfterRerun)
	}
}

func readMigrationFile(t *testing.T, filename string) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "../../../../db/migrations", filename)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration file %s: %v", filename, err)
	}
	return string(data)
}
