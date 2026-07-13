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

var univ3FullValuePool *pgxpool.Pool

func init() {
	registerTestFileSetup(univ3FullValueSchemaName, func() {
		univ3FullValuePool = testutil.SetupSchemaForMain(sharedDSN, univ3FullValueSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, univ3FullValuePool, univ3FullValueSchemaName)
	})
}

// TestUnderlyingValueCommentCoversUniV3 verifies the catalogue comment now
// documents uni_v3 rows as carrying the full position value and no longer
// lists uni_v3 among the NULL-by-design types.
func TestUnderlyingValueCommentCoversUniV3(t *testing.T) {
	ctx := context.Background()

	var comment string
	err := univ3FullValuePool.QueryRow(ctx, `
		SELECT col_description(a.attrelid, a.attnum)
		FROM pg_attribute a
		WHERE a.attrelid = 'allocation_position'::regclass
		  AND a.attname = 'underlying_value'`).Scan(&comment)
	if err != nil {
		t.Fatalf("read underlying_value comment: %v", err)
	}

	if !strings.Contains(comment, "uni_v3_pool/uni_v3_lp: full position value") {
		t.Errorf("comment %q should document the uni_v3 full position value", comment)
	}
	if strings.Contains(comment, "curve/uni_v3") {
		t.Errorf("comment %q still lists uni_v3 among NULL-by-design types", comment)
	}
	if !strings.Contains(comment, "uncollected fees") {
		t.Errorf("comment %q should state the uncollected-fees exclusion", comment)
	}
}

// TestBalanceCommentsCoverUniV3 verifies the sibling balance/scaled_balance
// catalogue comments also carry the changed uni_v3 semantics: balance is a
// computed full value (no balanceOf exists on a pool) and scaled_balance is
// NULL by design.
func TestBalanceCommentsCoverUniV3(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		column   string
		fragment string
	}{
		{"balance", "uni_v3"},
		{"scaled_balance", "uni_v3"},
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
			if !strings.Contains(comment, tc.fragment) {
				t.Errorf("%s comment %q should document the uni_v3 semantics", tc.column, comment)
			}
		})
	}
}

// TestUniV3PoolTokenRenameMigration exercises the rename against a seeded
// impostor row by re-executing the shipped migration file. The fresh-DB 0-row
// path already ran green during schema setup (the row does not exist then);
// this proves the 1-row rename and that a further re-run stays green.
func TestUniV3PoolTokenRenameMigration(t *testing.T) {
	ctx := context.Background()

	migrationSQL := readUniV3FullValueMigration(t)

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

	if _, err := univ3FullValuePool.Exec(ctx, migrationSQL); err != nil {
		t.Fatalf("re-execute migration against impostor row: %v", err)
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
	if symbol != "AUSDUSDC-UNIV3" {
		t.Fatalf("pool token symbol = %q, want AUSDUSDC-UNIV3", symbol)
	}

	// Idempotency: a second run must change nothing (the symbol filter makes
	// the UPDATE a 0-row no-op, so updated_at must not be rewritten) and raise
	// nothing.
	if _, err := univ3FullValuePool.Exec(ctx, migrationSQL); err != nil {
		t.Fatalf("re-execute migration a second time: %v", err)
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

func readUniV3FullValueMigration(t *testing.T) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve current file path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "../../../../db/migrations", univ3FullValueMigrationFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read migration file: %v", err)
	}
	return string(data)
}
