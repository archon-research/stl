//go:build integration

package schemamaster_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// The gates read catalog tables scoped to table_schema = 'public' and never
// write, so they need the migrated public schema itself, not a per-test schema.
var sharedPool *pgxpool.Pool

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	testutil.EnsurePublicMigrations(dsn)
	sharedPool = testutil.ConnectPoolForMain(dsn)

	code := m.Run()

	sharedPool.Close()
	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}
