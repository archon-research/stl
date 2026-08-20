//go:build integration

package schemamaster_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// The gates read catalog tables scoped to table_schema = 'public' and never
// write, so they run against one migrated database for the whole package.
var sharedPool *pgxpool.Pool

func TestMain(m *testing.M) {
	dsn, cleanup := testutil.StartTimescaleDBForMain()
	sharedPool = testutil.SetupDBForMain(dsn, "test_schemamaster")

	code := m.Run()

	testutil.CleanupDBForMain(dsn, sharedPool, "test_schemamaster")
	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}
