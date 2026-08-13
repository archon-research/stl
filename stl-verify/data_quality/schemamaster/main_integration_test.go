//go:build integration

package schemamaster_test

import (
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// sharedPool points at the public schema of the package's shared container. Both
// conformance gates only read catalog tables scoped to table_schema = 'public',
// so they need the migrated public schema itself and never write — one migrated
// database serves the whole package.
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
