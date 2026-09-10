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
var (
	sharedDSN  string
	sharedPool *pgxpool.Pool
)

const packageDBName = "test_schemamaster"

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		TimescaleDSN: &sharedDSN,
		BeforeRun:    func() { sharedPool = testutil.SetupDBForMain(sharedDSN, packageDBName) },
		AfterRun:     func() { testutil.CleanupDBForMain(sharedDSN, sharedPool, packageDBName) },
	}))
}
