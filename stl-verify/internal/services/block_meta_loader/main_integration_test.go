//go:build integration

package block_meta_loader

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Shared, container-backed dependencies started once for the whole package. The
// DB is isolated per test via testutil.SetupTestSchema; S3 buckets are named per
// test so a single LocalStack can back every subtest.
var (
	sharedDSN           string
	sharedLocalStackCfg testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	dsn, dbCleanup := testutil.StartTimescaleDBForMain()
	sharedDSN = dsn

	lsCfg, lsCleanup := testutil.StartLocalStackForMain("s3")
	sharedLocalStackCfg = lsCfg

	code := m.Run()

	lsCleanup()
	dbCleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}
