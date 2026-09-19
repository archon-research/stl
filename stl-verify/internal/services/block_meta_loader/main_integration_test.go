//go:build integration

package block_meta_loader

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Shared, container-backed dependencies. RunShared owns the lifecycle: it publishes every handle
// before the tests run, stops services in the order the leak check needs, and returns the code
// rather than letting m.Run own it. Hand-rolling that is what stl-verify/AGENTS.md asks packages
// not to do, and it also kept this package off the shard's shared LocalStack.
//
// The DB is isolated per test via testutil.SetupTestDB; S3 buckets are named per test, so one
// LocalStack backs every subtest.
var (
	sharedDSN           string
	sharedLocalStackCfg testutil.LocalStackConfig
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		PostgresDSN:       &sharedDSN,
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "s3",
	}))
}
