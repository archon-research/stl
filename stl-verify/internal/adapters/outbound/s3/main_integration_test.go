//go:build integration

package s3_test

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedLocalStackCfg testutil.LocalStackConfig

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{
		LocalStack:         &sharedLocalStackCfg,
		LocalStackServices: "s3",
	}))
}
