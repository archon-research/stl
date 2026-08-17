//go:build integration

package s3_test

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedLocalStackCfg testutil.LocalStackConfig

func TestMain(m *testing.M) {
	cfg, cleanup := testutil.StartLocalStackForMain("s3")
	sharedLocalStackCfg = cfg

	code := m.Run()

	cleanup()
	code = testutil.CheckGoroutineLeaks(code)
	os.Exit(code)
}
