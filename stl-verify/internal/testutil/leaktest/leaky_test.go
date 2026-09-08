//go:build leaktest

package leaktest

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{}))
}

func TestLeakGoroutine(t *testing.T) {
	LeakGoroutine()
}
