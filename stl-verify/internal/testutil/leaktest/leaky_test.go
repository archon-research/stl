//go:build leaktest

package leaktest

import (
	"os"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunTestsWithLeakCheck(m))
}

func TestLeakGoroutine(t *testing.T) {
	LeakGoroutine()
}
