//go:build integration

package leaktest_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{}))
}

// TestLeakDetection_CatchesLeakyGoroutine proves the goroutine leak detector
// works by running an intentionally-leaking test as a subprocess: it must exit
// non-zero. Asserting on the exit code alone keeps this uncoupled from any
// particular output format.
func TestLeakDetection_CatchesLeakyGoroutine(t *testing.T) {
	cmd := exec.Command(
		"go", "test",
		"-tags=leaktest",
		"-run=TestLeakGoroutine",
		"-count=1",
		"./internal/testutil/leaktest/",
	)
	cmd.Dir = moduleRoot(t)

	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected non-zero exit (leak should be caught), but got 0.\nOutput:\n%s", out)
	}
}

// moduleRoot returns the module root directory via "go env GOMOD".
func moduleRoot(t *testing.T) string {
	t.Helper()

	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	gomod := strings.TrimSpace(string(out))
	if gomod == "" {
		t.Fatal("go env GOMOD returned empty string (not in a module?)")
	}
	return filepath.Dir(gomod)
}
