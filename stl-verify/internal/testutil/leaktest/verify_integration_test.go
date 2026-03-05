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
	os.Exit(testutil.RunTestsWithLeakCheck(m))
}

// TestLeakDetection_CatchesLeakyGoroutine proves the goroutine leak detector
// works by running the same intentionally-leaking test twice as subprocesses:
//
//  1. With GOEXPERIMENT=goroutineleakprofile → must exit non-zero (leak caught).
//  2. Without the experiment → must exit zero (detection disabled, no failure).
//
// The contrast between the two exit codes proves the detector is responsible
// for the failure, without coupling to any particular output format.
func TestLeakDetection_CatchesLeakyGoroutine(t *testing.T) {
	root := moduleRoot(t)

	runLeakyTest := func(t *testing.T, enableExperiment bool) (exitCode int, output string) {
		t.Helper()

		cmd := exec.Command(
			"go", "test",
			"-tags=leaktest",
			"-run=TestLeakGoroutine",
			"-count=1",
			"./internal/testutil/leaktest/",
		)
		cmd.Dir = root

		env := os.Environ()
		if enableExperiment {
			env = appendOrReplaceEnv(env, "GOEXPERIMENT", "goroutineleakprofile")
		} else {
			env = removeEnv(env, "GOEXPERIMENT")
		}
		cmd.Env = env

		out, err := cmd.CombinedOutput()
		if err != nil {
			return 1, string(out)
		}
		return 0, string(out)
	}

	t.Run("detected_with_experiment", func(t *testing.T) {
		code, output := runLeakyTest(t, true)
		if code == 0 {
			t.Fatalf("expected non-zero exit (leak should be caught), but got 0.\nOutput:\n%s", output)
		}
	})

	t.Run("ignored_without_experiment", func(t *testing.T) {
		code, output := runLeakyTest(t, false)
		if code != 0 {
			t.Fatalf("expected exit 0 (no detection without experiment), but got non-zero.\nOutput:\n%s", output)
		}
	})
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

// removeEnv returns a copy of env with the given key removed.
func removeEnv(env []string, key string) []string {
	prefix := key + "="
	result := make([]string, 0, len(env))
	for _, e := range env {
		if !strings.HasPrefix(e, prefix) {
			result = append(result, e)
		}
	}
	return result
}

// appendOrReplaceEnv returns a copy of env with key=value set. If key already
// exists its value is replaced; otherwise the entry is appended.
func appendOrReplaceEnv(env []string, key, value string) []string {
	prefix := key + "="
	for i, e := range env {
		if strings.HasPrefix(e, prefix) {
			result := make([]string, len(env))
			copy(result, env)
			result[i] = prefix + value
			return result
		}
	}
	return append(env, prefix+value)
}
