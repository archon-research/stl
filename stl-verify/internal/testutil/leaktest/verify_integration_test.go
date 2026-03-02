//go:build integration

package leaktest_test

import (
	"os"
	"os/exec"
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
	goBin := goWithExperimentSupport(t)
	root := moduleRoot(t)

	runLeakyTest := func(t *testing.T, enableExperiment bool) (exitCode int, output string) {
		t.Helper()

		cmd := exec.Command(
			goBin, "test",
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

// goWithExperimentSupport returns the path to a Go binary that supports
// GOEXPERIMENT=goroutineleakprofile. It skips the test if no suitable binary
// is found.
func goWithExperimentSupport(t *testing.T) string {
	t.Helper()

	env := appendOrReplaceEnv(os.Environ(), "GOEXPERIMENT", "goroutineleakprofile")

	// Try the default go binary first, then the official SDK binary.
	for _, bin := range []string{"go", "go1.26.0"} {
		path, err := exec.LookPath(bin)
		if err != nil {
			continue
		}
		cmd := exec.Command(path, "version")
		cmd.Env = env
		if err := cmd.Run(); err == nil {
			return path
		}
	}

	t.Skip("no Go binary with GOEXPERIMENT=goroutineleakprofile support found")
	return ""
}

// moduleRoot returns the module root directory by looking for go.mod relative
// to the test binary's working directory.
func moduleRoot(t *testing.T) string {
	t.Helper()

	// go test sets the working directory to the package directory. Walk up
	// to find the module root (where go.mod lives).
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getting working directory: %v", err)
	}

	for {
		if _, err := os.Stat(dir + "/go.mod"); err == nil {
			return dir
		}
		parent := dir[:strings.LastIndex(dir, "/")]
		if parent == dir {
			t.Fatal("could not find module root (go.mod)")
		}
		dir = parent
	}
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
