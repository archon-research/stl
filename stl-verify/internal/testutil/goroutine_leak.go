package testutil

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
)

// CheckGoroutineLeaks triggers garbage collection and inspects the
// goroutineleak pprof profile (available with GOEXPERIMENT=goroutineleakprofile
// in Go 1.26+). If the experiment is not enabled the profile is nil and
// exitCode is returned unchanged. When leaked goroutines are detected, their
// stacks are printed to stderr and exit code 1 is returned.
func CheckGoroutineLeaks(exitCode int) int {
	// Two GC cycles ensure finalizers have run and unreachable goroutines
	// blocked on concurrency primitives are marked as leaked.
	runtime.GC()
	runtime.GC()

	profile := pprof.Lookup("goroutineleak")
	if profile == nil {
		// Experiment not enabled — nothing to check.
		return exitCode
	}

	if count := profile.Count(); count > 0 {
		fmt.Fprintf(os.Stderr, "\n=== GOROUTINE LEAK DETECTED: %d leaked goroutine(s) ===\n", count)
		profile.WriteTo(os.Stderr, 1)
		fmt.Fprintln(os.Stderr, "=== END GOROUTINE LEAK REPORT ===")
		return 1
	}

	return exitCode
}

// RunTestsWithLeakCheck is a convenience wrapper for packages that need no
// custom TestMain setup. It runs all tests via m.Run() and then checks for
// goroutine leaks. Returns the exit code suitable for os.Exit.
func RunTestsWithLeakCheck(m *testing.M) int {
	code := m.Run()
	return CheckGoroutineLeaks(code)
}
