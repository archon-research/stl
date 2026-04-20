package testutil

import (
	"strings"
	"testing"
)

// IsContainerRuntimeUnavailable reports whether testcontainers failed because
// no local Docker-compatible runtime is available in this environment.
func IsContainerRuntimeUnavailable(err error) bool {
	if err == nil {
		return false
	}

	msg := strings.ToLower(err.Error())
	for _, needle := range []string{
		"failed to create docker provider",
		"rootless docker not found",
		"cannot connect to the docker daemon",
		"is the docker daemon running",
		"docker daemon is not running",
		"podman machine",
		"cannot connect to podman",
		"podman.socket",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}

	return false
}

// HandleContainerRuntimeError is a no-op when err is nil; otherwise it fails
// the current test via tb.Fatalf. When IsContainerRuntimeUnavailable reports
// true, the fatal message is prefixed to make the root cause clear.
func HandleContainerRuntimeError(tb testing.TB, err error, subject string) {
	tb.Helper()
	if err == nil {
		return
	}
	if IsContainerRuntimeUnavailable(err) {
		tb.Fatalf("%s: container runtime unavailable: %v", subject, err)
	}
	tb.Fatalf("%s: %v", subject, err)
}
