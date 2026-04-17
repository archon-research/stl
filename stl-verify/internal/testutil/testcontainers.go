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
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}

	return false
}

// HandleContainerRuntimeError fails the current test immediately. We keep the
// unavailable-runtime detection only to improve the failure message.
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
