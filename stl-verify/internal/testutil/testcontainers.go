package testutil

import "strings"

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
