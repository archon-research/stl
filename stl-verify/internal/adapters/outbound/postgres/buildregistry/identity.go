package buildregistry

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
)

// Identity is the deploy artefact a process runs as (ADR-0006 §2): one row of
// build_registry.
type Identity struct {
	GitHash   string
	Service   string
	BuildTime string
}

// ResolveIdentity reads the running process's artefact identity: the commit from
// buildinfo.Resolve and the service from the binary name. ECR tag immutability
// (VEC-701) makes the <git-sha> tag resolve to one image forever, so the pair names
// the artefact on its own. Both parts are required — a row whose writer cannot be
// named is unreproducible — so a missing part is an error.
func ResolveIdentity() (Identity, error) {
	gitHash, buildTime := buildinfo.Resolve()
	return resolveIdentity(gitHash, buildTime, os.Args[0])
}

func resolveIdentity(gitHash, buildTime, argv0 string) (Identity, error) {
	if gitHash == "" {
		return Identity{}, fmt.Errorf("git hash not available: build with VCS info or set BUILD_GIT_HASH env var")
	}
	service := filepath.Base(argv0)
	if argv0 == "" || service == "." || service == string(filepath.Separator) {
		return Identity{}, fmt.Errorf("service name not available: os.Args[0] is %q", argv0)
	}
	return Identity{GitHash: gitHash, Service: service, BuildTime: buildTime}, nil
}
