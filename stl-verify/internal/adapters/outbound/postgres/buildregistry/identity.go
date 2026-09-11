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
	// "unknown" is rejected alongside "": Dockerfile.common defaults the
	// versioning args to it (ORB-366), so an image built without them passes
	// BUILD_GIT_HASH=unknown -- a non-empty string that would otherwise sail
	// through this check and be inserted. build_registry is insert-only, so
	// that row, and every governed row whose build_id references it, would be
	// permanently attributed to an artefact nobody can identify.
	if gitHash == "" || gitHash == "unknown" {
		return Identity{}, fmt.Errorf("git hash not available (got %q): build with VCS info or set BUILD_GIT_HASH env var", gitHash)
	}
	service := filepath.Base(argv0)
	if argv0 == "" || service == "." || service == string(filepath.Separator) {
		return Identity{}, fmt.Errorf("service name not available: os.Args[0] is %q", argv0)
	}
	return Identity{GitHash: gitHash, Service: service, BuildTime: buildTime}, nil
}
