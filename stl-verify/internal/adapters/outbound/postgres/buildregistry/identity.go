package buildregistry

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
)

const (
	// ImageDigestEnv carries the digest of the running container image. The deploy
	// pipeline injects it; nothing inside a container can read its own digest.
	ImageDigestEnv = "IMAGE_DIGEST"
	// DevIdentityEnv, when "1", lets a process with no IMAGE_DIGEST register as the
	// DevImageDigest artefact. For `go run`, kind and the test suite; never set in a
	// deployed environment, where a missing digest must stop the process.
	DevIdentityEnv = "STL_DEV_IDENTITY"
	// DevImageDigest is the image_digest a dev-identity process registers under.
	DevImageDigest = "dev"
)

var imageDigestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// Identity is the deploy artefact a process runs as (ADR-0006 §2): one row of
// build_registry.
type Identity struct {
	GitHash     string
	Service     string
	ImageDigest string
	BuildTime   string
}

// ResolveIdentity reads the running process's artefact identity: the commit from
// buildinfo.Resolve, the service from the binary name, the image digest from
// IMAGE_DIGEST. Every part is required — a row whose writer cannot be named is
// unreproducible — so any missing part is an error, except the digest under
// STL_DEV_IDENTITY=1.
func ResolveIdentity() (Identity, error) {
	gitHash, buildTime := buildinfo.Resolve()
	return resolveIdentity(gitHash, buildTime, os.Args[0], os.Getenv(ImageDigestEnv), os.Getenv(DevIdentityEnv) == "1")
}

func resolveIdentity(gitHash, buildTime, argv0, imageDigest string, devIdentity bool) (Identity, error) {
	if gitHash == "" {
		return Identity{}, fmt.Errorf("git hash not available: build with VCS info or set BUILD_GIT_HASH env var")
	}
	service := filepath.Base(argv0)
	if argv0 == "" || service == "." || service == string(filepath.Separator) {
		return Identity{}, fmt.Errorf("service name not available: os.Args[0] is %q", argv0)
	}
	digest, err := resolveImageDigest(imageDigest, devIdentity)
	if err != nil {
		return Identity{}, err
	}
	return Identity{GitHash: gitHash, Service: service, ImageDigest: digest, BuildTime: buildTime}, nil
}

func resolveImageDigest(imageDigest string, devIdentity bool) (string, error) {
	if imageDigest == "" {
		if devIdentity {
			return DevImageDigest, nil
		}
		return "", fmt.Errorf("%s is not set: the deploy pipeline must inject the running image's digest (set %s=1 for a local run)", ImageDigestEnv, DevIdentityEnv)
	}
	if !imageDigestPattern.MatchString(imageDigest) {
		return "", fmt.Errorf("%s=%q is not an image digest (want sha256:<64 hex>)", ImageDigestEnv, imageDigest)
	}
	return imageDigest, nil
}
