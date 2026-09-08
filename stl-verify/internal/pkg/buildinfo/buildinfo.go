package buildinfo

import (
	"os"
	"runtime/debug"
)

// Set via ldflags at build time by local `go build` callers that pass them:
//
//	-X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.GitCommit=...
//	-X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.BuildTime=...
//
// Released images deliberately stamp nothing (ORB-366) — see Populate.
var (
	GitCommit string
	BuildTime string
)

// setIfEmpty is how every source below defers to the one before it: the first
// source with a value wins, and a later one never overwrites it.
func setIfEmpty(dst *string, value string) {
	if *dst == "" {
		*dst = value
	}
}

// populateFromVCS fills commit and buildTime from Go's embedded VCS info when
// they haven't already been set via ldflags. It only ever fires for a binary
// built where .git was reachable: `go build`/`go run` from a checkout. The
// released images build from the stl-verify/ directory as their context, which
// carries no .git, so Go embeds no VCS info there.
func populateFromVCS(commit, buildTime *string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			setIfEmpty(commit, setting.Value)
		case "vcs.time":
			setIfEmpty(buildTime, setting.Value)
		}
	}
}

// Populate fills commit, branch and buildTime from the first source that has a
// value: an ldflags -X stamp, then Go's embedded VCS info, then the
// BUILD_GIT_HASH / BUILD_GIT_BRANCH / BUILD_TIME environment variables.
//
// The env vars are how a released image reports its build, and they are the
// point of ORB-366. A build stamp compiled into the binary makes an unchanged
// service produce a different binary — and so a different image layer — on
// every commit, which rolls every pod whether or not its code changed. The
// values ride in the image *config* as ENV instead: config is not a layer, so
// two builds of the same source still produce identical layer digests, which is
// what lets the deploy keep the older tag. Dockerfile.common sets all three at
// the end of the runtime stage.
//
// Consequence worth knowing at a call site: what a pod reports is the commit of
// the *image it is running*, not the commit of the deploy that last ran. When
// the deploy keeps an older tag because the layers were identical, those two
// differ on purpose, and the image's own commit is the honest answer — it is
// also the one build_registry is keyed on.
func Populate(commit, branch, buildTime *string) {
	populateFromVCS(commit, buildTime)
	populateFromEnv(commit, branch, buildTime)
}

// populateFromEnv is the last resort, and the only one a released image has.
// Split from Populate so it can be tested on its own: whether the VCS position
// above it yields anything depends on how the calling binary was built, which a
// test cannot control.
func populateFromEnv(commit, branch, buildTime *string) {
	setIfEmpty(commit, os.Getenv("BUILD_GIT_HASH"))
	setIfEmpty(branch, os.Getenv("BUILD_GIT_BRANCH"))
	setIfEmpty(buildTime, os.Getenv("BUILD_TIME"))
}

// Resolve returns the commit and build time of the running binary, from the
// same ordered sources as Populate. It reads nothing outside the process, so a
// service can report its service.version to telemetry before it opens any
// dependency.
//
// buildregistry resolves the same commit against build_registry, so the
// service_version on a metric and the build_id on a row name one build.
func Resolve() (commit, buildTime string) {
	commit, buildTime = GitCommit, BuildTime
	var branch string
	Populate(&commit, &branch, &buildTime)
	return commit, buildTime
}

// GitHash is Resolve's commit alone, for callers that only need to name the build.
func GitHash() string {
	commit, _ := Resolve()
	return commit
}
