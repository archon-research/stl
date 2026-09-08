package buildinfo

import (
	"os"
	"runtime/debug"
)

// Set via ldflags at build time:
//
//	-X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.GitCommit=...
//	-X github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo.BuildTime=...
var (
	GitCommit string
	BuildTime string
)

// PopulateFromVCS fills commit and buildTime from Go's embedded VCS info
// when they haven't already been set via ldflags.
func PopulateFromVCS(commit, buildTime *string) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if *commit == "" {
				*commit = setting.Value
			}
		case "vcs.time":
			if *buildTime == "" {
				*buildTime = setting.Value
			}
		}
	}
}

// Resolve returns the commit and build time of the running binary: the ldflags
// values, then Go's embedded VCS info, then BUILD_GIT_HASH for the commit
// (which is how `make run-*` stamps a `go run` build, where Go embeds no VCS
// info). It reads nothing outside the process, so a service can report its
// service.version to telemetry before it opens any dependency.
//
// buildregistry resolves the same commit against build_registry, so the
// service_version on a metric and the build_id on a row name one build.
func Resolve() (commit, buildTime string) {
	commit, buildTime = GitCommit, BuildTime
	if commit == "" || buildTime == "" {
		PopulateFromVCS(&commit, &buildTime)
	}
	if commit == "" {
		commit = os.Getenv("BUILD_GIT_HASH")
	}
	return commit, buildTime
}

// GitHash is Resolve's commit alone, for callers that only need to name the build.
func GitHash() string {
	commit, _ := Resolve()
	return commit
}
