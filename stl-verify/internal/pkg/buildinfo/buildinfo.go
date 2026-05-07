package buildinfo

import "runtime/debug"

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
