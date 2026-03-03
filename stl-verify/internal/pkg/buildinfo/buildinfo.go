package buildinfo

import "runtime/debug"

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
