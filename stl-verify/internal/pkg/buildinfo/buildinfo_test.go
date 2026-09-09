package buildinfo

import (
	"runtime/debug"
	"testing"
)

// stubBuildInfo swaps the package's readBuildInfo seam for a fixed result,
// restoring it after the test. That lets a case simulate either a binary
// with no embedded VCS info (ok=false) or one with a known revision/time,
// deterministically, regardless of how the test binary itself was actually
// built (plain `go test` embeds nothing; `go test -buildvcs=true` does).
func stubBuildInfo(t *testing.T, info *debug.BuildInfo, ok bool) {
	t.Helper()
	prev := readBuildInfo
	readBuildInfo = func() (*debug.BuildInfo, bool) { return info, ok }
	t.Cleanup(func() { readBuildInfo = prev })
}

func vcsBuildInfo(revision, buildTime string) *debug.BuildInfo {
	return &debug.BuildInfo{
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: revision},
			{Key: "vcs.time", Value: buildTime},
		},
	}
}

// Populate's three sources — VCS, environment, and whatever the caller
// already set — are exercised through the readBuildInfo seam, so each case
// is deterministic under both plain `go test` and `go test -buildvcs=true`.
func TestPopulate(t *testing.T) {
	tests := []struct {
		name                                  string
		vcsPresent                            bool
		vcsRevision, vcsBuildTime             string
		commit, branch, buildTime             string // the caller's starting point
		envCommit, envBranch, envBuildTime    string
		wantCommit, wantBranch, wantBuildTime string
	}{
		{
			name:          "no VCS info, environment supplies all three: the released-image path",
			envCommit:     "abc123",
			envBranch:     "main",
			envBuildTime:  "2026-09-08T10:00:00Z",
			wantCommit:    "abc123",
			wantBranch:    "main",
			wantBuildTime: "2026-09-08T10:00:00Z",
		},
		{
			name:          "VCS info present and environment also set: VCS wins commit/buildTime, env still supplies branch",
			vcsPresent:    true,
			vcsRevision:   "vcsrevision",
			vcsBuildTime:  "vcstime",
			envCommit:     "abc123",
			envBranch:     "main",
			envBuildTime:  "2026-09-08T10:00:00Z",
			wantCommit:    "vcsrevision",
			wantBranch:    "main",
			wantBuildTime: "vcstime",
		},
		{
			name: "nothing set anywhere leaves all three empty",
		},
		{
			name:          "a value set before the call is never overwritten by VCS or environment",
			vcsPresent:    true,
			vcsRevision:   "vcsrevision",
			vcsBuildTime:  "vcstime",
			commit:        "preset-commit",
			branch:        "preset-branch",
			buildTime:     "preset-time",
			envCommit:     "abc123",
			envBranch:     "main",
			envBuildTime:  "2026-09-08T10:00:00Z",
			wantCommit:    "preset-commit",
			wantBranch:    "preset-branch",
			wantBuildTime: "preset-time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.vcsPresent {
				stubBuildInfo(t, vcsBuildInfo(tt.vcsRevision, tt.vcsBuildTime), true)
			} else {
				stubBuildInfo(t, nil, false)
			}
			t.Setenv("BUILD_GIT_HASH", tt.envCommit)
			t.Setenv("BUILD_GIT_BRANCH", tt.envBranch)
			t.Setenv("BUILD_TIME", tt.envBuildTime)

			commit, branch, buildTime := tt.commit, tt.branch, tt.buildTime
			Populate(&commit, &branch, &buildTime)

			if commit != tt.wantCommit {
				t.Errorf("commit = %q, want %q", commit, tt.wantCommit)
			}
			if branch != tt.wantBranch {
				t.Errorf("branch = %q, want %q", branch, tt.wantBranch)
			}
			if buildTime != tt.wantBuildTime {
				t.Errorf("buildTime = %q, want %q", buildTime, tt.wantBuildTime)
			}
		})
	}
}

// Resolve and GitHash only wire Populate to fresh local variables. Stubbing
// no VCS info isolates the environment path so the exact value can be
// asserted, instead of merely checking the result is non-empty.
func TestResolveAndGitHash(t *testing.T) {
	stubBuildInfo(t, nil, false)
	t.Setenv("BUILD_GIT_HASH", "envcommit")
	t.Setenv("BUILD_GIT_BRANCH", "")
	t.Setenv("BUILD_TIME", "envtime")

	commit, buildTime := Resolve()

	if commit != "envcommit" || buildTime != "envtime" {
		t.Fatalf("Resolve() = (%q, %q), want (%q, %q)", commit, buildTime, "envcommit", "envtime")
	}
	if GitHash() != commit {
		t.Errorf("GitHash() = %q, want Resolve's commit %q", GitHash(), commit)
	}
}

// Resolve must not consume anything destructively: two calls have to agree,
// or a service's reported version would depend on call order.
func TestResolveIsRepeatable(t *testing.T) {
	stubBuildInfo(t, nil, false)
	t.Setenv("BUILD_GIT_HASH", "envcommit")
	t.Setenv("BUILD_TIME", "envtime")

	firstCommit, firstBuildTime := Resolve()
	secondCommit, secondBuildTime := Resolve()

	if firstCommit != secondCommit || firstBuildTime != secondBuildTime {
		t.Errorf("Resolve() = (%q, %q) then (%q, %q), want identical results",
			firstCommit, firstBuildTime, secondCommit, secondBuildTime)
	}
}

// The env position is what a released image relies on exclusively: it
// carries no embedded VCS info. Tested directly (bypassing Populate) so this
// keeps covering populateFromEnv in isolation, in addition to TestPopulate
// exercising it through the real entry point every cmd/*/main.go calls.
func TestPopulateFromEnv(t *testing.T) {
	tests := []struct {
		name                                  string
		commit, branch, buildTime             string
		envCommit, envBranch, envBuildTime    string
		wantCommit, wantBranch, wantBuildTime string
	}{
		{
			name:          "an image's three env vars supply all three values",
			envCommit:     "abc123",
			envBranch:     "main",
			envBuildTime:  "2026-09-08T10:00:00Z",
			wantCommit:    "abc123",
			wantBranch:    "main",
			wantBuildTime: "2026-09-08T10:00:00Z",
		},
		{
			name:          "an already-resolved value is never overwritten",
			commit:        "fromVCS",
			buildTime:     "fromVCS",
			envCommit:     "abc123",
			envBranch:     "main",
			envBuildTime:  "2026-09-08T10:00:00Z",
			wantCommit:    "fromVCS",
			wantBranch:    "main",
			wantBuildTime: "fromVCS",
		},
		{
			name:       "an unset env var leaves its value empty rather than blanking a peer",
			envCommit:  "abc123",
			wantCommit: "abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BUILD_GIT_HASH", tt.envCommit)
			t.Setenv("BUILD_GIT_BRANCH", tt.envBranch)
			t.Setenv("BUILD_TIME", tt.envBuildTime)

			commit, branch, buildTime := tt.commit, tt.branch, tt.buildTime
			populateFromEnv(&commit, &branch, &buildTime)

			if commit != tt.wantCommit {
				t.Errorf("commit = %q, want %q", commit, tt.wantCommit)
			}
			if branch != tt.wantBranch {
				t.Errorf("branch = %q, want %q", branch, tt.wantBranch)
			}
			if buildTime != tt.wantBuildTime {
				t.Errorf("buildTime = %q, want %q", buildTime, tt.wantBuildTime)
			}
		})
	}
}
