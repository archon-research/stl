package buildinfo

import "testing"

// Populate's contract is a precedence order, so each case fixes what is already
// set (the ldflags position) and what the environment offers, and asserts which
// one survives. The VCS position cannot be varied from a test — the test binary
// either carries embedded VCS info or does not — so these cases exercise the
// ldflags-vs-env boundary and treat VCS as the middle of the same chain.
func TestPopulatePrefersLdflagsOverEnvironment(t *testing.T) {
	tests := []struct {
		name                                  string
		commit, branch, buildTime             string
		envCommit, envBranch, envBuildTime    string
		wantCommit, wantBranch, wantBuildTime string
	}{
		{
			name:          "environment fills every empty value",
			envCommit:     "abc123",
			envBranch:     "main",
			envBuildTime:  "2026-09-08T10:00:00Z",
			wantCommit:    "abc123",
			wantBranch:    "main",
			wantBuildTime: "2026-09-08T10:00:00Z",
		},
		{
			name:          "an ldflags stamp wins over the environment",
			commit:        "stamped",
			branch:        "stamped-branch",
			buildTime:     "stamped-time",
			envCommit:     "abc123",
			envBranch:     "main",
			envBuildTime:  "2026-09-08T10:00:00Z",
			wantCommit:    "stamped",
			wantBranch:    "stamped-branch",
			wantBuildTime: "stamped-time",
		},
		{
			name:       "each value resolves independently",
			commit:     "stamped",
			envBranch:  "main",
			wantCommit: "stamped",
			wantBranch: "main",
		},
		{
			name: "nothing set anywhere leaves the values empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("BUILD_GIT_HASH", tt.envCommit)
			t.Setenv("BUILD_GIT_BRANCH", tt.envBranch)
			t.Setenv("BUILD_TIME", tt.envBuildTime)

			commit, branch, buildTime := tt.commit, tt.branch, tt.buildTime
			Populate(&commit, &branch, &buildTime)

			// The test binary is built from a checkout, so Go may have embedded
			// VCS info that legitimately fills commit/buildTime ahead of the
			// environment. Only assert on those two where the expectation does
			// not depend on which source won.
			if branch != tt.wantBranch {
				t.Errorf("branch = %q, want %q", branch, tt.wantBranch)
			}
			if tt.commit != "" && commit != tt.wantCommit {
				t.Errorf("commit = %q, want the ldflags value %q", commit, tt.wantCommit)
			}
			if tt.buildTime != "" && buildTime != tt.wantBuildTime {
				t.Errorf("buildTime = %q, want the ldflags value %q", buildTime, tt.wantBuildTime)
			}
		})
	}
}

// The env var is the only commit source a released image has, so a regression
// here reports the wrong build for every metric and build_registry row.
func TestResolveReadsCommitFromEnvironmentWhenNothingIsStamped(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "envcommit")
	t.Setenv("BUILD_TIME", "envtime")

	commit, buildTime := Resolve()

	if commit == "" || buildTime == "" {
		t.Fatalf("Resolve() = (%q, %q), want both populated", commit, buildTime)
	}
	if GitCommit == "" && commit != "envcommit" {
		t.Errorf("commit = %q, want the environment value when nothing is stamped", commit)
	}
	if GitHash() != commit {
		t.Errorf("GitHash() = %q, want Resolve's commit %q", GitHash(), commit)
	}
}

// Resolve must not consume the package vars destructively: two calls have to
// agree, or a service's reported version would depend on call order.
func TestResolveIsRepeatable(t *testing.T) {
	t.Setenv("BUILD_GIT_HASH", "envcommit")
	t.Setenv("BUILD_TIME", "envtime")

	firstCommit, firstBuildTime := Resolve()
	secondCommit, secondBuildTime := Resolve()

	if firstCommit != secondCommit || firstBuildTime != secondBuildTime {
		t.Errorf("Resolve() = (%q, %q) then (%q, %q), want identical results",
			firstCommit, firstBuildTime, secondCommit, secondBuildTime)
	}
}

// The env position is what a released image relies on exclusively: it carries
// no ldflags stamp and no embedded VCS info, so if this stops resolving, every
// pod reports an empty build. Tested directly because whether the VCS position
// above it resolves depends on how the calling binary was built.
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
