package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
)

// TestIdentity is the artefact a test registers as: the "test" service, which no
// deployed environment builds.
func TestIdentity(gitHash string) buildregistry.Identity {
	return buildregistry.Identity{GitHash: gitHash, Service: "test"}
}

// OpenTestRun registers the test's artefact and opens a writer run on pool,
// returning the ids repository constructors take. Reference data is loaded by
// nothing, so the run pins the instant it was opened.
func OpenTestRun(t *testing.T, ctx context.Context, pool *pgxpool.Pool) (buildregistry.BuildID, buildregistry.RunID) {
	t.Helper()
	reg, err := buildregistry.NewWithIdentity(ctx, pool, TestIdentity("test"))
	if err != nil {
		t.Fatalf("register test build: %v", err)
	}
	runID, err := reg.OpenRun(ctx, time.Now().UTC(), nil)
	if err != nil {
		t.Fatalf("open test writer run: %v", err)
	}
	return reg.BuildID(), runID
}

// SetBuildGitHash lets a binary driven by this test resolve its artefact the way
// `make run-*` does: a stand-in git hash, because a `go run` or `go test` build embeds
// no VCS info and buildregistry refuses to register without one.
func SetBuildGitHash(t *testing.T) {
	t.Helper()
	t.Setenv("BUILD_GIT_HASH", "test")
}

// RequireRunID fails the test unless a row's run_id (scanned as *int64, the column is
// nullable) names want.
func RequireRunID(t *testing.T, got *int64, want buildregistry.RunID) {
	t.Helper()
	if got == nil {
		t.Fatalf("run_id = NULL, want %d", want)
	}
	if *got != int64(want) {
		t.Fatalf("run_id = %d, want %d", *got, want)
	}
}
