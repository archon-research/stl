package testutil

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
)

// TestIdentity is the artefact a test registers as: the dev image digest, which no
// deployed environment can carry.
func TestIdentity(gitHash string) buildregistry.Identity {
	return buildregistry.Identity{GitHash: gitHash, Service: "test", ImageDigest: buildregistry.DevImageDigest}
}

// OpenTestRun registers the test as a dev-identity artefact and opens a writer run on
// pool, returning the ids repository constructors take. Reference data is loaded by
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

// SetDevIdentity lets a binary driven by this test resolve its artefact without an
// IMAGE_DIGEST, the way `make run-*` and the kind overlay do.
func SetDevIdentity(t *testing.T) {
	t.Helper()
	t.Setenv(buildregistry.DevIdentityEnv, "1")
	t.Setenv(buildregistry.ImageDigestEnv, "")
}
