//go:build integration

package buildregistry_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const testDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func setupDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	return pool
}

func TestNew_RegistersTheProcessIdentity(t *testing.T) {
	pool := setupDB(t)
	t.Setenv("BUILD_GIT_HASH", "abc123def456")
	t.Setenv(buildregistry.ImageDigestEnv, testDigest)

	reg, err := buildregistry.New(context.Background(), pool)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if reg.BuildID() <= 0 {
		t.Errorf("BuildID() = %d, want > 0", reg.BuildID())
	}
	if reg.GitHash() != "abc123def456" {
		t.Errorf("GitHash() = %q, want %q", reg.GitHash(), "abc123def456")
	}
	if reg.Service() == "" {
		t.Error("Service() is empty, want the test binary's name")
	}
	if reg.ImageDigest() != testDigest {
		t.Errorf("ImageDigest() = %q, want %q", reg.ImageDigest(), testDigest)
	}

	var service, digest string
	if err := pool.QueryRow(context.Background(),
		`SELECT service, image_digest FROM build_registry WHERE id = $1`, int(reg.BuildID())).Scan(&service, &digest); err != nil {
		t.Fatalf("read registered row: %v", err)
	}
	if service != reg.Service() || digest != testDigest {
		t.Errorf("registered (%q, %q), want (%q, %q)", service, digest, reg.Service(), testDigest)
	}
}

func TestNew_DevIdentityRegistersTheDevDigest(t *testing.T) {
	pool := setupDB(t)
	t.Setenv("BUILD_GIT_HASH", "dev")
	testutil.SetDevIdentity(t)

	reg, err := buildregistry.New(context.Background(), pool)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if reg.ImageDigest() != buildregistry.DevImageDigest {
		t.Errorf("ImageDigest() = %q, want %q", reg.ImageDigest(), buildregistry.DevImageDigest)
	}
}

func TestNew_MissingDigestIsAHardError(t *testing.T) {
	pool := setupDB(t)
	t.Setenv("BUILD_GIT_HASH", "abc123def456")
	t.Setenv(buildregistry.ImageDigestEnv, "")
	t.Setenv(buildregistry.DevIdentityEnv, "")

	_, err := buildregistry.New(context.Background(), pool)
	if err == nil || !strings.Contains(err.Error(), buildregistry.ImageDigestEnv) {
		t.Fatalf("New() error = %v, want one naming %s", err, buildregistry.ImageDigestEnv)
	}
}

func TestNew_IdempotentReregistration(t *testing.T) {
	pool := setupDB(t)
	t.Setenv("BUILD_GIT_HASH", "idempotent-hash")
	t.Setenv(buildregistry.ImageDigestEnv, testDigest)

	reg1, err := buildregistry.New(context.Background(), pool)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}
	reg2, err := buildregistry.New(context.Background(), pool)
	if err != nil {
		t.Fatalf("second New: %v", err)
	}
	if reg1.BuildID() != reg2.BuildID() {
		t.Errorf("BuildID mismatch: %d != %d", reg1.BuildID(), reg2.BuildID())
	}
}

func TestNew_DistinctArtefactsGetDistinctIDs(t *testing.T) {
	ctx := context.Background()
	base := buildregistry.Identity{GitHash: "hash-aaa", Service: "svc", ImageDigest: testDigest}
	tests := []struct {
		name  string
		other buildregistry.Identity
	}{
		{"different git hash", buildregistry.Identity{GitHash: "hash-bbb", Service: "svc", ImageDigest: testDigest}},
		{"different service", buildregistry.Identity{GitHash: "hash-aaa", Service: "other-svc", ImageDigest: testDigest}},
		{"different image digest", buildregistry.Identity{GitHash: "hash-aaa", Service: "svc", ImageDigest: buildregistry.DevImageDigest}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := setupDB(t)
			reg1, err := buildregistry.NewWithIdentity(ctx, pool, base)
			if err != nil {
				t.Fatalf("first NewWithIdentity: %v", err)
			}
			reg2, err := buildregistry.NewWithIdentity(ctx, pool, tt.other)
			if err != nil {
				t.Fatalf("second NewWithIdentity: %v", err)
			}
			if reg1.BuildID() == reg2.BuildID() {
				t.Errorf("distinct artefacts share build_id %d", reg1.BuildID())
			}
		})
	}
}

func TestNewWithIdentity_RejectsAnIncompleteIdentity(t *testing.T) {
	pool := setupDB(t)
	_, err := buildregistry.NewWithIdentity(context.Background(), pool, buildregistry.Identity{GitHash: "abc", Service: "svc"})
	if err == nil || !strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("NewWithIdentity() error = %v, want an incomplete-identity error", err)
	}
}
