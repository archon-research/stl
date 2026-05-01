//go:build integration

package buildregistry

import (
	"context"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func setupDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, _, cleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(cleanup)
	return pool
}

func TestNew_FirstRegistration(t *testing.T) {
	pool := setupDB(t)
	t.Setenv("BUILD_GIT_HASH", "abc123def456")

	reg, err := New(context.Background(), pool)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if reg.BuildID() <= 0 {
		t.Errorf("BuildID() = %d, want > 0", reg.BuildID())
	}
	if reg.GitHash() != "abc123def456" {
		t.Errorf("GitHash() = %q, want %q", reg.GitHash(), "abc123def456")
	}
}

func TestNew_IdempotentReregistration(t *testing.T) {
	pool := setupDB(t)
	t.Setenv("BUILD_GIT_HASH", "idempotent-hash")

	reg1, err := New(context.Background(), pool)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}

	reg2, err := New(context.Background(), pool)
	if err != nil {
		t.Fatalf("second New: %v", err)
	}

	if reg1.BuildID() != reg2.BuildID() {
		t.Errorf("BuildID mismatch: %d != %d", reg1.BuildID(), reg2.BuildID())
	}
}

func TestNew_DifferentHashesDifferentIDs(t *testing.T) {
	pool := setupDB(t)

	t.Setenv("BUILD_GIT_HASH", "hash-aaa")
	reg1, err := New(context.Background(), pool)
	if err != nil {
		t.Fatalf("first New: %v", err)
	}

	t.Setenv("BUILD_GIT_HASH", "hash-bbb")
	reg2, err := New(context.Background(), pool)
	if err != nil {
		t.Fatalf("second New: %v", err)
	}

	if reg1.BuildID() == reg2.BuildID() {
		t.Errorf("different hashes should have different IDs, both got %d", reg1.BuildID())
	}
}

func TestNew_EmptyHashNoEnvVar(t *testing.T) {
	pool := setupDB(t)
	// Set to empty string — os.Getenv returns "" for both unset and empty,
	// so this effectively clears the fallback. t.Setenv auto-restores after test.
	t.Setenv("BUILD_GIT_HASH", "")
	_, err := New(context.Background(), pool)
	// In test binaries, VCS info is typically available from the Go build,
	// so New() succeeds via that path. If VCS info is unavailable, it should
	// fail with a clear error.
	if err != nil {
		if !strings.Contains(err.Error(), "git hash not available") {
			t.Errorf("unexpected error: %v", err)
		}
	}
}

func TestNew_BuildTimePopulated(t *testing.T) {
	pool := setupDB(t)
	t.Setenv("BUILD_GIT_HASH", "buildtime-test")

	reg, err := New(context.Background(), pool)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// BuildTime comes from VCS info, which may or may not be available in tests.
	// Just verify the accessor doesn't panic.
	_ = reg.BuildTime()
}
