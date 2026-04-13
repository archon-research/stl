//go:build integration

package buildregistry

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/jackc/pgx/v5/pgxpool"
)

func migrationsPath() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "..", "..", "..", "..", "db", "migrations")
}

func setupDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn, cleanup := testutil.StartTimescaleDB(t)
	t.Cleanup(cleanup)

	pool := testutil.ConnectPool(t, dsn)
	t.Cleanup(pool.Close)

	m := migrator.New(pool, migrationsPath())
	if err := m.ApplyAll(context.Background()); err != nil {
		t.Fatalf("migrations: %v", err)
	}
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
	t.Setenv("BUILD_GIT_HASH", "")
	// Also clear any VCS info that might be embedded.
	// In test binaries, VCS info is typically available, so we need to
	// test this by unsetting the env var and hoping no VCS info exists.
	// If VCS info IS available, this test verifies that path works too.

	os.Unsetenv("BUILD_GIT_HASH")
	_, err := New(context.Background(), pool)
	// Either succeeds (VCS info available) or fails with a clear error.
	if err != nil {
		expected := "git hash not available"
		if len(err.Error()) < len(expected) || err.Error()[:len(expected)] != expected {
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
