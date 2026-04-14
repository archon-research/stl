package buildregistry

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/buildinfo"
)

// BuildID is a typed integer for build registry IDs.
// Using a named type prevents accidental transposition with other int parameters
// (e.g. batchSize) at compile time.
type BuildID int

// Registry resolves and caches the build_id for the running service's git commit.
// Created once at startup; the resolved ID is passed into repositories that write
// to state tables.
type Registry struct {
	buildID   BuildID
	gitHash   string
	buildTime string
}

// New registers the current build's git hash in build_registry and returns the
// resolved build_id. The git hash is read from buildinfo package-level vars
// (set via ldflags), with VCS info and BUILD_GIT_HASH env var as fallbacks.
// If the hash is already registered (pod restart, multiple replicas), it looks
// up the existing ID.
func New(ctx context.Context, db *pgxpool.Pool) (*Registry, error) {
	gitHash := buildinfo.GitCommit
	buildTime := buildinfo.BuildTime

	// Fallback: Go's embedded VCS info (works when .git is present)
	if gitHash == "" || buildTime == "" {
		buildinfo.PopulateFromVCS(&gitHash, &buildTime)
	}

	// Fallback: BUILD_GIT_HASH env var
	if gitHash == "" {
		gitHash = os.Getenv("BUILD_GIT_HASH")
	}

	if gitHash == "" {
		return nil, fmt.Errorf("git hash not available: build with VCS info or set BUILD_GIT_HASH env var")
	}

	var id int
	err := db.QueryRow(ctx, `
		INSERT INTO build_registry (git_hash) VALUES ($1)
		ON CONFLICT (git_hash) DO NOTHING
		RETURNING id`, gitHash).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Already registered — look up the existing id.
			err = db.QueryRow(ctx, `
				SELECT id FROM build_registry WHERE git_hash = $1`,
				gitHash).Scan(&id)
		}
		if err != nil {
			return nil, fmt.Errorf("resolving build_id for %s: %w", gitHash, err)
		}
	}

	return &Registry{buildID: BuildID(id), gitHash: gitHash, buildTime: buildTime}, nil
}

// BuildID returns the resolved build_id for this service's git commit.
func (r *Registry) BuildID() BuildID { return r.buildID }

// GitHash returns the git commit hash that was registered.
func (r *Registry) GitHash() string { return r.gitHash }

// BuildTime returns the build timestamp.
func (r *Registry) BuildTime() string { return r.buildTime }
