// Package buildregistry names the code that writes governed rows (ADR-0006 §2): the
// artefact a process runs as (build_registry) and each process start of it
// (writer_run). Every binary that connects to Postgres registers its artefact and
// opens a run at startup; a binary with no database connection cannot write
// governed rows and opens none.
package buildregistry

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BuildID is a typed integer for build registry IDs.
// Using a named type prevents accidental transposition with other int parameters
// (e.g. batchSize) at compile time.
type BuildID int

// Registry is the registered artefact of the running process. Created once at
// startup; its BuildID and the RunID it opens are passed into the repositories
// that write governed rows.
type Registry struct {
	db       *pgxpool.Pool
	buildID  BuildID
	identity Identity
}

// New registers the running process's artefact (ResolveIdentity) in build_registry
// and returns its build_id. Re-registering the same artefact (pod restart, several
// replicas) resolves to the existing row.
func New(ctx context.Context, db *pgxpool.Pool) (*Registry, error) {
	identity, err := ResolveIdentity()
	if err != nil {
		return nil, err
	}
	return NewWithIdentity(ctx, db, identity)
}

// NewWithIdentity is New for an identity the caller resolved itself — the test
// suite, where the process is the test binary.
func NewWithIdentity(ctx context.Context, db *pgxpool.Pool, identity Identity) (*Registry, error) {
	if identity.GitHash == "" || identity.Service == "" || identity.ImageDigest == "" {
		return nil, fmt.Errorf("incomplete artefact identity %+v: git hash, service and image digest are all required", identity)
	}

	var id int
	err := db.QueryRow(ctx, `
		INSERT INTO build_registry (git_hash, service, image_digest) VALUES ($1, $2, $3)
		ON CONFLICT (git_hash, service, image_digest) DO NOTHING
		RETURNING id`, identity.GitHash, identity.Service, identity.ImageDigest).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		err = db.QueryRow(ctx, `
			SELECT id FROM build_registry WHERE git_hash = $1 AND service = $2 AND image_digest = $3`,
			identity.GitHash, identity.Service, identity.ImageDigest).Scan(&id)
	}
	if err != nil {
		return nil, fmt.Errorf("resolving build_id for %s/%s@%s: %w", identity.Service, identity.GitHash, identity.ImageDigest, err)
	}

	return &Registry{db: db, buildID: BuildID(id), identity: identity}, nil
}

// BuildID returns the resolved build_id for this process's artefact.
func (r *Registry) BuildID() BuildID { return r.buildID }

// GitHash returns the git commit hash that was registered.
func (r *Registry) GitHash() string { return r.identity.GitHash }

// Service returns the service name that was registered.
func (r *Registry) Service() string { return r.identity.Service }

// ImageDigest returns the image digest that was registered.
func (r *Registry) ImageDigest() string { return r.identity.ImageDigest }

// BuildTime returns the build timestamp.
func (r *Registry) BuildTime() string { return r.identity.BuildTime }
