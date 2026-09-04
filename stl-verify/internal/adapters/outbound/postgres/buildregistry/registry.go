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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// BuildID is a typed integer for build registry IDs.
// Using a named type prevents accidental transposition with other int parameters
// (e.g. batchSize) at compile time.
type BuildID int

// RunID identifies one writer_run: the process start that wrote a governed row.
// Typed for the same reason as BuildID.
type RunID int64

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

// OpenRun records one process start in writer_run and returns its id. The row's
// reference_snapshot is pg_current_snapshot() of a REPEATABLE READ transaction in
// which load then reads the process's reference data, so "the reference rows this
// run saw" is exactly the rows visible in that snapshot with
// valid_from <= referenceEffectiveAt (ADR-0006 §2, §4). load receives the run id
// so it can construct the repositories that will carry it; a nil load records the
// run alone. If load fails nothing is committed: a run row for a process that
// never started would be provenance nobody could trust.
//
// A process that reloads its reference data must open a new run.
func (r *Registry) OpenRun(ctx context.Context, referenceEffectiveAt time.Time, load func(tx pgx.Tx, runID RunID) error) (RunID, error) {
	if referenceEffectiveAt.IsZero() {
		return 0, fmt.Errorf("opening writer run: referenceEffectiveAt must be set")
	}

	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		return 0, fmt.Errorf("opening writer run: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var runID RunID
	if err := tx.QueryRow(ctx, `
		INSERT INTO writer_run (build_id, reference_snapshot, reference_effective_at)
		VALUES ($1, pg_current_snapshot()::text, $2)
		RETURNING id`, int(r.buildID), referenceEffectiveAt.UTC()).Scan(&runID); err != nil {
		return 0, fmt.Errorf("opening writer run for build %d: %w", r.buildID, err)
	}

	if load != nil {
		if err := load(tx, runID); err != nil {
			return 0, fmt.Errorf("loading reference data for run %d: %w", runID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("committing writer run %d: %w", runID, err)
	}
	return runID, nil
}
