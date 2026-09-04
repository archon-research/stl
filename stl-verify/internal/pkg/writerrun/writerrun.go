// Package writerrun wires the per-process registration every Postgres-connected
// binary performs at startup: register the build artefact, then open the writer
// run whose id stamps every governed row the process writes.
package writerrun

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// Open registers the running binary and opens its writer run, pinned to the
// reference instant resolved from the environment. Binaries that load reference
// data inside the run use buildregistry.Registry.OpenRun directly instead.
func Open(ctx context.Context, pool *pgxpool.Pool) (*buildregistry.Registry, buildregistry.RunID, error) {
	reg, err := buildregistry.New(ctx, pool)
	if err != nil {
		return nil, 0, fmt.Errorf("registering build: %w", err)
	}
	referenceEffectiveAt, err := env.ReferenceEffectiveAt(time.Now().UTC())
	if err != nil {
		return nil, 0, fmt.Errorf("resolving reference effective time: %w", err)
	}
	runID, err := reg.OpenRun(ctx, referenceEffectiveAt, nil)
	if err != nil {
		return nil, 0, err
	}
	return reg, runID, nil
}
