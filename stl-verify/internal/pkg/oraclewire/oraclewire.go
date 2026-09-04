// Package oraclewire wires an oracle-pricing binary's writer run: the run is recorded
// and the oracle units are loaded in one transaction, so the run's reference snapshot
// is exactly the reference view the units were built from (ADR-0006 §2).
package oraclewire

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/services/oracle_pricing"
)

// OpenRun opens the process's writer run and returns the repository carrying its run id
// together with the oracle units loaded inside the run's transaction. The repository
// itself runs on pool; only the load was transaction-scoped.
func OpenRun(
	ctx context.Context,
	reg *buildregistry.Registry,
	pool *pgxpool.Pool,
	chainID int64,
	referenceEffectiveAt time.Time,
	batchSize int,
	logger *slog.Logger,
) (*postgres.OnchainPriceRepository, []*oracle_pricing.OracleUnit, error) {
	var repo *postgres.OnchainPriceRepository
	var units []*oracle_pricing.OracleUnit
	_, err := reg.OpenRun(ctx, referenceEffectiveAt, func(tx pgx.Tx, runID buildregistry.RunID) error {
		var err error
		repo, err = postgres.NewOnchainPriceRepository(pool, logger, reg.BuildID(), runID, batchSize)
		if err != nil {
			return fmt.Errorf("creating repository: %w", err)
		}
		units, err = oracle_pricing.LoadOracleUnits(ctx, repo.WithTx(tx), chainID, referenceEffectiveAt, logger)
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	return repo, units, nil
}
