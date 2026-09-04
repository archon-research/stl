package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PrimeCapitalStackRepository implements the port.
var _ outbound.PrimeCapitalStackRepository = (*PrimeCapitalStackRepository)(nil)

// PrimeCapitalStackRepository persists prime capital stack snapshots.
type PrimeCapitalStackRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
	runID  buildregistry.RunID
}

// NewPrimeCapitalStackRepository creates a new PrimeCapitalStackRepository.
func NewPrimeCapitalStackRepository(pool *pgxpool.Pool, logger *slog.Logger, runID buildregistry.RunID) *PrimeCapitalStackRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PrimeCapitalStackRepository{
		pool:   pool,
		logger: logger.With("component", "prime-capital-stack-repo"),
		runID:  runID,
	}
}

// SavePrimeCapitalSnapshots inserts a cycle's snapshots within the caller's
// transaction, so a failure here rolls back alongside whatever else the
// caller is writing at the same synced_at.
//
// Insert-only: a row is immutable once written, and the BEFORE INSERT trigger
// assigns processing_version, so a re-run under the same build_id reuses its
// version and conflicts away rather than overwriting history.
func (r *PrimeCapitalStackRepository) SavePrimeCapitalSnapshots(
	ctx context.Context,
	tx pgx.Tx,
	snapshots []entity.PrimeCapitalStackSnapshot,
) error {
	if len(snapshots) == 0 {
		return nil
	}

	const q = `
		INSERT INTO prime_capital_stack (
			prime_id,
			synced_at,
			exposure_usd,
			required_risk_capital_usd,
			total_risk_capital_usd,
			junior_risk_capital_usd,
			senior_risk_capital_usd,
			internal_junior_risk_capital_usd,
			external_junior_risk_capital_usd,
			tokenized_junior_risk_capital_usd,
			internal_senior_risk_capital_usd,
			external_senior_risk_capital_usd,
			encumbrance_ratio,
			exposure_share,
			epi_utilization,
			spj_utilization,
			source,
			build_id,
			run_id
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
		ON CONFLICT (prime_id, synced_at, processing_version) DO NOTHING
	`

	batch := &pgx.Batch{}
	for _, s := range snapshots {
		batch.Queue(
			q,
			s.PrimeID,
			s.SyncedAt,
			s.ExposureUSD,
			s.RequiredRiskCapitalUSD,
			s.TotalRiskCapitalUSD,
			s.JuniorRiskCapitalUSD,
			s.SeniorRiskCapitalUSD,
			s.InternalJuniorRiskCapitalUSD,
			s.ExternalJuniorRiskCapitalUSD,
			s.TokenizedJuniorRiskCapitalUSD,
			s.InternalSeniorRiskCapitalUSD,
			s.ExternalSeniorRiskCapitalUSD,
			s.EncumbranceRatio,
			s.ExposureShare,
			s.EPIUtilization,
			s.SPJUtilization,
			s.Source,
			s.BuildID,
			int64(r.runID),
		)
	}

	results := tx.SendBatch(ctx, batch)
	for i, s := range snapshots {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("insert capital stack snapshot %d (prime_id=%d): %w", i, s.PrimeID, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close batch: %w", err)
	}

	r.logger.Info("saved prime capital stack snapshots", "count", len(snapshots))
	return nil
}
