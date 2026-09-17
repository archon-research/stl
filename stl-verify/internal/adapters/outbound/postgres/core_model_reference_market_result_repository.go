package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that CoreModelReferenceMarketResultRepository implements the port.
var _ outbound.CoreModelReferenceMarketResultRepository = (*CoreModelReferenceMarketResultRepository)(nil)

// CoreModelReferenceMarketResultRepository persists per-cycle CORE market results.
// It holds no pool: every write goes through the caller's transaction.
type CoreModelReferenceMarketResultRepository struct {
	logger *slog.Logger
	runID  buildregistry.RunID
}

// NewCoreModelReferenceMarketResultRepository creates a new CoreModelReferenceMarketResultRepository.
func NewCoreModelReferenceMarketResultRepository(
	logger *slog.Logger,
	runID buildregistry.RunID,
) *CoreModelReferenceMarketResultRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &CoreModelReferenceMarketResultRepository{
		logger: logger.With("component", "core-model-reference-market-result-repo"),
		runID:  runID,
	}
}

// SaveMarketResults inserts a cycle's market rows within the caller's
// transaction, so a failure here rolls back alongside the vault rows written
// at the same synced_at.
//
// Insert-only: a row is immutable once written. The BEFORE INSERT trigger
// assigns processing_version, so the same (identity, synced_at) written again
// under the same build_id conflicts away, and under a new build_id appends a
// correction. The returned count is what the database inserted, so a cycle
// whose rows all conflicted away reports zero rather than its batch size.
func (r *CoreModelReferenceMarketResultRepository) SaveMarketResults(
	ctx context.Context,
	tx pgx.Tx,
	results []entity.CoreModelReferenceMarketResult,
) (int, error) {
	if len(results) == 0 {
		return 0, nil
	}

	const q = `
		INSERT INTO core_model_reference_market_result (
			network,
			chain_id,
			protocol_name,
			market_uid,
			market_symbol,
			loan_token_symbol,
			loan_token_address,
			model_date,
			synced_at,
			n_scenarios,
			horizon_days,
			effective_horizon_days,
			total_supply_usd,
			prob_no_bad_debt,
			crr_el,
			crr_var,
			crr_es,
			crr_el_se,
			crr_var_se,
			crr_es_se,
			crr_floor,
			external_flow_enabled,
			source,
			build_id,
			run_id
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25)
		ON CONFLICT (network, protocol_name, market_uid, synced_at, processing_version) DO NOTHING
	`

	batch := &pgx.Batch{}
	for _, m := range results {
		batch.Queue(q, r.marketInsertArgs(m)...)
	}

	batchResults := tx.SendBatch(ctx, batch)
	inserted := 0
	for i, m := range results {
		tag, err := batchResults.Exec()
		if err != nil {
			_ = batchResults.Close()
			return 0, fmt.Errorf("insert core model reference market result %d (%s/%s/%s): %w",
				i, m.Network, m.ProtocolName, m.MarketUID, err)
		}
		inserted += int(tag.RowsAffected())
	}
	if err := batchResults.Close(); err != nil {
		return 0, fmt.Errorf("close batch: %w", err)
	}

	r.logger.Info("saved core model reference market results", "submitted", len(results), "inserted", inserted)
	return inserted, nil
}

// marketInsertArgs orders one row's values to match the INSERT column list.
func (r *CoreModelReferenceMarketResultRepository) marketInsertArgs(m entity.CoreModelReferenceMarketResult) []any {
	return []any{
		m.Network,
		m.ChainID,
		m.ProtocolName,
		m.MarketUID,
		m.MarketSymbol,
		m.LoanTokenSymbol,
		m.LoanTokenAddress,
		m.ModelDate,
		m.SyncedAt,
		m.NScenarios,
		m.HorizonDays,
		m.EffectiveHorizonDays,
		m.TotalSupplyUSD,
		m.ProbNoBadDebt,
		m.CRREL,
		m.CRRVaR,
		m.CRRES,
		m.CRRELSE,
		m.CRRVaRSE,
		m.CRRESSE,
		m.CRRFloor,
		m.ExternalFlowEnabled,
		m.Source,
		m.BuildID,
		r.runID,
	}
}
