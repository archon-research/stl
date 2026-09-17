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

// Compile-time check that CoreModelReferenceVaultResultRepository implements the port.
var _ outbound.CoreModelReferenceVaultResultRepository = (*CoreModelReferenceVaultResultRepository)(nil)

// CoreModelReferenceVaultResultRepository persists per-cycle CORE vault results.
// It holds no pool: every write goes through the caller's transaction.
type CoreModelReferenceVaultResultRepository struct {
	logger *slog.Logger
	runID  buildregistry.RunID
}

// NewCoreModelReferenceVaultResultRepository creates a new CoreModelReferenceVaultResultRepository.
func NewCoreModelReferenceVaultResultRepository(
	logger *slog.Logger,
	runID buildregistry.RunID,
) *CoreModelReferenceVaultResultRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &CoreModelReferenceVaultResultRepository{
		logger: logger.With("component", "core-model-reference-vault-result-repo"),
		runID:  runID,
	}
}

// SaveVaultResults inserts a cycle's vault rows within the caller's
// transaction; insert-only under the same rules as SaveMarketResults.
func (r *CoreModelReferenceVaultResultRepository) SaveVaultResults(
	ctx context.Context,
	tx pgx.Tx,
	results []entity.CoreModelReferenceVaultResult,
) (int, error) {
	if len(results) == 0 {
		return 0, nil
	}

	const q = `
		INSERT INTO core_model_reference_vault_result (
			network,
			chain_id,
			protocol_name,
			vault_address,
			vault_symbol,
			vault_name,
			version_label,
			loan_token_symbol,
			loan_token_address,
			method,
			model_date,
			synced_at,
			n_markets,
			total_assets_usd,
			idle_assets_usd,
			crr_el,
			crr_el_se,
			crr_es,
			source,
			build_id,
			run_id
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
		ON CONFLICT (network, protocol_name, vault_address, synced_at, processing_version) DO NOTHING
	`

	batch := &pgx.Batch{}
	for _, v := range results {
		batch.Queue(q, r.vaultInsertArgs(v)...)
	}

	batchResults := tx.SendBatch(ctx, batch)
	inserted := 0
	for i, v := range results {
		tag, err := batchResults.Exec()
		if err != nil {
			_ = batchResults.Close()
			return 0, fmt.Errorf("insert core model reference vault result %d (%s/%s/%s): %w",
				i, v.Network, v.ProtocolName, v.VaultAddress, err)
		}
		inserted += int(tag.RowsAffected())
	}
	if err := batchResults.Close(); err != nil {
		return 0, fmt.Errorf("close batch: %w", err)
	}

	r.logger.Info("saved core model reference vault results", "submitted", len(results), "inserted", inserted)
	return inserted, nil
}

// vaultInsertArgs orders one row's values to match the INSERT column list.
func (r *CoreModelReferenceVaultResultRepository) vaultInsertArgs(v entity.CoreModelReferenceVaultResult) []any {
	return []any{
		v.Network,
		v.ChainID,
		v.ProtocolName,
		v.VaultAddress,
		v.VaultSymbol,
		v.VaultName,
		v.VersionLabel,
		v.LoanTokenSymbol,
		v.LoanTokenAddress,
		v.Method,
		v.ModelDate,
		v.SyncedAt,
		v.NMarkets,
		v.TotalAssetsUSD,
		v.IdleAssetsUSD,
		v.CRREL,
		v.CRRELSE,
		v.CRRES,
		v.Source,
		v.BuildID,
		r.runID,
	}
}
