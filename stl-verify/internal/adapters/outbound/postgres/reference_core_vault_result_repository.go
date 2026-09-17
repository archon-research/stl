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

// Compile-time check that ReferenceCoreVaultResultRepository implements the port.
var _ outbound.ReferenceCoreVaultResultRepository = (*ReferenceCoreVaultResultRepository)(nil)

// ReferenceCoreVaultResultRepository persists per-cycle CORE vault results.
// It holds no pool: every write goes through the caller's transaction.
type ReferenceCoreVaultResultRepository struct {
	logger *slog.Logger
	runID  buildregistry.RunID
}

// NewReferenceCoreVaultResultRepository creates a new ReferenceCoreVaultResultRepository.
func NewReferenceCoreVaultResultRepository(
	logger *slog.Logger,
	runID buildregistry.RunID,
) *ReferenceCoreVaultResultRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &ReferenceCoreVaultResultRepository{
		logger: logger.With("component", "reference-core-vault-result-repo"),
		runID:  runID,
	}
}

// SaveVaultResults inserts a cycle's vault rows within the caller's
// transaction; insert-only under the same rules as SaveMarketResults.
func (r *ReferenceCoreVaultResultRepository) SaveVaultResults(
	ctx context.Context,
	tx pgx.Tx,
	results []entity.ReferenceCoreVaultResult,
) error {
	if len(results) == 0 {
		return nil
	}

	const q = `
		INSERT INTO reference_core_vault_result (
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
	for i, v := range results {
		if _, err := batchResults.Exec(); err != nil {
			_ = batchResults.Close()
			return fmt.Errorf("insert reference core vault result %d (%s/%s/%s): %w",
				i, v.Network, v.ProtocolName, v.VaultAddress, err)
		}
	}
	if err := batchResults.Close(); err != nil {
		return fmt.Errorf("close batch: %w", err)
	}

	r.logger.Info("saved reference core vault results", "count", len(results))
	return nil
}

// vaultInsertArgs orders one row's values to match the INSERT column list.
func (r *ReferenceCoreVaultResultRepository) vaultInsertArgs(v entity.ReferenceCoreVaultResult) []any {
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
