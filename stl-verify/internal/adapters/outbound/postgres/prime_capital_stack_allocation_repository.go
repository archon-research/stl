package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PrimeCapitalStackAllocationRepository implements the port.
var _ outbound.PrimeCapitalStackAllocationRepository = (*PrimeCapitalStackAllocationRepository)(nil)

// PrimeCapitalStackAllocationRepository persists per-allocation breakdown rows.
type PrimeCapitalStackAllocationRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

// NewPrimeCapitalStackAllocationRepository creates a new PrimeCapitalStackAllocationRepository.
func NewPrimeCapitalStackAllocationRepository(
	pool *pgxpool.Pool,
	txm *TxManager,
	logger *slog.Logger,
) *PrimeCapitalStackAllocationRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PrimeCapitalStackAllocationRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "prime-capital-stack-allocation-repo"),
	}
}

// SaveCapitalStackAllocations inserts a cycle's breakdown rows in one transaction.
//
// Insert-only: a row is immutable once written, and the BEFORE INSERT trigger
// assigns processing_version, so a re-run under the same build_id reuses its
// version and conflicts away rather than overwriting history.
func (r *PrimeCapitalStackAllocationRepository) SaveCapitalStackAllocations(
	ctx context.Context,
	allocations []entity.PrimeCapitalStackAllocation,
) error {
	if len(allocations) == 0 {
		return nil
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO prime_capital_stack_allocation (
				prime_id,
				synced_at,
				network,
				chain_id,
				protocol_name,
				symbol,
				name,
				token_address,
				loan_token_address,
				loan_token_symbol,
				exposure_usd,
				required_risk_capital_usd,
				crr,
				source,
				build_id
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			ON CONFLICT (prime_id, synced_at, network, token_address, processing_version) DO NOTHING
		`

		batch := &pgx.Batch{}
		for _, a := range allocations {
			batch.Queue(
				q,
				a.PrimeID,
				a.SyncedAt,
				a.Network,
				a.ChainID,
				a.ProtocolName,
				a.Symbol,
				a.Name,
				a.TokenAddress,
				a.LoanTokenAddress,
				a.LoanTokenSymbol,
				a.ExposureUSD,
				a.RequiredRiskCapitalUSD,
				a.CRR,
				a.Source,
				a.BuildID,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, a := range allocations {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert capital stack allocation %d (prime_id=%d, token=%s): %w",
					i, a.PrimeID, a.TokenAddress, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Info("saved prime capital stack allocations", "count", len(allocations))
		return nil
	})
}
