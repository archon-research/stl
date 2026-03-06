package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PrimeDebtRepository implements the port interface.
var _ outbound.PrimeDebtRepository = (*PrimeDebtRepository)(nil)

// PrimeDebtRepository persists prime debt snapshots to Postgres.
type PrimeDebtRepository struct {
	pool   *pgxpool.Pool
	txm    *TxManager
	logger *slog.Logger
}

// NewPrimeDebtRepository creates a new PrimeDebtRepository.
func NewPrimeDebtRepository(
	pool *pgxpool.Pool,
	txm *TxManager,
	logger *slog.Logger,
) *PrimeDebtRepository {
	if logger == nil {
		logger = slog.Default()
	}
	return &PrimeDebtRepository{
		pool:   pool,
		txm:    txm,
		logger: logger.With("component", "prime-debt-repo"),
	}
}

// GetPrimes returns all rows from the prime table.
// vault_address is stored as BYTEA and scanned directly into common.Address.
func (r *PrimeDebtRepository) GetPrimes(ctx context.Context) ([]entity.Prime, error) {
	const q = `
		SELECT id, name, vault_address, created_at
		FROM prime
		ORDER BY id ASC
	`

	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query primes: %w", err)
	}
	defer rows.Close()

	var primes []entity.Prime
	for rows.Next() {
		var p entity.Prime
		var addrBytes []byte
		if err := rows.Scan(&p.ID, &p.Name, &addrBytes, &p.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan prime row: %w", err)
		}
		p.VaultAddress = common.BytesToAddress(addrBytes)
		primes = append(primes, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate prime rows: %w", err)
	}

	return primes, nil
}

// SaveDebtSnapshots writes all debt snapshots in a single batched transaction.
// vault_address is written as raw BYTEA; debt_wad is stored as NUMERIC via big.Int.
func (r *PrimeDebtRepository) SaveDebtSnapshots(ctx context.Context, debts []*entity.PrimeDebt) error {
	if len(debts) == 0 {
		return nil
	}

	for i, d := range debts {
		if err := d.Validate(); err != nil {
			return fmt.Errorf("debt snapshot %d: %w", i, err)
		}
	}

	return r.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		const q = `
			INSERT INTO prime_debt (prime_id, ilk_name, debt_wad, block_number, synced_at)
			VALUES ($1, $2, $3, $4, $5)
		`

		batch := &pgx.Batch{}
		for _, d := range debts {
			batch.Queue(q,
				d.PrimeID,
				d.IlkName,
				d.DebtWad.String(), // NUMERIC from decimal string representation of big.Int
				d.BlockNumber,
				d.SyncedAt,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for i, d := range debts {
			if _, err := results.Exec(); err != nil {
				_ = results.Close()
				return fmt.Errorf("insert debt snapshot %d (prime_id=%d): %w", i, d.PrimeID, err)
			}
		}
		if err := results.Close(); err != nil {
			return fmt.Errorf("close batch: %w", err)
		}

		r.logger.Debug("debt snapshots saved", "count", len(debts))
		return nil
	})
}
