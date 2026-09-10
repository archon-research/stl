package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check.
var _ outbound.PrimeRepository = (*PrimeRepository)(nil)

// PrimeRepository provides prime agent lookups from Postgres.
type PrimeRepository struct {
	pool *pgxpool.Pool
}

// NewPrimeRepository creates a new PrimeRepository.
func NewPrimeRepository(pool *pgxpool.Pool) *PrimeRepository {
	return &PrimeRepository{pool: pool}
}

// GetPrimeIDByName returns the prime ID for the given name.
func (r *PrimeRepository) GetPrimeIDByName(ctx context.Context, name string) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx, "SELECT id FROM prime WHERE name = $1", name).Scan(&id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("prime %q not found", name)
		}
		return 0, fmt.Errorf("get prime by name: %w", err)
	}
	return id, nil
}

// ListPrimes returns all prime rows ordered by id.
func (r *PrimeRepository) ListPrimes(ctx context.Context) ([]entity.Prime, error) {
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
