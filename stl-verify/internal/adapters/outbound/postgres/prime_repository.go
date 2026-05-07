package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

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
