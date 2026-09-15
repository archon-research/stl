package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PositionDailyCrystallizerRepository calls crystallize_position_daily() (VEC-636).
//
// The procedure pins its own search_path, work_mem, lock_timeout and tiered reads, so
// this adapter passes the settling window and nothing else: the settings that make the
// pass correct belong with the statement, not with each caller.
type PositionDailyCrystallizerRepository struct {
	pool *pgxpool.Pool
}

// NewPositionDailyCrystallizerRepository creates the repository.
func NewPositionDailyCrystallizerRepository(pool *pgxpool.Pool) *PositionDailyCrystallizerRepository {
	return &PositionDailyCrystallizerRepository{pool: pool}
}

// Crystallize runs one pass and returns the rows it wrote.
func (r *PositionDailyCrystallizerRepository) Crystallize(ctx context.Context, settleAfter time.Duration) (int64, error) {
	// The INOUT parameter comes back as the CALL's single result column. A NULL would
	// mean the procedure returned without reaching GET DIAGNOSTICS, which it cannot do
	// without raising, so scanning into a non-pointer int64 is the assertion.
	var appended int64
	if err := r.pool.QueryRow(ctx,
		`CALL crystallize_position_daily($1::interval, NULL)`, settleAfter).Scan(&appended); err != nil {
		return 0, fmt.Errorf("crystallizing position_daily: %w", err)
	}
	return appended, nil
}
