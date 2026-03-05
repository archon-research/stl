package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PrimeDebtRepository defines persistence operations for prime agent debt tracking.
type PrimeDebtRepository interface {
	// GetPrimes returns all registered prime agent vaults from the database.
	// The service calls this at startup to resolve which vaults to track.
	GetPrimes(ctx context.Context) ([]entity.Prime, error)

	// SaveDebtSnapshots persists a batch of on-chain debt readings in a single transaction.
	SaveDebtSnapshots(ctx context.Context, debts []*entity.PrimeDebt) error
}
