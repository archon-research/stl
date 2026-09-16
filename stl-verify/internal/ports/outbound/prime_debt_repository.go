package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PrimeDebtRepository defines persistence operations for prime agent debt tracking.
type PrimeDebtRepository interface {
	// GetPrimes returns all registered prime agent vaults from the database, whatever chain each vault
	// is deployed on. A prime's positions reach other chains, so a consumer of those reads them all.
	GetPrimes(ctx context.Context) ([]entity.Prime, error)

	// GetPrimesOnChain returns the vaults deployed on one chain. Debt is read from the Vat on the
	// vault's own chain, so a tracker configured for a chain must not read a vault deployed elsewhere.
	GetPrimesOnChain(ctx context.Context, chainID int64) ([]entity.Prime, error)

	// SaveDebtSnapshots persists a batch of on-chain debt readings in a single transaction.
	SaveDebtSnapshots(ctx context.Context, debts []*entity.PrimeDebt) error
}
