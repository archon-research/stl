package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// PrimeDebtRepository defines persistence operations for prime agent debt tracking.
type PrimeDebtRepository interface {
	// GetPrimes returns all registered prime agent vaults from the database.
	// The service calls this at startup to resolve which vaults to track.
	GetPrimes(ctx context.Context) ([]entity.Prime, error)

	// SaveDebtSnapshots persists a batch of on-chain debt readings in a single transaction.
	SaveDebtSnapshots(ctx context.Context, debts []*entity.PrimeDebt) error

	// ProtocolIDByAddress resolves an existing protocol row by (chain, contract address) and errors
	// if there is none. The indexer calls it once at startup for the Vat it reads, so the row's
	// identity stays owned by the migration that seeds it rather than being minted here.
	ProtocolIDByAddress(ctx context.Context, chainID int64, address common.Address) (int64, error)
}
