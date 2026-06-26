package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// FluidVaultRepository persists Fluid vault registry rows and their per-block
// aggregate state snapshots (append-only, ADR-0002).
type FluidVaultRepository interface {
	// UpsertVaults registers vaults and returns vault address -> fluid_vault.id.
	// Registry fields are immutable per vault; an incoming row that differs from
	// the stored one fails the call rather than overwriting it.
	UpsertVaults(ctx context.Context, tx pgx.Tx, vaults []*entity.FluidVault) (map[common.Address]int64, error)

	// GetAllVaults returns all known vaults for a chain, keyed by contract address.
	GetAllVaults(ctx context.Context, chainID int64) (map[common.Address]*entity.FluidVault, error)

	// SaveVaultStates appends per-vault aggregate snapshots within an external
	// transaction. The BEFORE INSERT trigger assigns processing_version and
	// ON CONFLICT DO NOTHING dedupes same-build retries.
	SaveVaultStates(ctx context.Context, tx pgx.Tx, states []*entity.FluidVaultState) error
}
