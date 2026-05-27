package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MapleRepository persists Maple Finance on-chain data
// (Syrup ERC-4626 vault registry, per-block vault state, per-user positions).
type MapleRepository interface {
	// GetAllVaults loads the full registry of known Syrup vaults for a chain,
	// keyed by the vault's contract address.
	GetAllVaults(ctx context.Context, chainID int64) (map[common.Address]*entity.MapleVault, error)

	// SaveVaultState writes a per-block vault snapshot inside an external tx.
	// Idempotent under (vault, block_number, block_version, timestamp, build_id) —
	// the processing_version trigger reuses an existing version on retry, and the
	// repository uses ON CONFLICT DO NOTHING for same-build replays.
	SaveVaultState(ctx context.Context, tx pgx.Tx, state *entity.MapleVaultState) error

	// SaveVaultPositions writes per-user position snapshots for a single block in
	// one batch, inside an external tx. Idempotent under the same key as state.
	SaveVaultPositions(ctx context.Context, tx pgx.Tx, positions []*entity.MapleVaultPosition) error
}
