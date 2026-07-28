package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MorphoRepository defines the interface for Morpho protocol data persistence.
type MorphoRepository interface {
	// GetOrCreateMarket retrieves or creates a Morpho Blue market.
	// Returns the market's database ID.
	GetOrCreateMarket(ctx context.Context, tx pgx.Tx, market *entity.MorphoMarket) (int64, error)

	// GetMarketByMarketID retrieves a market by its chain ID and 32-byte market ID hash.
	// Returns nil, nil if the market doesn't exist.
	GetMarketByMarketID(ctx context.Context, chainID int64, marketID common.Hash) (*entity.MorphoMarket, error)

	// SaveMarketState saves a market state snapshot within an external transaction.
	SaveMarketState(ctx context.Context, tx pgx.Tx, state *entity.MorphoMarketState) error

	// SaveMarketPosition saves a user market position snapshot within an external transaction.
	SaveMarketPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoMarketPosition) error

	// GetOrCreateVault retrieves or creates a MetaMorpho vault, converging
	// created_at_block downward to the earliest observation (LEAST) so a vault
	// first seen mid-life does not keep a wrong deploy block forever.
	// Returns the vault's database ID.
	GetOrCreateVault(ctx context.Context, tx pgx.Tx, vault *entity.MorphoVault) (int64, error)

	// GetVaultByAddress retrieves a vault by its chain ID and contract address.
	// Returns nil, nil if the vault doesn't exist.
	GetVaultByAddress(ctx context.Context, chainID int64, address common.Address) (*entity.MorphoVault, error)

	// GetAllVaults retrieves all known vaults for a chain, keyed by contract address.
	GetAllVaults(ctx context.Context, chainID int64) (map[common.Address]*entity.MorphoVault, error)

	// SaveVaultState saves a vault state snapshot within an external transaction.
	SaveVaultState(ctx context.Context, tx pgx.Tx, state *entity.MorphoVaultState) error

	// SaveVaultPosition saves a user vault position snapshot within an external transaction.
	SaveVaultPosition(ctx context.Context, tx pgx.Tx, position *entity.MorphoVaultPosition) error

	// GetOrCreateAdapter retrieves or creates a VaultV2 liquidity adapter registry
	// row for (morpho_vault_id, address). The candidate's added_at_block is matched
	// against the adapter's incarnations in a load-bearing ORDER:
	//
	//  1. A CLOSED incarnation whose window strictly covers the candidate wins
	//     FIRST. This step must precede the active-row match: for a removed-then-re-added
	//     adapter a closed row and an active row coexist, and a backfilled add
	//     belonging to the earlier (closed) window would otherwise match the active
	//     row, dragging the re-added incarnation's added_at_block into a prior
	//     window and leaving the closed row unconverged. With no active row it would
	//     instead insert a spuriously-active duplicate, resurrecting a de-registered
	//     adapter into GetActiveAdaptersByVault / realAssets forever.
	//  2. Otherwise an ACTIVE row is reused, its added_at_block converging downward
	//     to the earliest observation (LEAST), so a lazily-registered adapter
	//     collapses onto the true AddAdapter block once the backfiller replays it
	//     rather than becoming a second active row.
	//  3. Only a candidate added at or after every prior removal is a genuinely new
	//     incarnation and gets its own row (the UNIQUE key includes added_at_block).
	//
	// Both convergence steps also curate adapter_type: a row recorded as Unknown is
	// upgraded when a replay supplies a real type, and a real type is never
	// overwritten. Returns the row's ID.
	GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error)

	// MarkAdapterRemoved records the block at which an adapter was de-registered,
	// closing the incarnation that was live at removedAtBlock. removed_at_block
	// converges to the earliest observation, so a same-block replay is a no-op and a
	// reorg that re-lands the removal in a neighbouring block converges instead of
	// poisoning the queue. An adapter with no incarnation registered at or before
	// the removal block, or a close that would strand adapter_state snapshots after
	// the removal block, is a data bug and errors.
	MarkAdapterRemoved(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64) error

	// GetActiveAdapter retrieves the active (not-yet-removed) adapter for a vault
	// and address, reading within the caller's transaction so it sees writes made
	// earlier in the same tx. Returns nil, nil if there is no active adapter.
	GetActiveAdapter(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte) (*entity.MorphoAdapter, error)

	// GetActiveAdaptersByVault retrieves all currently-active adapters for a vault.
	GetActiveAdaptersByVault(ctx context.Context, morphoVaultID int64) ([]*entity.MorphoAdapter, error)

	// SaveAdapterState saves an adapter realAssets() snapshot within an external transaction.
	SaveAdapterState(ctx context.Context, tx pgx.Tx, state *entity.MorphoAdapterState) error

	// SaveVaultCap saves a VaultV2 allocation-cap snapshot within an external transaction.
	SaveVaultCap(ctx context.Context, tx pgx.Tx, vaultCap *entity.MorphoVaultCap) error

	// SaveVaultFee saves a VaultV2 full fee-config snapshot within an external transaction.
	SaveVaultFee(ctx context.Context, tx pgx.Tx, vaultFee *entity.MorphoVaultFee) error
}
