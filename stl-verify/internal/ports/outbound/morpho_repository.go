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

	// GetOrCreateVault retrieves or creates a MetaMorpho vault.
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
	// row, keyed on the ACTIVE incarnation of (morpho_vault_id, address): if an
	// active row exists it is reused and its added_at_block converges downward to
	// the earliest observation (LEAST), so a lazily-registered adapter collapses
	// onto the true AddAdapter block once the backfiller replays it, rather than
	// creating a second active row. A distinct row is created only for a re-add
	// after removal (the UNIQUE key includes added_at_block). Returns the row's ID.
	GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error)

	// MarkAdapterRemoved records the block at which an adapter was de-registered.
	// The removal is idempotent for the same block (backfill replays); an unknown
	// adapter or one already removed at a different block is a data bug and errors.
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

	// UpdateVaultFeeConfig applies a partial fee-configuration update to a vault.
	// Nil fields in the update leave their columns untouched; an unknown vault errors.
	UpdateVaultFeeConfig(ctx context.Context, tx pgx.Tx, morphoVaultID int64, update entity.MorphoVaultFeeUpdate) error
}
