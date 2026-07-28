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
	//  1. A CLOSED incarnation whose window STRICTLY covers the candidate (its
	//     removed_at_block is above the candidate's added_at_block) wins FIRST. This
	//     step must precede the active-row match: for a removed-then-re-added adapter
	//     a closed row and an active row coexist, and a backfilled add belonging to
	//     the earlier (closed) window would otherwise match the active row, dragging
	//     the re-added incarnation's added_at_block into a prior window and leaving
	//     the closed row unconverged. With no active row it would instead insert a
	//     spuriously-active duplicate, resurrecting a de-registered adapter into
	//     GetActiveAdaptersByVault / realAssets forever. Strict, not inclusive: a
	//     governance multicall can remove and re-add an adapter in ONE block, and an
	//     add AT a prior removal block must open a new incarnation rather than fold
	//     into the row just closed.
	//  2. Otherwise an ACTIVE row is reused, its added_at_block converging downward
	//     to the earliest observation (SQL LEAST), so a lazily-registered adapter
	//     collapses onto the true AddAdapter block once the backfiller replays it
	//     rather than becoming a second active row.
	//  3. Only a candidate added at or after every prior removal is a genuinely new
	//     incarnation and gets its own row (the UNIQUE key includes added_at_block, so
	//     a same-block replay is idempotent).
	//
	// Both convergence steps also curate adapter_type: a row recorded as Unknown is
	// upgraded when a replay supplies a real type, and a real type is never
	// overwritten. Returns the row's ID.
	//
	// One shape this key cannot represent is documented as a Residual on the
	// PostgreSQL implementation: add→remove→re-add inside a single block.
	GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error)

	// MarkAdapterRemoved records the block at which an adapter was de-registered,
	// closing the incarnation live at removedAtBlock — the latest one registered at
	// or before it. Serializes on the SAME per-(morpho_vault_id, address) advisory
	// lock as GetOrCreateAdapter, so registrations and removals of one adapter never
	// interleave (ADR-0002 §3).
	//
	// The close block is a converging observation, resolved from the incarnation's
	// state:
	//
	//   - OPEN incarnation (removed_at_block IS NULL): closes at removedAtBlock.
	//   - CLOSED incarnation, removedAtBlock within 64 blocks (Ethereum's
	//     finality/reorg bound) of the recorded close, in EITHER direction: the same
	//     removal, relocated by a reorg or replayed by the backfiller.
	//     removed_at_block converges to the EARLIEST observation, mirroring
	//     added_at_block, so the two arrive-in-any-order observations settle on the
	//     same value; a same-block replay is a no-op.
	//   - CLOSED incarnation, removedAtBlock further than 64 blocks from the recorded
	//     close, in EITHER direction: NOT a relocation, so it errors rather than
	//     rewriting the recorded close. See "Why the relocation bound is symmetric"
	//     below.
	//
	// Any close that NARROWS the recorded lifetime — an initial close, or a
	// convergence downward — is refused if the row owns adapter_state snapshots
	// recorded strictly after the new close block: they would be stranded outside the
	// lifetime window, so the caller must re-home them onto the incarnation that owns
	// them first. Snapshots IN the close block are inside the window. A close that
	// leaves removed_at_block unchanged skips the guard: it cannot strand anything the
	// guard did not already vet, and re-running it would turn an at-least-once SQS
	// redelivery into a poison pill.
	//
	// removedAtBlockVersion is the block_version of the block the removal was
	// observed in, and scopes that guard: snapshots above the new close carrying a
	// LOWER block_version are dead-chain residue, not orphans. Replacing a block
	// forces the watcher to republish every descendant at a bumped version, so a
	// snapshot recorded above the close at an older version belongs to the chain the
	// relocating reorg replaced. Without this the guard poison-pilled a CORRECT
	// removal: a reorg that moves a RemoveAdapter one block earlier leaves the old
	// chain's snapshot sitting just above the new close forever, and morpho_adapter
	// carries no block_version of its own to compare against.
	//
	// An adapter with no incarnation registered at or before removedAtBlock is a data
	// bug and errors.
	//
	// # Why the relocation bound is symmetric
	//
	// A removal far BELOW the recorded close looks like a repair opportunity: the
	// registry conflated two incarnations, and the earlier removal is the true one, so
	// converging down would fix the row. It is refused anyway, because that repair is
	// sound only when the replay ALSO covers the later incarnation's full history, and
	// nothing enforces that. A bounded replay of a conflated row instead erases the
	// recorded de-registration and leaves an adapter that is removed on-chain
	// permanently ACTIVE in the registry, with its snapshots outside the window
	// (reproduced in
	// TestMarkAdapterRemoved_ConflatedIncarnationsFromABoundedReplayAreRefused).
	// Under-repair that is only conditionally sound, a loud stop wins: removed_at_block
	// is a recorded fact, and no observation more than a reorg away from it can be
	// evidence about the same removal.
	//
	// The real fix is an incarnation-sequence key on morpho_adapter, so a replayed add
	// or remove names which lifetime it belongs to instead of being matched by block
	// range, and morpho_adapter_state rows can be re-homed between them. That is
	// ticketed as a follow-up; until it lands, a conflated row is repaired by hand or
	// by a replay spanning the adapter's whole lifecycle history.
	MarkAdapterRemoved(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64, removedAtBlockVersion int) error

	// GetActiveAdapter retrieves the active (not-yet-removed) adapter for a vault
	// and address, reading within the caller's transaction so it sees writes made
	// earlier in the same tx. Returns nil, nil if there is no active adapter.
	GetActiveAdapter(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte) (*entity.MorphoAdapter, error)

	// GetAdapterIncarnationAt retrieves the incarnation whose recorded lifetime
	// contains atBlock — added at or before it, and either still open or closed at or
	// after it. Returns nil, nil when the adapter has no recorded incarnation there.
	//
	// This is the question a RemoveAdapter must ask before registering anything: a
	// non-nil answer means MarkAdapterRemoved already has a row to close, so the
	// removal is idempotent against it. A nil answer means the adapter is unknown at
	// that block and needs registering first — including the case where a closed
	// incarnation ends BELOW atBlock, which is a later incarnation whose AddAdapter
	// was never observed. Reads within the caller's transaction (read-your-writes).
	GetAdapterIncarnationAt(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, atBlock int64) (*entity.MorphoAdapter, error)

	// GetActiveAdaptersByVault retrieves all currently-active adapters for a vault.
	GetActiveAdaptersByVault(ctx context.Context, morphoVaultID int64) ([]*entity.MorphoAdapter, error)

	// SaveAdapterState saves an adapter realAssets() snapshot within an external transaction.
	SaveAdapterState(ctx context.Context, tx pgx.Tx, state *entity.MorphoAdapterState) error

	// SaveVaultCap saves a VaultV2 allocation-cap snapshot within an external transaction.
	SaveVaultCap(ctx context.Context, tx pgx.Tx, vaultCap *entity.MorphoVaultCap) error

	// SaveVaultFee saves a VaultV2 full fee-config snapshot within an external transaction.
	SaveVaultFee(ctx context.Context, tx pgx.Tx, vaultFee *entity.MorphoVaultFee) error
}
