package outbound

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ErrAdapterUnclassified reports that EnsureIncarnationToClose had to register the
// incarnation a removal closes but was given no classification to register it with.
// Distinguished from any other failure because the caller — not the registry — is what
// has to change: it must supply the on-chain type probe's answer.
var ErrAdapterUnclassified = errors.New("no adapter classification supplied to register the incarnation a removal closes")

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
	// NEITHER convergence step bounds added_at_block from below, deliberately. Together
	// they are what make the single-incarnation mid-life-discovery case work — a row
	// seeded at the discovery block folding down onto the true AddAdapter block, which is
	// the whole reason the convergence exists (step 2 while that row is still open, step
	// 1 once it has been closed) — and no correct lower bound is expressible with this
	// key. The only candidate is the PREVIOUS incarnation's removed_at_block, and when
	// the registry has conflated two lifetimes that row does not exist: precisely the
	// shape the bound would need to catch. Bounding on a guess would break the legitimate
	// case to half-catch the broken one. Instead the fold stays unbounded and the
	// consequence is caught downstream, loudly, by MarkAdapterRemoved's symmetric
	// relocation bound — see its "Why the relocation bound is symmetric" section, which
	// also names the incarnation-sequence key as the real fix.
	//
	// One shape this key cannot represent is documented as a Residual on the
	// PostgreSQL implementation: add→remove→re-add inside a single block.
	GetOrCreateAdapter(ctx context.Context, tx pgx.Tx, adapter *entity.MorphoAdapter) (int64, error)

	// EnsureIncarnationToClose guarantees that a removal at removedAtBlock has exactly
	// one incarnation for MarkAdapterRemoved to close, registering one only when the
	// registry holds none. Reports whether it registered.
	//
	// This is ONE method, rather than a lookup the caller composes with a create,
	// because the decision and the insert it authorises must not be separable: the
	// read-then-write is serialized on the same per-(morpho_vault_id, address) advisory
	// lock as GetOrCreateAdapter and MarkAdapterRemoved, taken BEFORE the read
	// (ADR-0002 §3). Two overlapping writers that both read an empty registry mint two
	// incarnations for one on-chain lifetime, and ON CONFLICT cannot catch it because
	// their added_at_block differ. MarkAdapterRemoved's own acquisition of the same key
	// later in the transaction is re-entrant, not a second wait.
	//
	// A removal's decisive question is not "is there an ACTIVE row" but "is there an
	// incarnation COVERING removedAtBlock" — added at or before it and either still open
	// or closed at or after it. That is what makes a redelivered or replayed removal
	// idempotent: its incarnation is already closed at that very block, so an active-row
	// check would miss and register a zero-length incarnation the chain never had.
	//
	// Only when nothing covers the block is the adapter genuinely unknown there and a
	// zero-length incarnation registered from candidate — including the case of a closed
	// incarnation ending BELOW the removal, which is a later lifetime whose AddAdapter
	// was never observed.
	//
	// candidate is used ONLY on that registration path. A nil candidate means the caller has no
	// on-chain classification to record (the pre-transaction probe found the adapter
	// already active), so a registration it cannot honour fails with
	// ErrAdapterUnclassified rather than recording a defaulted type.
	//
	// # Why the registration cannot go through GetOrCreateAdapter
	//
	// GetOrCreateAdapter's active-row match converges added_at_block downward, which is
	// right for an AddAdapter — the earliest observation of a lifetime's START is the
	// better estimate of it. Applied to a removal heal it is catastrophic: healing a
	// historical removal at block B drags an on-chain-ACTIVE later incarnation down to
	// added_at_block = B, and the close then de-registers an adapter that is still
	// allocating (reproduced in
	// TestCreateAdapterIncarnation_HealingAnUnobservedRemovalSparesALaterIncarnation).
	// A removal carries no information about when its lifetime began, so it may not move
	// any added_at_block. What this writes instead is exactly the row it is given and
	// nothing else: an incarnation already recorded at the same (vault, address, added
	// block) is left untouched, so it can never fold onto, move, or re-close a DIFFERENT
	// incarnation.
	//
	// removedAtBlock is both bounds of the registered lifetime: added_at_block is set to
	// it as a LOWER BOUND, not a claim that the adapter was added there. A later replay
	// of the true AddAdapter converges it down through GetOrCreateAdapter's
	// closed-window match, which is where convergence belongs.
	//
	// A zero-length lifetime is the only shape this registers, and two properties depend
	// on it: the row is born CLOSED, so it cannot collide with a still-active later
	// incarnation on the partial unique index; and it owns no snapshots at insert time,
	// so it needs no orphan check — the only registration path that skips one.
	EnsureIncarnationToClose(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte, removedAtBlock int64, candidate *entity.MorphoAdapter) (bool, error)

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
	// Any close that NARROWS the recorded lifetime — an initial close, or a convergence
	// downward — is refused if the row owns adapter_state snapshots recorded strictly
	// after the new close block. Snapshots IN the close block are inside the window. A
	// close that leaves removed_at_block unchanged skips the check: it cannot strand
	// anything already vetted, and re-running it would turn an at-least-once redelivery
	// into a poison pill.
	//
	// The convergence arm is guarded too, and must be. Within the relocation window the
	// bound below cannot tell a reorg apart from a replay that conflated two lifetimes
	// only a few blocks apart, so the stranded snapshots are the ONLY remaining evidence
	// — exempting that arm silently erases the later de-registration. The check compares
	// block_number alone; the implementation records why block_version cannot arbitrate.
	//
	// Refusal is an operator-facing poison pill, not an assertion: nothing in this
	// service re-homes morpho_adapter_state rows (they are keyed by morpho_adapter_id and
	// no code path reassigns it) or deletes dead-chain residue, so the event stalls its
	// FIFO queue until an operator does one or the other by hand.
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
	// Given a repair that is only conditionally sound, a loud stop wins:
	// removed_at_block is a recorded fact, and no observation more than a reorg away
	// from it can be evidence about the same removal.
	//
	// The BELOW-window arm is reachable in production, not merely defensive: a bounded
	// replay of a conflated row hits it whenever EnsureIncarnationToClose finds a covering
	// incarnation — so it registers nothing, correctly — whose recorded close sits more
	// than 64 blocks above the removal's own block. Treat it as an operator-facing poison
	// pill, not an assertion: the event stalls its FIFO queue until the row is repaired.
	// The ABOVE-window arm has no service route today (a removal above a closed row makes
	// EnsureIncarnationToClose register [B,B] first, so the distance is 0); it guards
	// direct and future callers.
	//
	// The real fix is an incarnation-sequence key on morpho_adapter, so a replayed add
	// or remove names which lifetime it belongs to instead of being matched by block
	// range, and morpho_adapter_state rows can be re-homed between them. Until that
	// lands, a conflated row is repaired by hand or by a replay spanning the adapter's
	// whole lifecycle history.
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
