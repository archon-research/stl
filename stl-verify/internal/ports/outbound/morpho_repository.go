package outbound

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ErrAdapterUnclassified reports that ObserveAdapterMembership had to append an
// observation of MEMBERSHIP but was given no classification to record with it — on any
// path, matching the table's CHECK rather than a subset of it. Distinguished from any
// other failure because the caller — not the registry — is what has to change: it must
// supply the on-chain type probe's answer. An observation of NON-membership is exempt: an
// adapter first seen by its own de-registration has no known type and records none.
//
// In practice only an assertion reaches it: an AddAdapter transition carries the probe's
// answer by construction. The guard stays wider than that so a future caller gets a named
// error instead of a raw constraint violation.
var ErrAdapterUnclassified = errors.New("no adapter classification supplied to record an observation of membership")

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

	// ObserveAdapterMembership records one observation of whether an adapter belongs to a
	// vault's adapter set, creating the adapter's identity row on first sight. Returns the
	// adapter's stable registry id and whether an observation row was appended.
	//
	// A TRANSITION (an AddAdapter or RemoveAdapter log) is always appended: it is evidence
	// about WHEN the set changed, which the log must keep even when the answer is already
	// known — it is what makes "the block the adapter was added at" a MIN over observations
	// instead of a value some writer has to converge. An ASSERTION (a hash-pinned
	// adapters(i) enumeration, or the membership an Allocate log implies) is appended only
	// when the log does not already give the same answer at that block position: appending
	// unconditionally would put one row per allocation event in a table sized for
	// governance events.
	//
	// That conditional is a read-then-write decision, so the assertion path — and only it —
	// serializes on a per-(morpho_vault_id, address) advisory lock taken BEFORE the decisive
	// read (ADR-0002 §3). A transition needs none: it is an unconditional INSERT … ON
	// CONFLICT DO NOTHING with no decision to serialize.
	//
	// Appending an observation of MEMBERSHIP requires a classification; a nil AdapterType
	// fails with ErrAdapterUnclassified rather than recording a defaulted type, on the
	// transition path as well as the assertion one (see that error). An observation of
	// NON-membership may be untyped — an adapter first observed by its own removal has no
	// known type, and NULL is the honest record.
	//
	// Idempotent on the full membership key (adapter, block_number, block_version,
	// log_index, processing_version): the table's trigger reuses the version only for a
	// retry from the SAME build_id, so a redelivery or an exact replay converges on one row
	// and reports appended=false, while a reprocess from a new build appends its own
	// version+1 row at the same position. Nothing is ever updated, so a reorg-relocated or
	// re-observed transition is simply another row at its own position and the ordering
	// tuple selects between them — there is no convergence, no relocation bound, and no
	// incarnation for a snapshot to be stranded by.
	ObserveAdapterMembership(ctx context.Context, tx pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error)

	// GetActiveAdapter returns the adapter and its current membership for (vault, address),
	// reading within the caller's transaction so it sees writes made earlier in the same tx.
	// Returns nil, nil when the adapter is unknown or its latest observation says it is not
	// a member.
	GetActiveAdapter(ctx context.Context, tx pgx.Tx, morphoVaultID int64, address []byte) (*entity.MorphoAdapterMember, error)

	// GetActiveAdapterAt is GetActiveAdapter as of a block position, for replay and for the
	// pre-transaction probe decision: it answers "was this adapter a member HERE", not
	// "is it a member now", so a backfiller replaying a historical block is not told about
	// the present. Reads committed state through the pool.
	GetActiveAdapterAt(ctx context.Context, morphoVaultID int64, address []byte, at entity.BlockPosition) (*entity.MorphoAdapterMember, error)

	// GetActiveAdaptersByVaultAt returns every adapter the log calls a member of the
	// vault's set as of a block position — GetActiveAdapterAt for the whole set.
	//
	// Position-scoped with no unbounded variant on purpose: the only reason to read the
	// whole set is to diff it against an enumeration, and an enumeration is always pinned
	// to a block. Answering about the head instead would report an adapter added above
	// that block as a member the enumeration failed to return.
	GetActiveAdaptersByVaultAt(ctx context.Context, morphoVaultID int64, at entity.BlockPosition) ([]*entity.MorphoAdapterMember, error)

	// SaveAdapterState saves an adapter realAssets() snapshot within an external transaction.
	SaveAdapterState(ctx context.Context, tx pgx.Tx, state *entity.MorphoAdapterState) error

	// SaveVaultCap saves a VaultV2 allocation-cap snapshot within an external transaction.
	SaveVaultCap(ctx context.Context, tx pgx.Tx, vaultCap *entity.MorphoVaultCap) error

	// SaveVaultFee saves a VaultV2 full fee-config snapshot within an external transaction.
	SaveVaultFee(ctx context.Context, tx pgx.Tx, vaultFee *entity.MorphoVaultFee) error
}
