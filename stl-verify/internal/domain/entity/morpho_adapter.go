package entity

import (
	"fmt"
	"math"
	"slices"
	"time"
)

// MorphoAdapterType classifies a VaultV2 liquidity adapter by the kind of
// venue it routes assets into.
//
// A VaultV2 never touches Morpho Blue directly: it holds a set of adapter
// contracts, each wrapping one downstream venue. MarketV1 wraps a Morpho Blue
// market; VaultV1 wraps a nested MetaMorpho V1 vault. Unknown is a
// forward-compatible sentinel (mirroring the VaultShaped discovery pattern) so
// an adapter of a not-yet-modelled type is recorded rather than dropped.
//
// Unknown (99) and "no classification at all" are different facts and both are
// representable: 99 means an on-chain probe ran and could not classify the
// adapter, while a nil *MorphoAdapterType on an observation means that
// observation carried no probe (see MorphoAdapterMembership.AdapterType).
type MorphoAdapterType int16

const (
	MorphoAdapterTypeMarketV1 MorphoAdapterType = 1  // MorphoMarketV1AdapterV2 (Morpho Blue market)
	MorphoAdapterTypeVaultV1  MorphoAdapterType = 2  // MorphoVaultV1Adapter (nested MetaMorpho V1 vault)
	MorphoAdapterTypeUnknown  MorphoAdapterType = 99 // unrecognised adapter type, recorded for later curation
)

// IsValid reports whether the type is one of the three modelled values.
func (t MorphoAdapterType) IsValid() bool {
	return t == MorphoAdapterTypeMarketV1 || t == MorphoAdapterTypeVaultV1 || t == MorphoAdapterTypeUnknown
}

// MembershipSource records HOW an adapter-set membership was observed, and is
// stored verbatim in morpho_adapter_membership.observed_via.
type MembershipSource string

const (
	MembershipFromAddAdapter    MembershipSource = "add_adapter_event"
	MembershipFromRemoveAdapter MembershipSource = "remove_adapter_event"
	MembershipFromAllocation    MembershipSource = "allocation_event"
	MembershipFromDiscovery     MembershipSource = "vault_discovery"
	MembershipFromBootstrapSeed MembershipSource = "bootstrap_seed"
)

// membershipSources is the closed set the DB CHECK constraint also enforces.
var membershipSources = []MembershipSource{
	MembershipFromAddAdapter, MembershipFromRemoveAdapter, MembershipFromAllocation,
	MembershipFromDiscovery, MembershipFromBootstrapSeed,
}

// IsTransition reports whether the source witnessed a CHANGE of the adapter set
// rather than asserting its contents.
//
// The distinction decides whether an observation is recorded unconditionally. A
// TRANSITION (an AddAdapter or RemoveAdapter log) is evidence about WHEN the set
// changed, which the log must keep even when the answer is already known — it is
// what makes "the block the adapter was added at" a MIN over observations rather
// than a value some writer has to converge. An ASSERTION (a hash-pinned
// adapters(i) enumeration, or the membership an Allocate log implies) states only
// the set's contents at a position, and is recorded only when it changes the
// answer there.
func (s MembershipSource) IsTransition() bool {
	return s == MembershipFromAddAdapter || s == MembershipFromRemoveAdapter
}

// IsValid reports whether the source is one of the five recorded provenances.
func (s MembershipSource) IsValid() bool {
	return slices.Contains(membershipSources, s)
}

// EndOfBlockLogIndex marks an observation read from end-of-block state rather
// than emitted by a log, so it orders above every log in its block.
//
// A hash-pinned adapters(i) enumeration (vault discovery, the Temporal bootstrap
// seed) has no log to take an index from, and what it reads is the block's FINAL
// state — authoritative over every log in that block, including a removal in the
// last transaction. Sorting it last is therefore correct, not a convention.
const EndOfBlockLogIndex int32 = math.MaxInt32

// BlockPosition identifies one position within the chain's history: a block, the
// reorg version of that block we are reading, and the position inside it. It is
// the ordering tuple morpho_adapter_membership is keyed on, so "was this adapter
// a member as of here" is a bounded backward index scan.
type BlockPosition struct {
	BlockNumber  int64
	BlockVersion int
	LogIndex     int32
}

// MorphoAdapterIdentity is a liquidity adapter registered on a Morpho VaultV2:
// exactly one row per (MorphoVaultID, Address), forever.
//
// Every field is a genuinely immutable identity fact, so the row is written once
// and never converged. Whether the adapter is currently in the vault's adapter
// set, when it entered or left, and how it classifies are observations in
// MorphoAdapterMembership. AssetTokenID is the parent vault's underlying asset,
// the unit of the adapter's realAssets() reading; it is invariantly equal to
// morpho_vault.asset_token_id and denormalised so a state reader needs one join
// rather than two.
type MorphoAdapterIdentity struct {
	ID            int64
	MorphoVaultID int64
	Address       []byte // 20 bytes, the adapter contract address
	AssetTokenID  int64
}

// NewMorphoAdapterIdentity creates a MorphoAdapterIdentity with validation.
func NewMorphoAdapterIdentity(morphoVaultID int64, address []byte, assetTokenID int64) (*MorphoAdapterIdentity, error) {
	i := &MorphoAdapterIdentity{
		MorphoVaultID: morphoVaultID,
		Address:       address,
		AssetTokenID:  assetTokenID,
	}
	if err := i.Validate(); err != nil {
		return nil, fmt.Errorf("NewMorphoAdapterIdentity: %w", err)
	}
	return i, nil
}

func (i *MorphoAdapterIdentity) Validate() error {
	if i.MorphoVaultID <= 0 {
		return fmt.Errorf("morphoVaultID must be positive, got %d", i.MorphoVaultID)
	}
	if len(i.Address) != 20 {
		return fmt.Errorf("address must be 20 bytes, got %d", len(i.Address))
	}
	if i.AssetTokenID <= 0 {
		return fmt.Errorf("assetTokenID must be positive, got %d", i.AssetTokenID)
	}
	return nil
}

// MorphoAdapterMembership is ONE observation of whether an adapter belongs to its
// vault's adapter set, at one block position. It is never a mutable state: a
// later observation is another row, and "is it a member now" is the latest row by
// (BlockNumber, BlockVersion, LogIndex, processing_version) descending.
//
// AdapterType is the classification this observation carried; nil means the
// observation carried no probe, which only a removal may say (the DB CHECK
// mirrors it). MorphoAdapterID is filled in by the repository from the identity
// row, so a caller building an observation leaves it zero.
type MorphoAdapterMembership struct {
	MorphoAdapterID int64
	BlockNumber     int64
	BlockVersion    int
	LogIndex        int32
	Timestamp       time.Time
	IsMember        bool
	AdapterType     *MorphoAdapterType
	ObservedVia     MembershipSource
}

// Position returns the observation's ordering tuple.
func (m *MorphoAdapterMembership) Position() BlockPosition {
	return BlockPosition{BlockNumber: m.BlockNumber, BlockVersion: m.BlockVersion, LogIndex: m.LogIndex}
}

func (m *MorphoAdapterMembership) Validate() error {
	if m.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", m.BlockNumber)
	}
	if m.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", m.BlockVersion)
	}
	if m.LogIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", m.LogIndex)
	}
	if m.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	if !m.ObservedVia.IsValid() {
		return fmt.Errorf("observedVia must be one of %v, got %q", membershipSources, m.ObservedVia)
	}
	if m.AdapterType != nil && !m.AdapterType.IsValid() {
		return fmt.Errorf("adapterType must be 1, 2, or 99, got %d", *m.AdapterType)
	}
	// "Only a removal may be untyped" is deliberately NOT checked here. An
	// assertion whose caller already knows the adapter is a member carries no
	// probe and needs none, because nothing will be appended; the rule binds only
	// at APPEND time, where the repository raises ErrAdapterUnclassified and the
	// DB CHECK is the backstop.
	return nil
}

// MorphoAdapterObservation is what a writer records: the adapter's identity
// (created on first sight) plus one membership observation about it.
type MorphoAdapterObservation struct {
	Identity   MorphoAdapterIdentity
	Membership MorphoAdapterMembership
}

func (o *MorphoAdapterObservation) Validate() error {
	if err := o.Identity.Validate(); err != nil {
		return err
	}
	return o.Membership.Validate()
}

// MorphoAdapterMember is an adapter whose latest observation says it is in its
// vault's adapter set, together with what that observation recorded. AsOfBlock is
// the block of that latest observation, not the block the adapter was added at —
// the add block is MIN(block_number) over its add_adapter_event observations.
type MorphoAdapterMember struct {
	MorphoAdapterIdentity
	AdapterType MorphoAdapterType
	AsOfBlock   int64
	ObservedVia MembershipSource
}
