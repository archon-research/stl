package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// appendedRows is how many versioned rows a replay actually wrote, per table.
//
// The event count alone cannot tell a healthy run from one that persisted nothing: a
// replay whose every write deduped reports the same "292 events" as the run that wrote
// them, which is how the compressed-chunk drop 20260821_120000 fixes went unnoticed for a
// whole E2E. Kept per table rather than as one sum so a table that wrote nothing stays
// visible behind the ones that did.
type appendedRows struct {
	AdapterStates          int `json:"adapterStates"`
	VaultCaps              int `json:"vaultCaps"`
	VaultFees              int `json:"vaultFees"`
	MembershipObservations int `json:"membershipObservations"`
}

func (a appendedRows) total() int {
	return a.AdapterStates + a.VaultCaps + a.VaultFees + a.MembershipObservations
}

func (a *appendedRows) add(other appendedRows) {
	a.AdapterStates += other.AdapterStates
	a.VaultCaps += other.VaultCaps
	a.VaultFees += other.VaultFees
	a.MembershipObservations += other.MembershipObservations
}

// String keeps the breakdown to one log value, readable through both loggers a run uses
// (slog on the backfiller, Temporal's on the activity).
func (a appendedRows) String() string {
	return fmt.Sprintf("total=%d adapterStates=%d vaultCaps=%d vaultFees=%d membershipObservations=%d",
		a.total(), a.AdapterStates, a.VaultCaps, a.VaultFees, a.MembershipObservations)
}

// countingMorphoRepository tallies the rows a replay appends through the morpho
// repository. It wraps the port rather than reaching into the service, so the counting
// lives where the backfiller does its own wiring and the indexer's write path is
// untouched.
//
// Embedded interface, not a hand-written pass-through: the port carries a dozen read
// methods this has nothing to say about. Not safe for concurrent use — one partition's
// logs replay sequentially (see replayPartition).
type countingMorphoRepository struct {
	outbound.MorphoRepository
	counts appendedRows
}

func newCountingMorphoRepository(inner outbound.MorphoRepository) *countingMorphoRepository {
	return &countingMorphoRepository{MorphoRepository: inner}
}

func (c *countingMorphoRepository) SaveAdapterState(ctx context.Context, tx pgx.Tx, state *entity.MorphoAdapterState) (bool, error) {
	appended, err := c.MorphoRepository.SaveAdapterState(ctx, tx, state)
	if appended {
		c.counts.AdapterStates++
	}
	return appended, err
}

func (c *countingMorphoRepository) SaveVaultCap(ctx context.Context, tx pgx.Tx, vaultCap *entity.MorphoVaultCap) (bool, error) {
	appended, err := c.MorphoRepository.SaveVaultCap(ctx, tx, vaultCap)
	if appended {
		c.counts.VaultCaps++
	}
	return appended, err
}

func (c *countingMorphoRepository) SaveVaultFee(ctx context.Context, tx pgx.Tx, vaultFee *entity.MorphoVaultFee) (bool, error) {
	appended, err := c.MorphoRepository.SaveVaultFee(ctx, tx, vaultFee)
	if appended {
		c.counts.VaultFees++
	}
	return appended, err
}

func (c *countingMorphoRepository) ObserveAdapterMembership(ctx context.Context, tx pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
	adapterID, appended, err := c.MorphoRepository.ObserveAdapterMembership(ctx, tx, obs)
	if appended {
		c.counts.MembershipObservations++
	}
	return adapterID, appended, err
}
