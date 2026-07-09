package outbound

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

// PinMode enumerates the legitimate ways to pin an on-chain read to a block.
// The zero value is invalid: an unset pin fails hard at the reader instead of
// silently number-pinning (the VEC-471 bug class).
type PinMode uint8

const (
	// PinReorgSafe pins by block hash. Required for versioned per-block state on
	// the live path: after a reorg an archive node answers eth_call-by-number
	// with the new canonical state, which can silently disagree with the
	// reorged (older-version) data being processed.
	PinReorgSafe PinMode = iota + 1
	// PinSettled pins by number, for replaying an already-settled block with no
	// live BlockEvent (backfill, CLI snapshot): no fork ambiguity to guard.
	PinSettled
	// PinStatic pins by number, for structurally immutable identity data (token
	// metadata, market params, ilks) that is not versioned per block.
	PinStatic
)

// BlockPin is the coordinate of one on-chain read: which block, and how the
// read is pinned to it. Opaque and immutable — the constructors are the only
// way to build one. The reorg-safe path (PinForEvent) validates a non-zero
// hash and positive block number; number-pinned paths accept the caller's
// number as-is.
type BlockPin struct {
	number  int64
	version int
	hash    common.Hash
	mode    PinMode
}

// PinForEvent derives the reorg-safe pin for the exact block a live event
// describes. It fails on a missing hash rather than downgrading to
// number-pinning; for state reads this subsumes the ParsedBlockHash guard.
func PinForEvent(e BlockEvent) (BlockPin, error) {
	h, err := e.ParsedBlockHash()
	if err != nil {
		return BlockPin{}, fmt.Errorf("deriving block pin: %w", err)
	}
	if e.BlockNumber <= 0 {
		return BlockPin{}, fmt.Errorf("block %d v%d: non-positive block number on event", e.BlockNumber, e.Version)
	}
	return BlockPin{number: e.BlockNumber, version: e.Version, hash: h, mode: PinReorgSafe}, nil
}

// PinForSettledBlock builds a number pin for replaying an already-settled
// block that has no BlockEvent (backfillers, CLI snapshots). Live handlers
// hold a BlockEvent and use PinForEvent; reaching this constructor requires an
// explicit, greppable call site.
func PinForSettledBlock(blockNumber int64, version int) BlockPin {
	return BlockPin{number: blockNumber, version: version, mode: PinSettled}
}

// PinForStaticRead builds a number pin for structurally immutable identity
// data read at startup, where no block event exists. Version is 0, matching
// the archive keys such reads produced before the pin existed. Mid-block
// static probes derive from the event pin via Static() instead, preserving
// the block's version.
func PinForStaticRead(blockNumber int64) BlockPin {
	return BlockPin{number: blockNumber, mode: PinStatic}
}

// Static derives a static-probe pin at this pin's block, preserving the
// version so a metadata read issued while processing block N v2 archives
// under v2 — exactly as the context-stamped path did.
func (p BlockPin) Static() BlockPin {
	return BlockPin{number: p.number, version: p.version, mode: PinStatic}
}

func (p BlockPin) Number() int64 { return p.number }
func (p BlockPin) Version() int  { return p.version }
func (p BlockPin) Mode() PinMode { return p.mode }

// Hash returns the pinned hash; ok is false for number-pinned modes.
func (p BlockPin) Hash() (common.Hash, bool) {
	return p.hash, p.mode == PinReorgSafe
}

func (p BlockPin) IsZero() bool { return p.mode == 0 }
