// Package blockversion answers which block_version a height was indexed under, for a
// replay that reads its events from a node and so carries no version of its own.
//
// The rule is the maintainer-set highest-version-wins one every read of the raw buckets
// uses — stated in full on the morpho-vault-backfill's listHighestVersionReceipts, which
// resolves the same version from the key it replays. The archive is asked, rather than
// block_states, because it is the same source a replay of the stored payload would use,
// and because it can also prove the version it names speaks for the block being replayed.
package blockversion

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ErrHeightNotArchived marks a height the archive cannot answer for: it holds no object
// there, or none of the objects it holds identifies a block. Either way the archive is
// what has to be repaired — by the republisher or the bulk downloader — before a replay
// of that height can carry the version live indexing used.
var ErrHeightNotArchived = errors.New("the raw archive identifies no block at that height")

// ErrArchivedBlockMismatch marks an archive that holds a DIFFERENT block at the height
// being replayed: an orphaned fork kept past its reorg, the ARCT-379 hole shape. Its
// version speaks for that block, not for the canonical one, so a run stops here rather
// than stamp rows with a version no canonical block was archived under.
var ErrArchivedBlockMismatch = errors.New("the raw archive holds another block at that height")

// Resolver answers from the raw archive, and proves the archive holds the block being
// asked about before it answers.
//
// One Resolver serves one replay run. Its memo is only true of the archive the run read,
// so a run that inherited another's would stamp a version it never proved and could not
// be cleared by repairing the archive and starting again.
type Resolver struct {
	archive     outbound.ArchiveReader
	archiveName string

	resolved map[int64]archivedBlock
}

// archivedBlock is what the archive holds at one height: its TOP version, and the block
// that version identifies.
type archivedBlock struct {
	version int
	hash    common.Hash
}

// NewResolver takes the archive to ask and the name to call it in errors and logs — the
// bucket URL for the S3 one, so an operator is told what to repair.
func NewResolver(archive outbound.ArchiveReader, archiveName string) *Resolver {
	return &Resolver{
		archive:     archive,
		archiveName: archiveName,
		resolved:    map[int64]archivedBlock{},
	}
}

func (r *Resolver) ResolveBlockVersion(ctx context.Context, blockNumber int64, blockHash common.Hash) (int, error) {
	if archived, memoized := r.resolved[blockNumber]; memoized {
		if err := r.requireSameBlockAsProved(blockNumber, archived, blockHash); err != nil {
			return 0, err
		}
		return archived.version, nil
	}
	archived, err := r.readArchivedBlock(ctx, blockNumber)
	if err != nil {
		return 0, err
	}
	if err := r.requireSameBlock(blockNumber, archived, blockHash); err != nil {
		return 0, err
	}
	r.remember(blockNumber, archived)
	return archived.version, nil
}

func (r *Resolver) requireSameBlock(blockNumber int64, archived archivedBlock, blockHash common.Hash) error {
	if archived.hash == blockHash {
		return nil
	}
	return fmt.Errorf("block %d version %d in %s holds %s, replaying %s: %w",
		blockNumber, archived.version, r.archiveName, archived.hash.Hex(), blockHash.Hex(), ErrArchivedBlockMismatch)
}

// requireSameBlockAsProved re-proves a memoised height: the version is true of the block
// it was proved against, so a second, different hash at that height is the node handing
// one run two blocks, not the archive holding another one.
func (r *Resolver) requireSameBlockAsProved(blockNumber int64, archived archivedBlock, blockHash common.Hash) error {
	if archived.hash == blockHash {
		return nil
	}
	return fmt.Errorf("block %d version %d in %s was already proved to be %s, and the node now replays %s at that height: %w",
		blockNumber, archived.version, r.archiveName, archived.hash.Hex(), blockHash.Hex(), ErrArchivedBlockMismatch)
}

// RunSummary is what one run asked the archive for: how many heights it answered, and
// what each version it answered with covers.
type RunSummary struct {
	HeightsResolved int
	// Versions holds one entry per distinct version, ascending.
	Versions []VersionExtent
}

// VersionExtent is one version's share of a run: how many heights resolved to it, and the
// lowest and highest of them. A version above 0 is what live indexing stamped at those
// heights, not evidence of a reorg — most of the deep history the bulk downloader wrote
// exists only at version 1.
type VersionExtent struct {
	Version int
	Heights int
	From    int64
	To      int64
}

func (r *Resolver) Summary() RunSummary {
	extents := map[int]VersionExtent{}
	for height, archived := range r.resolved {
		extent, seen := extents[archived.version]
		if !seen {
			extent = VersionExtent{Version: archived.version, From: height, To: height}
		}
		extent.Heights++
		extent.From, extent.To = min(extent.From, height), max(extent.To, height)
		extents[archived.version] = extent
	}
	versions := slices.SortedFunc(maps.Values(extents), func(a, b VersionExtent) int {
		return cmp.Compare(a.Version, b.Version)
	})
	return RunSummary{HeightsResolved: len(r.resolved), Versions: versions}
}

// readArchivedBlock asks the archive what it holds at a height.
func (r *Resolver) readArchivedBlock(ctx context.Context, blockNumber int64) (archivedBlock, error) {
	version, found, err := r.archive.HighestVersion(ctx, blockNumber)
	if err != nil {
		return archivedBlock{}, fmt.Errorf("reading the archived versions of block %d: %w", blockNumber, err)
	}
	if !found {
		return archivedBlock{}, fmt.Errorf("block %d in %s: %w", blockNumber, r.archiveName, ErrHeightNotArchived)
	}

	hash, found, err := r.archive.BlockHashAt(ctx, blockNumber, version)
	if err != nil {
		return archivedBlock{}, fmt.Errorf("reading block %d at version %d: %w", blockNumber, version, err)
	}
	if !found {
		return archivedBlock{}, fmt.Errorf("block %d version %d in %s carries no block hash: %w",
			blockNumber, version, r.archiveName, ErrHeightNotArchived)
	}
	// common.HexToHash crops, pads and drops decode errors, so an unchecked string turns
	// a corrupt object into a mismatch — the verdict answered by republishing.
	if !common.IsHexHash(hash) {
		return archivedBlock{}, fmt.Errorf("block %d version %d in %s names %q, which is not a block hash: %w",
			blockNumber, version, r.archiveName, hash, ErrHeightNotArchived)
	}

	return archivedBlock{version: version, hash: common.HexToHash(hash)}, nil
}

// remember answers every later call about a height from the first read: every log of one
// block asks the same question, and the run asks again for the head it seeds at. Only a
// proven height is remembered, so a mismatch the archive is then repaired for is re-read.
func (r *Resolver) remember(blockNumber int64, block archivedBlock) {
	r.resolved[blockNumber] = block
}
