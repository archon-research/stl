package s3

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/archiveblock"
	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// versionObjects is the S3 access this adapter needs: one listing per partition, and the
// ranged read that names the block a version holds.
type versionObjects interface {
	ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error)
	ReadRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}

var _ outbound.BlockVersionResolver = (*BlockVersionResolver)(nil)

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

// BlockVersionResolver answers which version of a height the raw archive holds, and
// proves it holds the block being asked about. The rule is the maintainer-set
// highest-version-wins one every read of the raw buckets uses — stated in full on the
// morpho-vault-backfill's listHighestVersionReceipts, which resolves the same version
// from the key it replays.
type BlockVersionResolver struct {
	objects versionObjects
	bucket  string

	mu     sync.Mutex
	listed *partitionListing
}

func NewBlockVersionResolver(objects versionObjects, bucket string) *BlockVersionResolver {
	return &BlockVersionResolver{objects: objects, bucket: bucket}
}

// partitionListing is the one partition the resolver keeps in hand: the top version its
// listing named per height, and the block hash each resolved height turned out to hold.
// A replay walks blocks in ascending order, so entering the next partition supersedes it
// — which is what keeps a two-million-block sweep to one listing per partition without
// accumulating a thousand heights of every partition it has passed.
type partitionListing struct {
	prefix   string
	versions map[int64]int
	hashes   map[int64]string
}

func (r *BlockVersionResolver) ResolveBlockVersion(ctx context.Context, blockNumber int64, blockHash common.Hash) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	listing, err := r.partitionOf(ctx, blockNumber)
	if err != nil {
		return 0, err
	}
	version, archived := listing.versions[blockNumber]
	if !archived {
		return 0, fmt.Errorf("block %d in s3://%s: %w", blockNumber, r.bucket, ErrHeightNotArchived)
	}
	archivedHash, err := r.hashAt(ctx, listing, blockNumber, version)
	if err != nil {
		return 0, err
	}
	if common.HexToHash(archivedHash) != blockHash {
		return 0, fmt.Errorf("block %d version %d in s3://%s holds %s, replaying %s: %w",
			blockNumber, version, r.bucket, archivedHash, blockHash.Hex(), ErrArchivedBlockMismatch)
	}
	return version, nil
}

// partitionOf returns the listing covering blockNumber, fetching it when the resolver is
// holding another partition.
func (r *BlockVersionResolver) partitionOf(ctx context.Context, blockNumber int64) (*partitionListing, error) {
	prefix := s3key.PartitionPrefix(partition.GetPartition(blockNumber))
	if r.listed != nil && r.listed.prefix == prefix {
		return r.listed, nil
	}

	keys, err := r.objects.ListPrefix(ctx, r.bucket, prefix)
	if err != nil {
		return nil, fmt.Errorf("listing s3://%s/%s: %w", r.bucket, prefix, err)
	}
	occupancies, err := s3key.Occupancies(keys)
	if err != nil {
		return nil, fmt.Errorf("reading s3://%s/%s: %w", r.bucket, prefix, err)
	}

	versions := make(map[int64]int, len(occupancies))
	for height, occupancy := range occupancies {
		versions[height] = occupancy.Version
	}
	r.listed = &partitionListing{prefix: prefix, versions: versions, hashes: make(map[int64]string)}
	return r.listed, nil
}

// hashAt returns the block hash the height's top version holds, reading it once per
// height: every log of one block asks the same question.
func (r *BlockVersionResolver) hashAt(ctx context.Context, listing *partitionListing, blockNumber int64, version int) (string, error) {
	if hash, cached := listing.hashes[blockNumber]; cached {
		return hash, nil
	}

	hash, found, err := archiveblock.Hash(ctx, r.objects, r.bucket, blockNumber, version)
	if err != nil {
		return "", fmt.Errorf("reading block %d at version %d in s3://%s: %w", blockNumber, version, r.bucket, err)
	}
	if !found {
		return "", fmt.Errorf("block %d version %d in s3://%s carries no block hash: %w",
			blockNumber, version, r.bucket, ErrHeightNotArchived)
	}
	listing.hashes[blockNumber] = hash
	return hash, nil
}
