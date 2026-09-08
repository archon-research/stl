package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
)

var (
	canonicalHash = common.HexToHash("0x4d1c1a52b1f5e5a0c6f0b0a0d9e8c7b6a594837261504f3e2d1c0b9a8f7e6d5c")
	orphanedHash  = common.HexToHash("0x9a8f7e6d5c4b3a2910ff0e1d2c3b4a5968778695a4b3c2d1e0f9a8b7c6d5e4f3")
)

// storedArchive is a raw bucket as the backup worker leaves it: objects under their real
// keys, listings filtered by prefix, and a count of the listings a resolver asked for.
type storedArchive struct {
	objects  map[string][]byte
	listings []string
}

func newStoredArchive() *storedArchive {
	return &storedArchive{objects: map[string][]byte{}}
}

// holdsBlock archives one version of a height as a block object naming hash.
func (a *storedArchive) holdsBlock(t *testing.T, blockNumber int64, version int, hash common.Hash) *storedArchive {
	t.Helper()
	a.objects[s3key.Build(blockNumber, version, s3key.Block)] = gzippedBlock(t, hash.Hex())
	return a
}

// holdsOnlyTraces archives a version that occupies its slot without naming any block.
func (a *storedArchive) holdsOnlyTraces(t *testing.T, blockNumber int64, version int) *storedArchive {
	t.Helper()
	a.objects[s3key.Build(blockNumber, version, s3key.Traces)] = gzipBytes(t, []byte(`[]`))
	return a
}

func (a *storedArchive) resolver() *BlockVersionResolver {
	mock := &mockS3API{
		listObjectsV2Func: func(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			prefix := aws.ToString(params.Prefix)
			a.listings = append(a.listings, prefix)
			var contents []types.Object
			for key := range a.objects {
				if strings.HasPrefix(key, prefix) {
					contents = append(contents, types.Object{Key: aws.String(key)})
				}
			}
			return &s3.ListObjectsV2Output{Contents: contents}, nil
		},
		getObjectFunc: func(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
			body, ok := a.objects[aws.ToString(params.Key)]
			if !ok {
				return nil, &types.NoSuchKey{}
			}
			return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body))}, nil
		},
	}
	reader := &Reader{client: mock, logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	return NewBlockVersionResolver(reader, archiveBucket)
}

// The version a correction wrote is the one live indexing stamped its rows with, so the
// highest version the archive holds is the one a replay of that block must carry — the
// same rule the morpho-vault-backfill reads off the key it replays.
func TestBlockVersionResolver_ResolvesTheHighestVersionTheArchiveHolds(t *testing.T) {
	archive := newStoredArchive().
		holdsBlock(t, archiveHeight, 0, orphanedHash).
		holdsBlock(t, archiveHeight, 1, canonicalHash)

	version, err := archive.resolver().ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

	if err != nil {
		t.Fatalf("ResolveBlockVersion: %v", err)
	}
	if version != 1 {
		t.Errorf("version = %d, want 1: the correction is what the live row carries", version)
	}
}

// The ARCT-379 hole shape: the archive kept an orphaned fork and never received the
// canonical block. Its version speaks for the fork, so stamping rows with it would file
// the replayed history under a block that never happened.
func TestBlockVersionResolver_StopsWhenTheArchiveHoldsAnotherBlock(t *testing.T) {
	archive := newStoredArchive().holdsBlock(t, archiveHeight, 0, orphanedHash)

	_, err := archive.resolver().ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

	if !errors.Is(err, ErrArchivedBlockMismatch) {
		t.Fatalf("error = %v, want ErrArchivedBlockMismatch", err)
	}
	for _, want := range []string{"25395651", orphanedHash.Hex(), canonicalHash.Hex()} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to name %q", err, want)
		}
	}
}

func TestBlockVersionResolver_StopsAtAHeightTheArchiveCannotAnswerFor(t *testing.T) {
	tests := []struct {
		name    string
		archive func(*testing.T) *storedArchive
	}{
		{
			name:    "a height the archive never received",
			archive: func(*testing.T) *storedArchive { return newStoredArchive() },
		},
		{
			name: "a version occupied by objects that name no block",
			archive: func(t *testing.T) *storedArchive {
				return newStoredArchive().holdsOnlyTraces(t, archiveHeight, 0)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.archive(t).resolver().ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

			if !errors.Is(err, ErrHeightNotArchived) {
				t.Fatalf("error = %v, want ErrHeightNotArchived", err)
			}
			if !strings.Contains(err.Error(), "25395651") {
				t.Errorf("error = %v, want it to name the height", err)
			}
		})
	}
}

// A sweep resolves one version per log, and a partition holds a thousand heights: one
// listing per partition is what keeps that from being a listing per log.
func TestBlockVersionResolver_ListsEachPartitionOnce(t *testing.T) {
	const neighbour = archiveHeight + 1
	const nextPartition = int64(25396000)

	archive := newStoredArchive().
		holdsBlock(t, archiveHeight, 0, canonicalHash).
		holdsBlock(t, neighbour, 0, canonicalHash).
		holdsBlock(t, nextPartition, 0, canonicalHash)
	resolver := archive.resolver()

	for _, height := range []int64{archiveHeight, archiveHeight, neighbour, nextPartition} {
		if _, err := resolver.ResolveBlockVersion(context.Background(), height, canonicalHash); err != nil {
			t.Fatalf("ResolveBlockVersion(%d): %v", height, err)
		}
	}

	want := []string{"25395000-25395999/", "25396000-25396999/"}
	if len(archive.listings) != len(want) || archive.listings[0] != want[0] || archive.listings[1] != want[1] {
		t.Errorf("listings = %v, want %v", archive.listings, want)
	}
}
