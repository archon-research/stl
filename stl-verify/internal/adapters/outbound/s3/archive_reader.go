package s3

import (
	"context"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/archiveblock"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// archiveObjects is the S3 access this adapter needs: one listing per height, the
// ranged read that names the block a version holds, and the startup probe that
// proves it is allowed to make them.
type archiveObjects interface {
	ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error)
	ReadRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
	ProbeListAccess(ctx context.Context, bucket string) error
}

var _ outbound.ArchiveReader = (*ArchiveReader)(nil)

// ArchiveReader answers what the raw archive holds at a height: one prefix
// listing for the versions, and a ranged read of the top version's first
// kilobytes for the block it holds.
type ArchiveReader struct {
	lister archiveObjects
	bucket string
}

func NewArchiveReader(lister archiveObjects, bucket string) *ArchiveReader {
	return &ArchiveReader{lister: lister, bucket: bucket}
}

// Ping reports whether the archive can be listed at all, so a missing grant or a
// bucket that is not there stops the worker at startup instead of failing every
// height of the first run.
func (r *ArchiveReader) Ping(ctx context.Context) error {
	if err := r.lister.ProbeListAccess(ctx, r.bucket); err != nil {
		return fmt.Errorf("listing s3://%s: %w", r.bucket, err)
	}
	return nil
}

func (r *ArchiveReader) HighestVersion(ctx context.Context, blockNumber int64) (int, bool, error) {
	prefix := s3key.HeightPrefix(blockNumber)

	keys, err := r.lister.ListPrefix(ctx, r.bucket, prefix)
	if err != nil {
		return 0, false, fmt.Errorf("listing s3://%s/%s: %w", r.bucket, prefix, err)
	}

	version, found, err := s3key.HighestVersion(keys, blockNumber)
	if err != nil {
		return 0, false, fmt.Errorf("reading s3://%s/%s: %w", r.bucket, prefix, err)
	}
	return version, found, nil
}

// BlockHashAt reads the hash from the first kilobytes of the version's block
// object, falling back to its receipts. No object is ever downloaded whole.
func (r *ArchiveReader) BlockHashAt(ctx context.Context, blockNumber int64, version int) (string, bool, error) {
	hash, found, err := archiveblock.Hash(ctx, r.lister, r.bucket, blockNumber, version)
	if err != nil {
		return "", false, fmt.Errorf("reading block %d at version %d in s3://%s: %w", blockNumber, version, r.bucket, err)
	}
	return hash, found, nil
}
