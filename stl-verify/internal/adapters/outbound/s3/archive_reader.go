package s3

import (
	"context"
	"errors"
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
	ProbeListAccess(ctx context.Context, bucket, prefix string) error
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

// probePrefix is what the startup probes work under: a real partition prefix, so
// a grant conditioned on one covers them. probeObjectKey names a key nothing can
// ever be stored at — its absence is the answer the read expects.
var (
	probePrefix    = s3key.HeightPrefix(0)
	probeObjectKey = probePrefix + "startup-probe"
)

// Ping reports whether the archive can be used at all: listed, and read. A
// missing grant or a bucket that is not there stops the worker at startup instead
// of failing every height of the first run.
func (r *ArchiveReader) Ping(ctx context.Context) error {
	if err := r.lister.ProbeListAccess(ctx, r.bucket, probePrefix); err != nil {
		return fmt.Errorf("listing s3://%s: this pod needs s3:ListBucket on that bucket: %w", r.bucket, err)
	}
	return r.probeObjectRead(ctx)
}

// probeObjectRead reads one byte of a key that does not exist. Listing proves
// nothing about reading, and deciding whether a height is already canonical reads
// objects — so a bucket this pod may list but not read would fail every archived
// height of the first run, half an hour in.
func (r *ArchiveReader) probeObjectRead(ctx context.Context) error {
	_, err := r.lister.ReadRange(ctx, r.bucket, probeObjectKey, 0, 0)
	switch {
	case err == nil, errors.Is(err, outbound.ErrObjectNotFound):
		return nil
	case isAccessDenied(err):
		return fmt.Errorf("reading s3://%s/%s: this pod needs s3:GetObject on that bucket: %w",
			r.bucket, probeObjectKey, err)
	default:
		return fmt.Errorf("reading s3://%s/%s: %w", r.bucket, probeObjectKey, err)
	}
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
