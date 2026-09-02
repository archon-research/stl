package s3

import (
	"context"
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// bucketLister is the S3 access this adapter needs: one listing per height, and
// the startup probe that proves it is allowed to make them.
type bucketLister interface {
	ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error)
	ProbeListAccess(ctx context.Context, bucket string) error
}

var _ outbound.ArchiveVersionReader = (*ArchiveVersionReader)(nil)

// ArchiveVersionReader answers what the raw archive holds at a height from one
// prefix listing per height.
type ArchiveVersionReader struct {
	lister bucketLister
	bucket string
}

func NewArchiveVersionReader(lister bucketLister, bucket string) *ArchiveVersionReader {
	return &ArchiveVersionReader{lister: lister, bucket: bucket}
}

// Ping reports whether the archive can be listed at all, so a missing grant or a
// bucket that is not there stops the worker at startup instead of failing every
// height of the first run.
func (r *ArchiveVersionReader) Ping(ctx context.Context) error {
	if err := r.lister.ProbeListAccess(ctx, r.bucket); err != nil {
		return fmt.Errorf("listing s3://%s: %w", r.bucket, err)
	}
	return nil
}

func (r *ArchiveVersionReader) HighestVersion(ctx context.Context, blockNumber int64) (int, bool, error) {
	prefix := s3key.HeightPrefix(blockNumber)

	keys, err := r.lister.ListPrefix(ctx, r.bucket, prefix)
	if err != nil {
		return 0, false, fmt.Errorf("listing s3://%s/%s: %w", r.bucket, prefix, err)
	}

	version, found := s3key.HighestVersion(keys, blockNumber)
	return version, found, nil
}
