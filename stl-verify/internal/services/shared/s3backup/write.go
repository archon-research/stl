package s3backup

import (
	"bytes"
	"context"
	"encoding/json"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// WriteArtifact writes a single block artifact (block / receipts / traces /
// blobs) to S3 with the canonical key derived from blockNum, version, and
// dataType. The underlying call is WriteFileIfNotExists with gzip enabled, so
// re-runs are no-ops and the function is safe to call from multiple writers
// concurrently. Returns true when the object was newly written, false when an
// object already existed at the key.
//
// This helper is the single S3 write call used by every component that backs
// up raw block data — historically the raw_data_backup worker, and after
// VEC-216 the watcher's inline backup goroutines as well.
func WriteArtifact(ctx context.Context, w outbound.S3Writer, bucket string, blockNum int64, version int, dt s3key.DataType, payload json.RawMessage) (bool, error) {
	return WriteArtifactByKey(ctx, w, bucket, s3key.Build(blockNum, version, dt), payload)
}

// WriteArtifactByKey is WriteArtifact's lower-level variant for callers that
// have already computed the S3 key (e.g. inside the parallel-PUT goroutines,
// where pre-computing the key allows it to appear on the trace span before
// the write begins).
func WriteArtifactByKey(ctx context.Context, w outbound.S3Writer, bucket, key string, payload json.RawMessage) (bool, error) {
	return w.WriteFileIfNotExists(ctx, bucket, key, bytes.NewReader(payload), true)
}
