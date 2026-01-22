package outbound

import (
	"context"
	"io"
)

// S3Writer defines the interface for writing files to S3.
type S3Writer interface {
	// WriteFileIfNotExists writes content to the specified key in the bucket only if it does not exist.
	// Returns true if written, false if it already existed.
	WriteFileIfNotExists(ctx context.Context, bucket, key string, content io.Reader, compressGzip bool) (bool, error)

	// FileExists checks if a file already exists at the given key.
	FileExists(ctx context.Context, bucket, key string) (bool, error)
}
