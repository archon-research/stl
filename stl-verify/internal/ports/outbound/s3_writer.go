package outbound

import (
	"context"
	"io"
)

// S3Writer defines the interface for writing files to S3.
type S3Writer interface {
	// WriteFile writes content to the specified key in the bucket.
	// The content will be gzip compressed if compressGzip is true.
	WriteFile(ctx context.Context, bucket, key string, content io.Reader, compressGzip bool) error

	// FileExists checks if a file already exists at the given key.
	FileExists(ctx context.Context, bucket, key string) (bool, error)
}
