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

// S3Overwriter is a deliberately-narrow port for callers that legitimately need
// unconditional PutObject (e.g. one-shot cleanup tools healing corrupt keys).
// Production services that should never overwrite must depend on S3Writer
// instead so the dangerous method is physically out of reach.
type S3Overwriter interface {
	// WriteFile writes content to the specified key in the bucket, overwriting
	// any existing object.
	WriteFile(ctx context.Context, bucket, key string, content io.Reader, compressGzip bool) error
}
