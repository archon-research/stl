package outbound

import (
	"context"
	"io"
	"time"
)

// S3File represents metadata about a file in S3.
type S3File struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// S3Reader defines the interface for reading files from S3.
type S3Reader interface {
	// ListFiles lists all files in the bucket with the given prefix.
	ListFiles(ctx context.Context, bucket, prefix string) ([]S3File, error)

	// ListPrefix lists all keys in the bucket with the given prefix.
	// Returns a slice of key names only (lighter weight than ListFiles).
	ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error)

	// StreamFile returns a reader for the file content.
	// The caller is responsible for closing the reader.
	// If the file is gzipped (.gz extension), the reader automatically decompresses.
	StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

// S3RangeReader reads part of an object. It is separate from S3Reader so only
// the callers that need a partial read take the dependency.
type S3RangeReader interface {
	// ReadRange returns bytes [start,end] of the object exactly as stored, so a
	// gzipped object comes back compressed: a range of a gzip stream cannot be
	// decompressed on its own.
	ReadRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error)
}
