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

	// StreamFile returns a reader for the file content.
	// The caller is responsible for closing the reader.
	// If the file is gzipped (.gz extension), the reader automatically decompresses.
	StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}
