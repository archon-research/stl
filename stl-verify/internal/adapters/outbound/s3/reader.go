// Package s3 provides an S3 adapter for reading files from AWS S3.
package s3

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// s3API defines the subset of S3 operations needed by the Reader.
type s3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
}

// Compile-time check that Reader implements outbound.S3Reader
var _ outbound.S3Reader = (*Reader)(nil)

// Reader implements the S3Reader interface using the AWS SDK.
type Reader struct {
	client s3API
	logger *slog.Logger
}

// NewReader creates a new S3 Reader with the given AWS config.
func NewReader(cfg aws.Config, logger *slog.Logger) *Reader {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reader{
		client: s3.NewFromConfig(cfg),
		logger: logger,
	}
}

// ListFiles lists all files in the bucket with the given prefix.
func (r *Reader) ListFiles(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error) {
	var files []outbound.S3File

	paginator := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			// Skip nil or incomplete objects
			if obj.Key == nil || obj.Size == nil || obj.LastModified == nil {
				continue
			}
			// Skip directories (keys ending with /)
			if strings.HasSuffix(*obj.Key, "/") {
				continue
			}
			files = append(files, outbound.S3File{
				Key:          *obj.Key,
				Size:         *obj.Size,
				LastModified: *obj.LastModified,
			})
		}
	}

	r.logger.Info("listed S3 files", "bucket", bucket, "prefix", prefix, "count", len(files))
	return files, nil
}

// StreamFile returns a reader for the file content.
// If the file is gzipped (.gz extension), the reader automatically decompresses.
// The caller is responsible for closing the reader.
func (r *Reader) StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	result, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s/%s: %w", bucket, key, err)
	}

	// If gzipped, wrap in gzip reader
	if strings.HasSuffix(key, ".gz") {
		gzReader, err := gzip.NewReader(result.Body)
		if err != nil {
			result.Body.Close()
			return nil, fmt.Errorf("failed to create gzip reader for %s: %w", key, err)
		}
		return &gzipReadCloser{
			gzReader: gzReader,
			body:     result.Body,
		}, nil
	}

	return result.Body, nil
}

// gzipReadCloser wraps a gzip reader and the underlying body for proper cleanup.
type gzipReadCloser struct {
	gzReader *gzip.Reader
	body     io.ReadCloser
}

func (g *gzipReadCloser) Read(p []byte) (int, error) {
	return g.gzReader.Read(p)
}

func (g *gzipReadCloser) Close() error {
	gzErr := g.gzReader.Close()
	bodyErr := g.body.Close()
	if gzErr != nil {
		return gzErr
	}
	return bodyErr
}
