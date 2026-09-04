// Package s3 provides an S3 adapter for reading files from AWS S3.
package s3

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// s3API defines the subset of S3 operations needed by the Reader.
type s3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

// Compile-time checks that Reader implements the read ports.
var (
	_ outbound.S3Reader      = (*Reader)(nil)
	_ outbound.S3RangeReader = (*Reader)(nil)
)

// Reader implements the S3Reader interface using the AWS SDK.
type Reader struct {
	client s3API
	logger *slog.Logger
}

// NewReader creates a new S3 Reader with the given AWS config.
func NewReader(cfg aws.Config, logger *slog.Logger) *Reader {
	return NewReaderWithOptions(cfg, logger)
}

// NewReaderWithOptions creates a new S3 Reader with optional S3 client options.
// Use this to pass options like UsePathStyle for LocalStack compatibility.
func NewReaderWithOptions(cfg aws.Config, logger *slog.Logger, optFns ...func(*s3.Options)) *Reader {
	if logger == nil {
		logger = slog.Default()
	}
	return &Reader{
		client: s3.NewFromConfig(cfg, optFns...),
		logger: logger,
	}
}

// NewReaderFromEnv creates a Reader honouring AWS_S3_ENDPOINT, the LocalStack
// override every worker needs in kind/dev. Six worker entrypoints previously
// inlined this same block; sparklend-indexer omitted it and its S3 fallback was
// silently unreachable in dev as a result, which is why the convention lives
// here rather than at each call site.
func NewReaderFromEnv(cfg aws.Config, logger *slog.Logger) *Reader {
	return NewReaderWithOptions(cfg, logger, EndpointOptionsFromEnv()...)
}

// EndpointOptionsFromEnv returns the S3 client options implied by
// AWS_S3_ENDPOINT, or nil when it is unset (i.e. real AWS). Path-style
// addressing accompanies a custom endpoint because LocalStack does not serve
// virtual-host-style bucket URLs.
func EndpointOptionsFromEnv() []func(*s3.Options) {
	endpoint := env.Get("AWS_S3_ENDPOINT", "")
	if endpoint == "" {
		return nil
	}
	return []func(*s3.Options){func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	}}
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

// ListPrefix lists all keys in the bucket with the given prefix.
// Returns a slice of key names only (lighter weight than ListFiles).
func (r *Reader) ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(r.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects with prefix %s: %w", prefix, err)
		}

		for _, obj := range page.Contents {
			// A folder marker occupies no version, and a caller folding the
			// listing into archive occupancy refuses a key with no stem.
			if obj.Key == nil || strings.HasSuffix(*obj.Key, "/") {
				continue
			}
			keys = append(keys, *obj.Key)
		}
	}

	return keys, nil
}

// HeadBucket reports whether the bucket exists and this caller may list it: the
// single call a long job makes before starting, rather than discovering the
// answer once per partition.
func (r *Reader) HeadBucket(ctx context.Context, bucket string) error {
	_, err := r.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		return fmt.Errorf("failed to head bucket %s: %w", bucket, err)
	}
	return nil
}

// ProbeListAccess issues the cheapest listing S3 offers, for a caller checking
// at startup that it may list the bucket at all.
func (r *Reader) ProbeListAccess(ctx context.Context, bucket string) error {
	_, err := r.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return fmt.Errorf("failed to list bucket %s: %w", bucket, err)
	}
	return nil
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
			closeErr := result.Body.Close()
			return nil, errors.Join(fmt.Errorf("failed to create gzip reader for %s: %w", key, err), closeErr)
		}
		return &gzipReadCloser{
			gzReader: gzReader,
			body:     result.Body,
		}, nil
	}

	return result.Body, nil
}

// ReadRange returns the requested byte range of an object as stored. Unlike
// StreamFile it never decompresses: a slice of a gzip stream is not one.
func (r *Reader) ReadRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	result, err := r.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	})
	if isMissingObject(err) {
		return nil, fmt.Errorf("%s/%s: %w: %w", bucket, key, outbound.ErrObjectNotFound, err)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get range of object %s/%s: %w", bucket, key, err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read range of object %s/%s: %w", bucket, key, err)
	}
	return data, nil
}

// isAccessDenied tells a refusal from any other failure: S3 answers AccessDenied
// for a missing grant, and a proxy in front of it may refuse with a bare 403.
func isAccessDenied(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDenied", "AllAccessDisabled", "Forbidden":
			return true
		}
	}

	var statusErr interface{ HTTPStatusCode() int }
	return errors.As(err, &statusErr) && statusErr.HTTPStatusCode() == http.StatusForbidden
}

// isMissingObject tells the one answer that means "nothing at this key" from a
// read that failed: S3 models it as NoSuchKey, and a HEAD-shaped 404 as NotFound.
func isMissingObject(err error) bool {
	var noSuchKey *s3types.NoSuchKey
	var notFound *s3types.NotFound
	return errors.As(err, &noSuchKey) || errors.As(err, &notFound)
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
