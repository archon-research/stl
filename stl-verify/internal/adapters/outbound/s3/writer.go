// Package s3 provides S3 adapters for reading and writing files to AWS S3.
package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// s3WriterAPI defines the subset of S3 operations needed by the Writer.
type s3WriterAPI interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

// Compile-time check that Writer implements outbound.S3Writer
var _ outbound.S3Writer = (*Writer)(nil)

// Writer implements the S3Writer interface using the AWS SDK.
type Writer struct {
	client s3WriterAPI
	logger *slog.Logger
}

// NewWriter creates a new S3 Writer with the given AWS config.
func NewWriter(cfg aws.Config, logger *slog.Logger) *Writer {
	return NewWriterWithOptions(cfg, logger)
}

// NewWriterWithOptions creates a new S3 Writer with optional S3 client options.
func NewWriterWithOptions(cfg aws.Config, logger *slog.Logger, optFns ...func(*s3.Options)) *Writer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Writer{
		client: s3.NewFromConfig(cfg, optFns...),
		logger: logger.With("component", "s3-writer"),
	}
}

// NewWriterWithHTTPClient creates a new S3 Writer with a custom HTTP client.
// This is useful for controlling connection pooling and timeouts.
func NewWriterWithHTTPClient(cfg aws.Config, httpClient *http.Client, logger *slog.Logger) *Writer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Writer{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.HTTPClient = httpClient
		}),
		logger: logger.With("component", "s3-writer"),
	}
}

// prepareBody handles optional gzip compression for the upload body.
func (w *Writer) prepareBody(content io.Reader, compressGzip bool) (io.Reader, *string, error) {
	if !compressGzip {
		return content, nil, nil
	}

	// Read all content and compress
	data, err := io.ReadAll(content)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read content: %w", err)
	}

	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	if _, err := gzWriter.Write(data); err != nil {
		return nil, nil, fmt.Errorf("failed to compress content: %w", err)
	}
	if err := gzWriter.Close(); err != nil {
		return nil, nil, fmt.Errorf("failed to close gzip writer: %w", err)
	}

	return bytes.NewReader(buf.Bytes()), aws.String("gzip"), nil
}

// WriteFileIfNotExists writes content to the specified key in the bucket only if it does not exist.
func (w *Writer) WriteFileIfNotExists(ctx context.Context, bucket, key string, content io.Reader, compressGzip bool) (bool, error) {
	body, contentEncoding, err := w.prepareBody(content, compressGzip)
	if err != nil {
		return false, err
	}

	input := &s3.PutObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		Body:            body,
		ContentType:     aws.String("application/json"),
		ContentEncoding: contentEncoding,
		IfNoneMatch:     aws.String("*"),
	}

	_, err = w.client.PutObject(ctx, input)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "412") {
			return false, nil
		}
		return false, fmt.Errorf("failed to write to S3: %w", err)
	}

	w.logger.Debug("wrote file to S3 (new)", "bucket", bucket, "key", key, "compressed", compressGzip)
	return true, nil
}

// FileExists checks if a file already exists at the given key.
func (w *Writer) FileExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := w.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if the error is "not found"
		var notFound *types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		// Also check for NoSuchKey
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check if file exists: %w", err)
	}
	return true, nil
}
