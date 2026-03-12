// Package dataexport transforms and exports block data from the staging backup bucket
// into the flat key format expected by the mock blockchain server's LoadFromS3.
//
// Staging backup key format: {partition}/{blockNumber}_{version}_{dataType}.json.gz (gzipped)
// Mock server key format:    {prefix}/{blockNumber}/{dataType}.json (plain JSON)
package dataexport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// S3Exporter copies block data from a staging backup bucket to a stress-test bucket.
// The reader reads from the staging source (real AWS credentials required).
// The writer writes to the destination bucket (LocalStack for local dev, or real S3 for EKS).
type S3Exporter struct {
	reader     outbound.S3Reader
	writer     outbound.S3Writer
	srcBucket  string
	destBucket string
	destPrefix string
	logger     *slog.Logger
}

// NewS3Exporter creates an exporter that reads from srcBucket and writes to destBucket.
// destPrefix is prepended to all destination keys (e.g. "blocks/chain-1").
func NewS3Exporter(
	reader outbound.S3Reader,
	writer outbound.S3Writer,
	srcBucket, destBucket, destPrefix string,
	logger *slog.Logger,
) *S3Exporter {
	if logger == nil {
		logger = slog.Default()
	}
	return &S3Exporter{
		reader:     reader,
		writer:     writer,
		srcBucket:  srcBucket,
		destBucket: destBucket,
		destPrefix: destPrefix,
		logger:     logger.With("component", "s3-exporter"),
	}
}

// ReadFromBackup downloads all four data types for a single block from the staging backup bucket.
// The staging key format is: {partition}/{blockNumber}_{version}_{dataType}.json.gz
// StreamFile auto-decompresses .gz files, so the returned BlockData contains plain JSON.
func (e *S3Exporter) ReadFromBackup(ctx context.Context, blockNum int64, version int) (outbound.BlockData, error) {
	bd := outbound.BlockData{BlockNumber: blockNum}

	for _, dt := range []s3key.DataType{s3key.Block, s3key.Receipts, s3key.Traces, s3key.Blobs} {
		key := s3key.Build(blockNum, version, dt)
		raw, err := streamAll(ctx, e.reader, e.srcBucket, key)
		if err != nil {
			if dt == s3key.Blobs && isNotFound(err) {
				continue // blobs are optional — not all blocks have them
			}
			return outbound.BlockData{}, fmt.Errorf("reading %s for block %d: %w", dt, blockNum, err)
		}
		switch dt {
		case s3key.Block:
			bd.Block = raw
		case s3key.Receipts:
			bd.Receipts = raw
		case s3key.Traces:
			bd.Traces = raw
		case s3key.Blobs:
			bd.Blobs = raw
		}
	}

	return bd, nil
}

// Upload writes all four data types for a block to the destination bucket.
// The destination key format is: {prefix}/{blockNumber}/{dataType}.json
// Data is stored as plain JSON (no compression).
func (e *S3Exporter) Upload(ctx context.Context, bd outbound.BlockData) error {
	type entry struct {
		dataType string
		raw      []byte
	}
	entries := []entry{
		{"block", bd.Block},
		{"receipts", bd.Receipts},
		{"traces", bd.Traces},
		{"blobs", bd.Blobs},
	}

	for _, ent := range entries {
		if len(ent.raw) == 0 {
			continue
		}
		key := destKey(e.destPrefix, bd.BlockNumber, ent.dataType)
		_, err := e.writer.WriteFileIfNotExists(ctx, e.destBucket, key, bytes.NewReader(ent.raw), false)
		if err != nil {
			return fmt.Errorf("uploading %s for block %d: %w", ent.dataType, bd.BlockNumber, err)
		}
	}

	return nil
}

// ExportRange exports count blocks starting from startBlock, using the given version.
// For each block, it reads from the staging backup and writes to the destination bucket.
func (e *S3Exporter) ExportRange(ctx context.Context, startBlock int64, count int, version int) error {
	for i := range count {
		blockNum := startBlock + int64(i)
		bd, err := e.ReadFromBackup(ctx, blockNum, version)
		if err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
		if err := e.Upload(ctx, bd); err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
		e.logger.Info("exported block",
			"block", blockNum,
			"index", i+1,
			"total", count,
		)
	}
	return nil
}

// destKey returns the destination S3 key for a block data file.
// Format: {prefix}/{blockNumber}/{dataType}.json
func destKey(prefix string, blockNum int64, dataType string) string {
	return fmt.Sprintf("%s/%d/%s.json", prefix, blockNum, dataType)
}

// streamAll reads the full content of an S3 file into a byte slice.
func streamAll(ctx context.Context, reader outbound.S3Reader, bucket, key string) ([]byte, error) {
	rc, err := reader.StreamFile(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// isNotFound reports whether err represents a missing S3 object (NoSuchKey or NotFound).
func isNotFound(err error) bool {
	var noSuchKey *s3types.NoSuchKey
	var notFound *s3types.NotFound
	return errors.As(err, &noSuchKey) || errors.As(err, &notFound)
}
