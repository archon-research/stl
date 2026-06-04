package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/klauspost/compress/zstd"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rawsckey"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// CallArchiver is the S3 implementation of outbound.CallArchiver (VEC-81).
// Each batch is serialised to newline-delimited JSON, compressed with zstd,
// and written under the version-aware key produced by rawsckey.Build.
//
// Idempotency: keys include a wall-clock UTC timestamp, so retries naturally
// produce distinct keys. PutObject is issued with IfNoneMatch="*" so the
// extremely rare collision (same source, same chain/block/bv/build at the same
// second) is also safe.
//
// Fail-open: any error during serialise/compress/PUT is logged and swallowed.
// The indexer's call has already succeeded by the time ArchiveBatch runs and
// must not be reverted because of a transient S3 issue. The only error return
// from ArchiveBatch is a programmer error (nil receiver / empty bucket /
// missing required meta), which we still want to surface loudly.
//
// Lifecycle: object retention (e.g. 365-day Glacier transition) is configured
// on the bucket out-of-band — TODO confirm policy with the data team.
type CallArchiver struct {
	client  s3CallArchiveAPI
	bucket  string
	logger  *slog.Logger
	encoder *zstd.Encoder
}

// s3CallArchiveAPI is the subset of S3 client behaviour the archiver needs.
type s3CallArchiveAPI interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// Compile-time check.
var _ outbound.CallArchiver = (*CallArchiver)(nil)

// NewCallArchiver wires a CallArchiver around the given AWS config. bucket is
// the single shared raw-sc-calls bucket (not chain-scoped — see VEC-81).
func NewCallArchiver(cfg aws.Config, bucket string, logger *slog.Logger, optFns ...func(*s3.Options)) (*CallArchiver, error) {
	return newCallArchiverWithClient(s3.NewFromConfig(cfg, optFns...), bucket, logger)
}

// NewCallArchiverWithHTTPClient is the HTTP-client-overriding variant used by
// services that share a tuned http.Client across S3 adapters.
func NewCallArchiverWithHTTPClient(cfg aws.Config, httpClient *http.Client, bucket string, logger *slog.Logger) (*CallArchiver, error) {
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.HTTPClient = httpClient
	})
	return newCallArchiverWithClient(client, bucket, logger)
}

func newCallArchiverWithClient(client s3CallArchiveAPI, bucket string, logger *slog.Logger) (*CallArchiver, error) {
	if bucket == "" {
		return nil, errors.New("s3 call archiver: bucket is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	// SpeedDefault keeps CPU cost low — these archives are write-once,
	// read-rarely audit artefacts. We re-use one encoder across batches.
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("init zstd encoder: %w", err)
	}
	return &CallArchiver{
		client:  client,
		bucket:  bucket,
		logger:  logger.With("component", "s3-call-archiver", "bucket", bucket),
		encoder: enc,
	}, nil
}

// Close releases the underlying zstd encoder.
func (a *CallArchiver) Close() error {
	if a.encoder == nil {
		return nil
	}
	return a.encoder.Close()
}

// ArchiveBatch serialises, compresses and uploads the batch. Errors are
// fail-open (logged + swallowed) per port contract.
func (a *CallArchiver) ArchiveBatch(ctx context.Context, batch outbound.CallBatch) error {
	if batch.Source == "" {
		return errors.New("s3 call archiver: batch.Source is required")
	}
	if len(batch.Records) == 0 {
		return nil // nothing to archive
	}

	payload, err := a.encodeBatch(batch)
	if err != nil {
		a.logArchiveFailure(ctx, batch, "encode", err)
		return nil
	}

	key := rawsckey.Build(
		batch.ChainID,
		batch.BlockNumber,
		batch.BlockVersion,
		batch.BuildID,
		batch.Source,
		dominantMethod(batch.Records),
		batch.Timestamp,
	)

	if err := a.put(ctx, key, payload); err != nil {
		a.logArchiveFailure(ctx, batch, "put", err)
		return nil
	}
	return nil
}

// encodeBatch serialises every record as one JSON object per line, then
// zstd-compresses the result. Returns the compressed bytes.
func (a *CallArchiver) encodeBatch(batch outbound.CallBatch) ([]byte, error) {
	var jsonl bytes.Buffer
	enc := json.NewEncoder(&jsonl)
	enc.SetEscapeHTML(false)
	for i := range batch.Records {
		line := buildLine(batch, &batch.Records[i])
		if err := enc.Encode(line); err != nil {
			return nil, fmt.Errorf("encode record %d: %w", i, err)
		}
	}
	return a.encoder.EncodeAll(jsonl.Bytes(), make([]byte, 0, jsonl.Len()/2)), nil
}

// archiveLine is the on-disk JSONL schema. Field order is preserved for
// downstream tooling (Athena / DuckDB) friendliness.
type archiveLine struct {
	ChainID         int64  `json:"chain_id"`
	BlockNumber     int64  `json:"block_number"`
	BlockVersion    int    `json:"block_version"`
	BuildID         int    `json:"build_id"`
	Source          string `json:"source"`
	Timestamp       string `json:"timestamp"`
	Multicaller     string `json:"multicaller"`
	ContractAddress string `json:"contract_address"`
	Method          string `json:"method,omitempty"`
	CallData        string `json:"call_data"`
	Success         bool   `json:"success"`
	Response        string `json:"response"`
	Decoded         any    `json:"decoded,omitempty"`
}

func buildLine(batch outbound.CallBatch, rec *outbound.CallRecord) archiveLine {
	return archiveLine{
		ChainID:         batch.ChainID,
		BlockNumber:     batch.BlockNumber,
		BlockVersion:    batch.BlockVersion,
		BuildID:         batch.BuildID,
		Source:          batch.Source,
		Timestamp:       batch.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z"),
		Multicaller:     batch.Multicaller.Hex(),
		ContractAddress: rec.ContractAddress.Hex(),
		Method:          rec.Method,
		CallData:        "0x" + hex.EncodeToString(rec.CallData),
		Success:         rec.Success,
		Response:        "0x" + hex.EncodeToString(rec.Response),
		Decoded:         rec.Decoded,
	}
}

// put uploads payload with IfNoneMatch="*" so accidental key collisions are
// reported as 412 (treated as a benign no-op).
func (a *CallArchiver) put(ctx context.Context, key string, payload []byte) error {
	_, err := a.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:          aws.String(a.bucket),
		Key:             aws.String(key),
		Body:            bytes.NewReader(payload),
		ContentType:     aws.String("application/x-ndjson"),
		ContentEncoding: aws.String("zstd"),
		IfNoneMatch:     aws.String("*"),
	})
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "PreconditionFailed" || apiErr.ErrorCode() == "412") {
		return nil
	}
	return err
}

func (a *CallArchiver) logArchiveFailure(ctx context.Context, batch outbound.CallBatch, stage string, err error) {
	a.logger.WarnContext(ctx, "raw SC-call archive failed (fail-open: ingest continues)",
		"stage", stage,
		"chain_id", batch.ChainID,
		"block_number", batch.BlockNumber,
		"block_version", batch.BlockVersion,
		"build_id", batch.BuildID,
		"source", batch.Source,
		"records", len(batch.Records),
		"error", err,
	)
}

// dominantMethod returns the single shared method name when the entire batch
// targets one method, otherwise rawsckey.MixedMethod. Per-record methods are
// still recorded inside the JSONL.
func dominantMethod(records []outbound.CallRecord) string {
	if len(records) == 0 {
		return rawsckey.MixedMethod
	}
	first := records[0].Method
	if first == "" {
		// No decoder supplied — uniformly unknown.
		return rawsckey.MixedMethod
	}
	for i := 1; i < len(records); i++ {
		if records[i].Method != first {
			return rawsckey.MixedMethod
		}
	}
	return first
}
