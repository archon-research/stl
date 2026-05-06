// Package s3backup provides the watcher's inline S3 backup primitive: kick off
// parallel PUTs for a block's artifacts as soon as the data is in memory, then
// await them right before the block is exposed to downstream consumers.
package s3backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	smithy "github.com/aws/smithy-go"

	"github.com/archon-research/stl/stl-verify/internal/pkg/chainexpect"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const tracerName = "github.com/archon-research/stl/stl-verify/internal/services/shared/s3backup"

// Backup encapsulates the per-watcher dependencies needed to write block
// artifacts to S3. One Backup is constructed per watcher and reused for every
// block.
type Backup struct {
	writer      outbound.S3Writer
	bucket      string
	chainID     int64
	expectation chainexpect.Expectation
	metrics     outbound.S3BackupRecorder
	logger      *slog.Logger
}

// Config holds the parameters for NewBackup.
type Config struct {
	Writer  outbound.S3Writer
	Bucket  string
	ChainID int64
	Metrics outbound.S3BackupRecorder
	Logger  *slog.Logger
}

// NewBackup constructs a Backup. Returns an error if the chain is not
// registered in chainexpect — callers should treat this as fatal so a
// misconfigured chain prevents the watcher from starting (see VEC-217).
func NewBackup(cfg Config) (*Backup, error) {
	if cfg.Writer == nil {
		return nil, fmt.Errorf("s3backup: writer is required")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3backup: bucket is required")
	}
	expectation, ok := chainexpect.ForChain(cfg.ChainID)
	if !ok {
		return nil, fmt.Errorf("s3backup: chain %d is not registered in chainexpect — refusing to construct (see VEC-217)", cfg.ChainID)
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Backup{
		writer:      cfg.Writer,
		bucket:      cfg.Bucket,
		chainID:     cfg.ChainID,
		expectation: expectation,
		metrics:     cfg.Metrics,
		logger:      logger.With("component", "s3backup"),
	}, nil
}

// Expectation returns the chainexpect entry the Backup was constructed with.
// Callers can use it to skip fetching artifacts the chain does not require.
func (b *Backup) Expectation() chainexpect.Expectation {
	return b.expectation
}

// Artifacts is the subset of block data that the watcher writes to S3.
// Each non-nil field is written as a separate object.
type Artifacts struct {
	Block    json.RawMessage // required, never empty
	Receipts json.RawMessage // required if Expectation.ExpectReceipts
	Traces   json.RawMessage // required if Expectation.ExpectTraces
	Blobs    json.RawMessage // required if Expectation.ExpectBlobs
}

// Group represents an in-flight parallel S3 backup for a single block. Created
// by Start; the caller must invoke Wait exactly once.
type Group struct {
	eg        *errgroup.Group
	parent    *Backup
	startedAt time.Time
	span      trace.Span
	blockNum  int64
	version   int
	dataTypes []string
}

// Start launches the per-data-type S3 PUTs in parallel goroutines. It returns
// immediately so the caller can do other in-process work concurrently. The
// returned *Group MUST have Wait called on it before the block is exposed to
// downstream consumers (e.g. before SNS publish).
func (b *Backup) Start(ctx context.Context, blockNum int64, version int, art Artifacts) *Group {
	tracer := otel.Tracer(tracerName)
	spanCtx, span := tracer.Start(ctx, "s3backup.group",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.Int64("chain.id", b.chainID),
			attribute.Int64("block.number", blockNum),
			attribute.Int("block.version", version),
		),
	)

	eg, egCtx := errgroup.WithContext(spanCtx)
	g := &Group{
		eg:        eg,
		parent:    b,
		startedAt: time.Now(),
		span:      span,
		blockNum:  blockNum,
		version:   version,
	}

	if err := b.validate(art); err != nil {
		eg.Go(func() error { return err })
		return g
	}

	g.scheduleIfPresent(egCtx, art.Block, s3key.Block)
	if b.expectation.ExpectReceipts {
		g.scheduleIfPresent(egCtx, art.Receipts, s3key.Receipts)
	}
	if b.expectation.ExpectTraces {
		g.scheduleIfPresent(egCtx, art.Traces, s3key.Traces)
	}
	if b.expectation.ExpectBlobs {
		g.scheduleIfPresent(egCtx, art.Blobs, s3key.Blobs)
	}
	span.SetAttributes(attribute.StringSlice("s3backup.data_types", g.dataTypes))

	return g
}

func (b *Backup) validate(art Artifacts) error {
	if len(art.Block) == 0 {
		return fmt.Errorf("s3backup: block payload is required")
	}
	if b.expectation.ExpectReceipts && len(art.Receipts) == 0 {
		return fmt.Errorf("s3backup: receipts payload required for chain %d but missing", b.chainID)
	}
	if b.expectation.ExpectTraces && len(art.Traces) == 0 {
		return fmt.Errorf("s3backup: traces payload required for chain %d but missing", b.chainID)
	}
	if b.expectation.ExpectBlobs && len(art.Blobs) == 0 {
		return fmt.Errorf("s3backup: blobs payload required for chain %d but missing", b.chainID)
	}
	return nil
}

func (g *Group) scheduleIfPresent(ctx context.Context, payload json.RawMessage, dt s3key.DataType) {
	if len(payload) == 0 {
		return
	}
	dataType := string(dt)
	g.dataTypes = append(g.dataTypes, dataType)
	key := s3key.Build(g.blockNum, g.version, dt)
	g.eg.Go(func() error {
		return g.parent.putOne(ctx, dataType, key, g.blockNum, g.version, payload)
	})
}

func (b *Backup) putOne(ctx context.Context, dataType, key string, blockNum int64, version int, payload json.RawMessage) error {
	tracer := otel.Tracer(tracerName)
	ctx, span := tracer.Start(ctx, "s3backup.put",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("data_type", dataType),
			attribute.String("s3.bucket", b.bucket),
			attribute.String("s3.key", key),
			attribute.Int("s3.bytes", len(payload)),
		),
	)
	defer span.End()

	start := time.Now()
	written, err := WriteArtifactByKey(ctx, b.writer, b.bucket, key, payload)
	duration := time.Since(start)

	outcome := "success"
	switch {
	case err != nil:
		outcome = "error"
	case !written:
		outcome = "skipped"
	}

	if b.metrics != nil {
		b.metrics.RecordPutDuration(ctx, dataType, outcome, duration)
		if err == nil && written {
			b.metrics.RecordPutBytes(ctx, dataType, int64(len(payload)))
		}
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "s3 put failed")
		if b.metrics != nil {
			b.metrics.RecordPutError(ctx, dataType, classifyError(err))
		}
		return fmt.Errorf("s3backup: put %s (block=%d version=%d key=%s): %w",
			dataType, blockNum, version, key, err)
	}

	span.SetAttributes(attribute.String("s3.outcome", outcome))
	return nil
}

// Wait blocks until every scheduled PUT has finished. It records the group
// duration metric and ends the parent span before returning. Wait must be
// called exactly once.
func (g *Group) Wait(ctx context.Context) error {
	err := g.eg.Wait()
	duration := time.Since(g.startedAt)

	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if g.parent.metrics != nil {
		g.parent.metrics.RecordGroupDuration(ctx, outcome, duration)
	}

	g.span.SetAttributes(attribute.Int64("s3backup.duration_ms", duration.Milliseconds()))
	if err != nil {
		g.span.RecordError(err)
		g.span.SetStatus(codes.Error, "s3 backup group failed")
		g.parent.logger.Error("S3 backup failed — watcher will return error and let backfill recover",
			"chain_id", g.parent.chainID,
			"block_number", g.blockNum,
			"version", g.version,
			"duration_ms", duration.Milliseconds(),
			"error", err)
	}
	g.span.End()
	return err
}

// classifyError maps an AWS SDK error to a bounded label for metrics. The
// returned value is one of the outbound.S3BackupErrorClass* constants.
func classifyError(err error) string {
	if err == nil {
		return outbound.S3BackupErrorClassOther
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return outbound.S3BackupErrorClassTimeout
	}

	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		switch {
		case code == "RequestThrottled", code == "Throttling", code == "SlowDown",
			strings.Contains(code, "TooManyRequests"):
			return outbound.S3BackupErrorClassThrottle
		case code == "AccessDenied", code == "InvalidAccessKeyId",
			code == "SignatureDoesNotMatch", code == "ExpiredToken":
			return outbound.S3BackupErrorClassAuth
		}
	}

	var respErr interface {
		HTTPStatusCode() int
	}
	if errors.As(err, &respErr) {
		status := respErr.HTTPStatusCode()
		switch {
		case status == http.StatusTooManyRequests:
			return outbound.S3BackupErrorClassThrottle
		case status >= 500:
			return outbound.S3BackupErrorClass5xx
		case status >= 400:
			return outbound.S3BackupErrorClass4xx
		}
	}

	return outbound.S3BackupErrorClassOther
}
