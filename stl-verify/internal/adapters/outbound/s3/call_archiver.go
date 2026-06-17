package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rawsckey"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/klauspost/compress/zstd"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const archiveTimestampFormat = "20060102T150405Z"

// CallArchiver writes raw SC call batches to S3 as zstd-compressed JSONL,
// one object per batch with one line per call.
type CallArchiver struct {
	writer     outbound.S3Writer
	bucket     string
	chainName  string
	logger     *slog.Logger
	encoder    *zstd.Encoder // (*zstd.Encoder).EncodeAll is safe for concurrent use
	objectSize metric.Int64Histogram
}

// archiveLine is the on-disk JSON shape for one call within a batch; bytes are
// hex-encoded. Each batch object contains one such line per call.
type archiveLine struct {
	ChainID         int64  `json:"chain_id"`
	BlockNumber     int64  `json:"block_number"`
	BlockVersion    int    `json:"block_version"`
	BuildID         int64  `json:"build_id"`
	Source          string `json:"source"`
	Multicaller     string `json:"multicaller"`
	Timestamp       string `json:"timestamp"`
	ContractAddress string `json:"contract_address"`
	Selector        string `json:"selector"`
	CallData        string `json:"call_data"`
	Success         bool   `json:"success"`
	Response        string `json:"response"`
}

// NewCallArchiver returns an S3-backed CallArchiver writing to bucket. chainName
// is the resolved chain name used as the `chain` metric label. A nil mp uses the
// global meter provider. It errors if the zstd encoder cannot be constructed; the
// caller bubbles that up to main rather than the adapter panicking.
func NewCallArchiver(writer outbound.S3Writer, bucket, chainName string, logger *slog.Logger, mp metric.MeterProvider) (*CallArchiver, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("creating zstd encoder: %w", err)
	}
	objectSize, err := mp.
		Meter("github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3").
		Int64Histogram(
			"archive.object_size",
			metric.WithDescription("Compressed size in bytes of archived raw SC call batch objects"),
			metric.WithUnit("By"),
			metric.WithExplicitBucketBoundaries(64, 256, 1024, 4096, 16384, 65536, 262144, 1048576),
		)
	if err != nil {
		// Metrics must never break the archiving hot path, so a histogram that
		// fails to construct is logged and left nil (recordObjectSize no-ops on
		// nil) rather than failing NewCallArchiver.
		logger.Error("building archive.object_size histogram; archive size metric disabled", "error", err)
	}
	return &CallArchiver{writer: writer, bucket: bucket, chainName: chainName, logger: logger, encoder: encoder, objectSize: objectSize}, nil
}

// Archive implements outbound.CallArchiver.
func (a *CallArchiver) Archive(ctx context.Context, record outbound.CallBatchRecord) error {
	if len(record.Calls) == 0 {
		// Mirror the decorator: an empty batch has nothing to replay; writing
		// a phantom object would waste a PUT and clutter listings.
		return nil
	}

	hash, err := batchHash(record.Calls)
	if err != nil {
		return fmt.Errorf("hashing call batch: %w", err)
	}
	key := rawsckey.Build(record.ChainID, record.BlockNumber, record.BlockVersion, record.Source, hash)

	payload, err := a.encode(record)
	if err != nil {
		return fmt.Errorf("encoding call batch: %w", err)
	}

	if _, err := a.writer.WriteFileIfNotExists(ctx, a.bucket, key, bytes.NewReader(payload), false); err != nil {
		return fmt.Errorf("writing call batch archive %s: %w", key, err)
	}
	a.recordObjectSize(ctx, record.Source, len(payload))
	return nil
}

// recordObjectSize observes the compressed object size in bytes. A nil histogram
// (construction failed) is a no-op. Labelled by chain (fixed per archiver) and
// the batch's source.
func (a *CallArchiver) recordObjectSize(ctx context.Context, source string, size int) {
	if a.objectSize == nil {
		return
	}
	a.objectSize.Record(ctx, int64(size), metric.WithAttributes(
		attribute.String("chain", a.chainName),
		attribute.String("source", source),
	))
}

// batchHash derives the per-batch hash suffix for the S3 key.
// ContractAddress is the EIP-55 checksum hex returned by common.Address.Hex();
// hashing the string form is deterministic because the same address always
// produces the same hex.
func batchHash(calls []outbound.CallEntry) (string, error) {
	inputs := make([]rawsckey.BatchHashInput, len(calls))
	for i := range calls {
		inputs[i] = rawsckey.BatchHashInput{
			Target:   []byte(calls[i].ContractAddress),
			CallData: calls[i].CallData,
		}
	}
	return rawsckey.BatchHash(inputs)
}

// encode serialises a batch to JSONL (one line per call) and zstd-compresses
// the whole frame in a single pass.
func (a *CallArchiver) encode(record outbound.CallBatchRecord) ([]byte, error) {
	ts := record.Timestamp.UTC().Format(archiveTimestampFormat)
	var buf bytes.Buffer
	for i := range record.Calls {
		c := &record.Calls[i]
		line := archiveLine{
			ChainID:         record.ChainID,
			BlockNumber:     record.BlockNumber,
			BlockVersion:    record.BlockVersion,
			BuildID:         record.BuildID,
			Source:          record.Source,
			Multicaller:     record.Multicaller,
			Timestamp:       ts,
			ContractAddress: c.ContractAddress,
			Selector:        c.Selector,
			CallData:        hexutil.Encode(c.CallData),
			Success:         c.Success,
			Response:        hexutil.Encode(c.Response),
		}
		jsonBytes, err := json.Marshal(line)
		if err != nil {
			return nil, fmt.Errorf("marshalling line %d: %w", i, err)
		}
		buf.Write(jsonBytes)
		buf.WriteByte('\n')
	}

	return a.encoder.EncodeAll(buf.Bytes(), nil), nil
}
