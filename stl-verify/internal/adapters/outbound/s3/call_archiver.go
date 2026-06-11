package s3

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rawsckey"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/klauspost/compress/zstd"
)

const archiveTimestampFormat = "20060102T150405Z"

// CallArchiver writes raw SC call batches to S3 as zstd-compressed JSONL,
// one object per batch with one line per call.
type CallArchiver struct {
	writer  outbound.S3Writer
	bucket  string
	logger  *slog.Logger
	encoder *zstd.Encoder // (*zstd.Encoder).EncodeAll is safe for concurrent use
}

// NewCallArchiver returns an S3-backed CallArchiver writing to bucket.
func NewCallArchiver(writer outbound.S3Writer, bucket string, logger *slog.Logger) *CallArchiver {
	if logger == nil {
		logger = slog.Default()
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		// Construction only fails on an invalid option combination, which is a
		// programming error fixed at compile-not-run time; failing loud at
		// startup beats a nil-encoder panic on the first write.
		panic(fmt.Sprintf("zstd.NewWriter: %v", err))
	}
	return &CallArchiver{writer: writer, bucket: bucket, logger: logger, encoder: encoder}
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

// Archive implements outbound.CallArchiver.
func (a *CallArchiver) Archive(ctx context.Context, record outbound.CallBatchRecord) error {
	if len(record.Calls) == 0 {
		// Mirror the decorator: an empty batch has nothing to replay; writing
		// a phantom object would waste a PUT and clutter listings.
		return nil
	}

	key := rawsckey.Build(record.ChainID, record.BlockNumber, record.BlockVersion, record.Source, batchHash(record.Calls))

	payload, err := a.encode(record)
	if err != nil {
		return fmt.Errorf("encoding call batch: %w", err)
	}

	if _, err := a.writer.WriteFileIfNotExists(ctx, a.bucket, key, bytes.NewReader(payload), false); err != nil {
		return fmt.Errorf("writing call batch archive %s: %w", key, err)
	}
	return nil
}

// batchHash derives the per-batch hash suffix for the S3 key.
// ContractAddress is the EIP-55 checksum hex returned by common.Address.Hex();
// hashing the string form keeps this adapter free of go-ethereum imports and
// is deterministic because the same address always produces the same hex.
func batchHash(calls []outbound.CallEntry) string {
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
			CallData:        "0x" + hex.EncodeToString(c.CallData),
			Success:         c.Success,
			Response:        "0x" + hex.EncodeToString(c.Response),
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
