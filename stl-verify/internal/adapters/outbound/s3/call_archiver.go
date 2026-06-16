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

// NewCallArchiver returns an S3-backed CallArchiver writing to bucket. It errors
// if the zstd encoder cannot be constructed; the caller bubbles that up to main
// rather than the adapter panicking.
func NewCallArchiver(writer outbound.S3Writer, bucket string, logger *slog.Logger) (*CallArchiver, error) {
	if logger == nil {
		logger = slog.Default()
	}
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, fmt.Errorf("creating zstd encoder: %w", err)
	}
	return &CallArchiver{writer: writer, bucket: bucket, logger: logger, encoder: encoder}, nil
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
	return nil
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
