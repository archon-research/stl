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

// zstdEncoder is shared across all Archive calls. (*zstd.Encoder).EncodeAll is
// safe for concurrent use, so a single encoder avoids a per-call allocation.
var zstdEncoder *zstd.Encoder

func init() {
	var err error
	zstdEncoder, err = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		panic(fmt.Sprintf("init zstd encoder: %v", err))
	}
}

// CallArchiver writes raw SC call records to S3 as zstd-compressed JSONL.
type CallArchiver struct {
	writer outbound.S3Writer
	bucket string
	logger *slog.Logger
}

// NewCallArchiver returns an S3-backed CallArchiver writing to bucket.
func NewCallArchiver(writer outbound.S3Writer, bucket string, logger *slog.Logger) *CallArchiver {
	if logger == nil {
		logger = slog.Default()
	}
	return &CallArchiver{writer: writer, bucket: bucket, logger: logger}
}

// archiveLine is the on-disk JSON shape; bytes are hex-encoded.
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
func (a *CallArchiver) Archive(ctx context.Context, record outbound.CallRecord) error {
	key := rawsckey.Build(record.ChainID, record.BlockNumber, record.BlockVersion, record.Source, record.CallData)

	payload, err := a.encode(record)
	if err != nil {
		return fmt.Errorf("encoding call record: %w", err)
	}

	if _, err := a.writer.WriteFileIfNotExists(ctx, a.bucket, key, bytes.NewReader(payload), false); err != nil {
		return fmt.Errorf("writing call archive %s: %w", key, err)
	}
	return nil
}

// encode serialises one record to a single zstd-compressed JSONL line.
func (a *CallArchiver) encode(record outbound.CallRecord) ([]byte, error) {
	line := archiveLine{
		ChainID:         record.ChainID,
		BlockNumber:     record.BlockNumber,
		BlockVersion:    record.BlockVersion,
		BuildID:         record.BuildID,
		Source:          record.Source,
		Multicaller:     record.Multicaller,
		Timestamp:       record.Timestamp.UTC().Format(archiveTimestampFormat),
		ContractAddress: record.ContractAddress,
		Selector:        record.Selector,
		CallData:        "0x" + hex.EncodeToString(record.CallData),
		Success:         record.Success,
		Response:        "0x" + hex.EncodeToString(record.Response),
	}

	jsonBytes, err := json.Marshal(line)
	if err != nil {
		return nil, fmt.Errorf("marshalling line: %w", err)
	}
	jsonBytes = append(jsonBytes, '\n')

	return zstdEncoder.EncodeAll(jsonBytes, nil), nil
}
