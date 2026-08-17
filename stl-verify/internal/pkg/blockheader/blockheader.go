// Package blockheader decodes the authoritative on-chain block-header timestamp from an
// archived raw-block JSON object. The raw_data_backup worker archives each block as
// {partition}/{block}_{version}_block.json.gz; the header's "timestamp" field is the exact
// on-chain time, hex-encoded seconds since the Unix epoch (e.g. "0x67c00000").
//
// It lives in internal/pkg so both adapters and services can import it without violating
// hexagonal architecture: like internal/pkg/blockchain, it depends only on the outbound port
// interface (S3Reader), never an adapter. It centralises a decode that was previously copied
// across block_meta_loader, sparklend_backfill and oracle_price_worker.
package blockheader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ParseTimestamp decodes the on-chain header timestamp from a raw-block JSON payload. The
// payload must carry a hex "timestamp" field; an empty or unparseable field is an error rather
// than a silent zero time.
func ParseTimestamp(data []byte) (time.Time, error) {
	var hdr struct {
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(data, &hdr); err != nil {
		return time.Time{}, fmt.Errorf("decode block header: %w", err)
	}
	if hdr.Timestamp == "" {
		return time.Time{}, fmt.Errorf("block header has no timestamp field")
	}
	sec, err := hexutil.ParseInt64(hdr.Timestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse block timestamp %q: %w", hdr.Timestamp, err)
	}
	return time.Unix(sec, 0).UTC(), nil
}

// ReadTimestampFromS3 reads {partition}/{block}_{version}_block.json.gz from the given bucket and
// returns the on-chain header timestamp. The S3Reader adapter auto-decompresses .gz keys, so the
// stream yields plain JSON — the caller must not gunzip again.
func ReadTimestampFromS3(ctx context.Context, reader outbound.S3Reader, bucket string, blockNumber int64, version int) (time.Time, error) {
	key := s3key.Build(blockNumber, version, s3key.Block)
	rc, err := reader.StreamFile(ctx, bucket, key)
	if err != nil {
		return time.Time{}, fmt.Errorf("s3 get %s: %w", key, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return time.Time{}, fmt.Errorf("read %s: %w", key, err)
	}
	ts, err := ParseTimestamp(data)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: %w", key, err)
	}
	return ts, nil
}
