package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/klauspost/compress/zstd"
)

type fakeWriter struct {
	bucket    string
	key       string
	body      []byte
	compress  bool
	written   bool
	returnErr error
}

func (f *fakeWriter) WriteFileIfNotExists(_ context.Context, bucket, key string, content io.Reader, compressGzip bool) (bool, error) {
	if f.returnErr != nil {
		return false, f.returnErr
	}
	b, err := io.ReadAll(content)
	if err != nil {
		return false, err
	}
	f.bucket, f.key, f.body, f.compress = bucket, key, b, compressGzip
	f.written = true
	return true, nil
}
}

func (f *fakeWriter) FileExists(_ context.Context, _, _ string) (bool, error) { return false, nil }

func sampleRecord() outbound.CallRecord {
	return outbound.CallRecord{
		ChainID:         1,
		BlockNumber:     21500042,
		BlockVersion:    0,
		BuildID:         47,
		Source:          "oracle-price",
		Multicaller:     "0xcA11bde05977b3631167028862bE2a173976CA11",
		Timestamp:       time.Date(2026, 6, 8, 10, 30, 45, 0, time.UTC),
		ContractAddress: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
		Selector:        "0xfeaf968c",
		CallData:        []byte{0xfe, 0xaf, 0x96, 0x8c, 0xaa},
		Success:         true,
		Response:        []byte{0x00, 0x01, 0x02},
	}
}

func TestArchiveKeyAndBucket(t *testing.T) {
	fw := &fakeWriter{}
	a := NewCallArchiver(fw, "raw-sc-calls-prod", nil)

	if err := a.Archive(context.Background(), sampleRecord()); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if fw.bucket != "raw-sc-calls-prod" {
		t.Fatalf("bucket = %q", fw.bucket)
	}
	wantKey := "raw-sc-calls/chain_id=1/block=21500000-21500999/21500042_0_oracle-price_"
	if !bytes.HasPrefix([]byte(fw.key), []byte(wantKey)) {
		t.Fatalf("key = %q, want prefix %q", fw.key, wantKey)
	}
	if fw.compress {
		t.Fatalf("compressGzip must be false (payload is zstd, not gzip)")
	}
}

func TestArchivePayloadRoundTrip(t *testing.T) {
	fw := &fakeWriter{}
	a := NewCallArchiver(fw, "bucket", nil)
	if err := a.Archive(context.Background(), sampleRecord()); err != nil {
		t.Fatalf("Archive: %v", err)
	}

	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	raw, err := dec.DecodeAll(fw.body, nil)
	if err != nil {
		t.Fatalf("zstd decode: %v", err)
	}

	var line struct {
		ChainID     int64  `json:"chain_id"`
		BlockNumber int64  `json:"block_number"`
		BuildID     int64  `json:"build_id"`
		Selector    string `json:"selector"`
		CallData    string `json:"call_data"`
		Response    string `json:"response"`
		Success     bool   `json:"success"`
		Timestamp   string `json:"timestamp"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(raw), &line); err != nil {
		t.Fatalf("json: %v", err)
	}
	if line.ChainID != 1 || line.BlockNumber != 21500042 || line.BuildID != 47 {
		t.Fatalf("scalar fields wrong: %+v", line)
	}
	if line.CallData != "0xfeaf968caa" {
		t.Fatalf("call_data = %q", line.CallData)
	}
	if line.Response != "0x000102" {
		t.Fatalf("response = %q", line.Response)
	}
	if line.Timestamp != "20260608T103045Z" {
		t.Fatalf("timestamp = %q", line.Timestamp)
	}
	if line.Selector != "0xfeaf968c" {
		t.Fatalf("selector = %q", line.Selector)
	}
}

func TestArchivePropagatesWriterError(t *testing.T) {
	fw := &fakeWriter{returnErr: io.ErrClosedPipe}
	a := NewCallArchiver(fw, "bucket", nil)
	if err := a.Archive(context.Background(), sampleRecord()); err == nil {
		t.Fatal("expected error from writer to propagate")
	}
}
