package s3

import (
	"bufio"
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
	writeN    int   // number of WriteFileIfNotExists invocations
	readErr   error // set if reading the content reader failed
	returnErr error
}

func (f *fakeWriter) WriteFileIfNotExists(_ context.Context, bucket, key string, content io.Reader, compressGzip bool) (bool, error) {
	f.writeN++
	if f.returnErr != nil {
		return false, f.returnErr
	}
	b, err := io.ReadAll(content)
	if err != nil {
		f.readErr = err
		return false, err
	}
	f.bucket, f.key, f.body, f.compress = bucket, key, b, compressGzip
	f.written = true
	return true, nil
}

func (f *fakeWriter) FileExists(_ context.Context, _, _ string) (bool, error) { return false, nil }

func sampleBatch() outbound.CallBatchRecord {
	return outbound.CallBatchRecord{
		ChainID:      1,
		BlockNumber:  21500042,
		BlockVersion: 0,
		BuildID:      47,
		Source:       "oracle-price",
		Multicaller:  "0xcA11bde05977b3631167028862bE2a173976CA11",
		Timestamp:    time.Date(2026, 6, 8, 10, 30, 45, 0, time.UTC),
		Calls: []outbound.CallEntry{
			{
				ContractAddress: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Selector:        "0xfeaf968c",
				CallData:        []byte{0xfe, 0xaf, 0x96, 0x8c, 0xaa},
				Success:         true,
				Response:        []byte{0x00, 0x01, 0x02},
			},
			{
				ContractAddress: "0x1F98431c8aD98523631AE4a59f267346ea31F984",
				Selector:        "0x1816d0dd",
				CallData:        []byte{0x18, 0x16, 0x0d, 0xdd},
				Success:         false,
				Response:        []byte{0xde, 0xad},
			},
		},
	}
}

func TestArchiveKeyAndBucket(t *testing.T) {
	fw := &fakeWriter{}
	a := NewCallArchiver(fw, "raw-sc-calls-prod", nil)

	if err := a.Archive(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if fw.bucket != "raw-sc-calls-prod" {
		t.Fatalf("bucket = %q", fw.bucket)
	}
	wantKeyPrefix := "raw-sc-calls/chain_id=1/block=21500000-21500999/21500042_0_oracle-price_"
	if !bytes.HasPrefix([]byte(fw.key), []byte(wantKeyPrefix)) {
		t.Fatalf("key = %q, want prefix %q", fw.key, wantKeyPrefix)
	}
	if !bytes.HasSuffix([]byte(fw.key), []byte(".jsonl.zst")) {
		t.Fatalf("key = %q, want .jsonl.zst suffix", fw.key)
	}
	if fw.compress {
		t.Fatalf("compressGzip must be false (payload is zstd, not gzip)")
	}
}

func TestArchiveSingleObjectPerBatch(t *testing.T) {
	fw := &fakeWriter{}
	a := NewCallArchiver(fw, "bucket", nil)
	if err := a.Archive(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if fw.writeN != 1 {
		t.Fatalf("WriteFileIfNotExists called %d times, want 1 per batch", fw.writeN)
	}
}

func TestArchivePayloadIsJSONLOneLinePerCall(t *testing.T) {
	fw := &fakeWriter{}
	a := NewCallArchiver(fw, "bucket", nil)
	batch := sampleBatch()
	if err := a.Archive(context.Background(), batch); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	if fw.readErr != nil {
		t.Fatalf("writer failed to read payload: %v", fw.readErr)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	defer dec.Close()
	raw, err := dec.DecodeAll(fw.body, nil)
	if err != nil {
		t.Fatalf("zstd decode: %v", err)
	}

	type line struct {
		ChainID         int64  `json:"chain_id"`
		BlockNumber     int64  `json:"block_number"`
		BuildID         int64  `json:"build_id"`
		Source          string `json:"source"`
		Multicaller     string `json:"multicaller"`
		Timestamp       string `json:"timestamp"`
		ContractAddress string `json:"contract_address"`
		Selector        string `json:"selector"`
		CallData        string `json:"call_data"`
		Response        string `json:"response"`
		Success         bool   `json:"success"`
	}
	var lines []line
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		var l line
		if err := json.Unmarshal(scanner.Bytes(), &l); err != nil {
			t.Fatalf("json line: %v (%q)", err, scanner.Bytes())
		}
		lines = append(lines, l)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(lines) != len(batch.Calls) {
		t.Fatalf("got %d JSONL lines, want %d", len(lines), len(batch.Calls))
	}

	// Batch metadata is repeated on every line so each is self-describing.
	for i, l := range lines {
		if l.ChainID != 1 || l.BlockNumber != 21500042 || l.BuildID != 47 {
			t.Fatalf("line %d scalar fields wrong: %+v", i, l)
		}
		if l.Source != "oracle-price" || l.Multicaller != batch.Multicaller {
			t.Fatalf("line %d batch metadata wrong: %+v", i, l)
		}
		if l.Timestamp != "20260608T103045Z" {
			t.Fatalf("line %d timestamp = %q", i, l.Timestamp)
		}
	}

	// Per-call fields follow the order of batch.Calls.
	if lines[0].CallData != "0xfeaf968caa" || lines[0].Response != "0x000102" || !lines[0].Success {
		t.Fatalf("line 0 per-call fields wrong: %+v", lines[0])
	}
	if lines[1].CallData != "0x18160ddd" || lines[1].Response != "0xdead" || lines[1].Success {
		t.Fatalf("line 1 per-call fields wrong: %+v", lines[1])
	}
}

func TestArchiveEmptyBatchSkipsWrite(t *testing.T) {
	fw := &fakeWriter{}
	a := NewCallArchiver(fw, "bucket", nil)
	empty := sampleBatch()
	empty.Calls = nil
	if err := a.Archive(context.Background(), empty); err != nil {
		t.Fatalf("Archive empty batch: %v", err)
	}
	if fw.writeN != 0 {
		t.Fatalf("WriteFileIfNotExists called %d times for empty batch, want 0", fw.writeN)
	}
}

func TestArchiveKeyDiffersByBatchComposition(t *testing.T) {
	fw1 := &fakeWriter{}
	fw2 := &fakeWriter{}
	a1 := NewCallArchiver(fw1, "bucket", nil)
	a2 := NewCallArchiver(fw2, "bucket", nil)

	b1 := sampleBatch()
	b2 := sampleBatch()
	// Swap the two calls; same data, different order ⇒ different batch hash.
	b2.Calls[0], b2.Calls[1] = b2.Calls[1], b2.Calls[0]

	if err := a1.Archive(context.Background(), b1); err != nil {
		t.Fatalf("Archive b1: %v", err)
	}
	if err := a2.Archive(context.Background(), b2); err != nil {
		t.Fatalf("Archive b2: %v", err)
	}
	if fw1.key == fw2.key {
		t.Fatalf("expected different keys for reordered batches, got %q", fw1.key)
	}
}

func TestArchivePropagatesWriterError(t *testing.T) {
	fw := &fakeWriter{returnErr: io.ErrClosedPipe}
	a := NewCallArchiver(fw, "bucket", nil)
	if err := a.Archive(context.Background(), sampleBatch()); err == nil {
		t.Fatal("expected error from writer to propagate")
	}
}
