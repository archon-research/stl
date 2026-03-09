package mockchain

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// TestDataStore_Get verifies Get across valid indexes, missing indexes, and all data types.
func TestDataStore_Get(t *testing.T) {
	ds := NewFixtureDataStore()

	tests := []struct {
		name     string
		index    int
		dataType string
		wantOK   bool
	}{
		{"block at index 0", 0, "block", true},
		{"receipts at index 0", 0, "receipts", true},
		{"traces at index 0", 0, "traces", true},
		{"blobs at index 0", 0, "blobs", true},
		{"missing index", 99, "block", false},
		{"missing data type", 0, "unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := ds.Get(tt.index, tt.dataType)
			if ok != tt.wantOK {
				t.Errorf("Get(%d, %q) ok = %v, want %v", tt.index, tt.dataType, ok, tt.wantOK)
			}
		})
	}
}

// TestDataStore_Add verifies that Add overwrites an existing entry.
func TestDataStore_Add(t *testing.T) {
	ds := NewFixtureDataStore()
	ds.Add(0, "block", json.RawMessage(`"overwritten"`))
	raw, ok := ds.Get(0, "block")
	if !ok {
		t.Fatal("expected entry after overwrite")
	}
	if string(raw) != `"overwritten"` {
		t.Errorf("expected overwritten value, got %s", raw)
	}
}

// TestDataStore_Len verifies the header count after population.
func TestDataStore_Len(t *testing.T) {
	tests := []struct {
		name    string
		store   *DataStore
		wantLen int
	}{
		{"empty store", NewDataStore(), 0},
		{"test store", NewFixtureDataStore(), 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.store.Len(); got != tt.wantLen {
				t.Errorf("Len() = %d, want %d", got, tt.wantLen)
			}
		})
	}
}

// TestHeaders_ReturnsCopy verifies that mutating the slice returned by Headers does not affect the store.
func TestHeaders_ReturnsCopy(t *testing.T) {
	ds := NewFixtureDataStore()
	original := ds.Headers()[0].Hash

	h := ds.Headers()
	h[0].Hash = "0xmutated"

	if ds.Headers()[0].Hash != original {
		t.Fatal("mutating returned slice must not affect the DataStore")
	}
}

// TestNewDataStore verifies that a freshly created store is empty.
func TestNewDataStore(t *testing.T) {
	ds := NewDataStore()
	if ds.Len() != 0 {
		t.Fatalf("expected empty store, got %d", ds.Len())
	}
	_, ok := ds.Get(0, "block")
	if ok {
		t.Fatal("expected empty store to return not found")
	}
}

// --- parseS3Key ---

// TestParseS3Key covers the valid path and all failure modes.
func TestParseS3Key(t *testing.T) {
	tests := []struct {
		name         string
		key, prefix  string
		wantBlockNum int64
		wantDataType string
		wantOK       bool
	}{
		{"valid block", "blocks/1000/block.json", "blocks", 1000, "block", true},
		{"valid receipts", "blocks/999/receipts.json", "blocks", 999, "receipts", true},
		{"valid traces", "pfx/42/traces.json", "pfx", 42, "traces", true},
		{"valid blobs", "pfx/0/blobs.json", "pfx", 0, "blobs", true},
		{"wrong prefix", "other/1/block.json", "blocks", 0, "", false},
		{"missing data type segment", "blocks/1000", "blocks", 0, "", false},
		{"non-numeric block number", "blocks/abc/block.json", "blocks", 0, "", false},
		{"no .json suffix", "blocks/1/block.txt", "blocks", 0, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockNum, dataType, ok := parseS3Key(tt.key, tt.prefix)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if blockNum != tt.wantBlockNum {
				t.Errorf("blockNum = %d, want %d", blockNum, tt.wantBlockNum)
			}
			if dataType != tt.wantDataType {
				t.Errorf("dataType = %q, want %q", dataType, tt.wantDataType)
			}
		})
	}
}

// --- streamAll ---

// TestStreamAll_Success verifies that bytes are returned and the reader is closed.
func TestStreamAll_Success(t *testing.T) {
	payload := `{"number":"0x1"}`
	closed := false
	getter := &mockS3Lister{
		streamFile: func(_ context.Context, _, _ string) (io.ReadCloser, error) {
			return &closeTracker{Reader: strings.NewReader(payload), closed: &closed}, nil
		},
	}
	raw, err := streamAll(context.Background(), getter, "bucket", "key")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(raw) != payload {
		t.Errorf("got %q, want %q", raw, payload)
	}
	if !closed {
		t.Error("reader was not closed")
	}
}

// TestStreamAll_StreamError verifies that a StreamFile error is propagated unchanged.
func TestStreamAll_StreamError(t *testing.T) {
	sentinel := errors.New("stream error")
	getter := &mockS3Lister{
		streamFile: func(_ context.Context, _, _ string) (io.ReadCloser, error) {
			return nil, sentinel
		},
	}
	_, err := streamAll(context.Background(), getter, "bucket", "key")
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error %q, got: %v", sentinel, err)
	}
}

// --- LoadFromS3 ---

// TestLoadFromS3_Success verifies that blocks are loaded in ascending order,
// headers are populated, and data is accessible by index.
func TestLoadFromS3_Success(t *testing.T) {
	header1 := outbound.BlockHeader{Number: "0x3e8", Hash: "0xaaa"}
	header2 := outbound.BlockHeader{Number: "0x3e9", Hash: "0xbbb"}
	raw1, err := json.Marshal(header1)
	if err != nil {
		t.Fatalf("marshal header1: %v", err)
	}
	raw2, err := json.Marshal(header2)
	if err != nil {
		t.Fatalf("marshal header2: %v", err)
	}

	files := map[string]string{
		"blocks/1001/block.json":    string(raw2), // out of order — should load 1000 first
		"blocks/1000/block.json":    string(raw1),
		"blocks/1000/receipts.json": `[]`,
	}
	getter := newStaticS3Lister("bucket", files)

	ds := NewDataStore()
	if err := ds.LoadFromS3(context.Background(), getter, "bucket", "blocks"); err != nil {
		t.Fatalf("LoadFromS3: %v", err)
	}

	headers := ds.Headers()
	if len(headers) != 2 {
		t.Fatalf("expected 2 headers, got %d", len(headers))
	}
	// Index 0 → block 1000 (ascending order)
	if headers[0].Hash != "0xaaa" {
		t.Errorf("expected first header hash 0xaaa, got %s", headers[0].Hash)
	}
	if headers[1].Hash != "0xbbb" {
		t.Errorf("expected second header hash 0xbbb, got %s", headers[1].Hash)
	}
	// Receipts at index 0 should be present.
	if _, ok := ds.Get(0, "receipts"); !ok {
		t.Error("expected receipts at index 0")
	}
	// Block 1001 has no receipts.
	if _, ok := ds.Get(1, "receipts"); ok {
		t.Error("expected no receipts at index 1")
	}
}

// TestLoadFromS3_Errors verifies the exact error wrapping for each failure mode.
func TestLoadFromS3_Errors(t *testing.T) {
	errList := errors.New("list error")
	errStream := errors.New("stream error")

	tests := []struct {
		name        string
		makeGetter  func() *mockS3Lister
		wantWraps   error  // sentinel that must be reachable via errors.Is
		wantMessage string // full wrapping prefix that must appear in err.Error()
	}{
		{
			name: "list error",
			makeGetter: func() *mockS3Lister {
				return &mockS3Lister{
					listFiles: func(_ context.Context, _, _ string) ([]outbound.S3File, error) {
						return nil, errList
					},
				}
			},
			wantWraps:   errList,
			wantMessage: "loading from S3: listing files: list error",
		},
		{
			name: "stream error",
			makeGetter: func() *mockS3Lister {
				g := newStaticS3Lister("bucket", map[string]string{
					"blocks/1000/block.json": `{"number":"0x3e8","hash":"0xaaa"}`,
				})
				g.streamFile = func(_ context.Context, _, _ string) (io.ReadCloser, error) {
					return nil, errStream
				}
				return g
			},
			wantWraps:   errStream,
			wantMessage: "loading from S3: block 1000 block: stream error",
		},
		{
			name: "invalid block JSON",
			makeGetter: func() *mockS3Lister {
				return newStaticS3Lister("bucket", map[string]string{
					"blocks/1000/block.json": `not-json`,
				})
			},
			wantWraps:   nil, // json errors are not sentinels; check message only
			wantMessage: "loading from S3: block 1000: unmarshalling header:",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := NewDataStore()
			err := ds.LoadFromS3(context.Background(), tt.makeGetter(), "bucket", "blocks")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("error = %q; want it to contain %q", err.Error(), tt.wantMessage)
			}
			if tt.wantWraps != nil && !errors.Is(err, tt.wantWraps) {
				t.Errorf("errors.Is(%v) = false; sentinel not in chain", tt.wantWraps)
			}
		})
	}
}

// TestLoadFromS3_SkipsUnparsableKeys verifies that keys not matching the expected
// format are silently skipped and do not cause errors.
func TestLoadFromS3_SkipsUnparsableKeys(t *testing.T) {
	header := outbound.BlockHeader{Number: "0x1", Hash: "0xaaa"}
	raw, _ := json.Marshal(header)
	files := map[string]string{
		"blocks/1000/block.json": string(raw),
		"blocks/README.md":       "docs",
		"blocks/bad/path":        "ignored",
	}
	getter := newStaticS3Lister("bucket", files)

	ds := NewDataStore()
	if err := ds.LoadFromS3(context.Background(), getter, "bucket", "blocks"); err != nil {
		t.Fatalf("LoadFromS3: %v", err)
	}
	if ds.Len() != 1 {
		t.Errorf("expected 1 header (unparsable keys skipped), got %d", ds.Len())
	}
}

// --- test helpers ---

// mockS3Lister is a configurable s3Lister for testing.
type mockS3Lister struct {
	listFiles  func(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error)
	streamFile func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

func (m *mockS3Lister) ListFiles(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error) {
	return m.listFiles(ctx, bucket, prefix)
}

func (m *mockS3Lister) StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return m.streamFile(ctx, bucket, key)
}

// newStaticS3Lister builds a mockS3Lister backed by an in-memory map of key→body.
// ListFiles filters by the prefix argument passed at call time, so the test catches
// production code that wires the wrong prefix.
func newStaticS3Lister(wantBucket string, files map[string]string) *mockS3Lister {
	return &mockS3Lister{
		listFiles: func(_ context.Context, bucket, prefix string) ([]outbound.S3File, error) {
			if bucket != wantBucket {
				return nil, fmt.Errorf("ListFiles: unexpected bucket %q (want %q)", bucket, wantBucket)
			}
			out := make([]outbound.S3File, 0, len(files))
			for k := range files {
				if strings.HasPrefix(k, prefix+"/") {
					out = append(out, outbound.S3File{Key: k})
				}
			}
			return out, nil
		},
		streamFile: func(_ context.Context, bucket, key string) (io.ReadCloser, error) {
			if bucket != wantBucket {
				return nil, fmt.Errorf("StreamFile: unexpected bucket %q (want %q)", bucket, wantBucket)
			}
			body, ok := files[key]
			if !ok {
				return nil, errors.New("key not found: " + key)
			}
			return io.NopCloser(strings.NewReader(body)), nil
		},
	}
}

// closeTracker wraps an io.Reader and records whether Close was called.
type closeTracker struct {
	io.Reader
	closed *bool
}

func (c *closeTracker) Close() error {
	*c.closed = true
	return nil
}
