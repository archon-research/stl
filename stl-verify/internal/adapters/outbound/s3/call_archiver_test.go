package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/ethereum/go-ethereum/common"
	"github.com/klauspost/compress/zstd"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// fakeS3 implements s3CallArchiveAPI for unit tests.
type fakeS3 struct {
	mu      sync.Mutex
	puts    []capturedPut
	putErr  error
	apiCode string // when set, returned as a smithy.APIError
}

type capturedPut struct {
	Bucket          string
	Key             string
	Body            []byte
	ContentType     string
	ContentEncoding string
	IfNoneMatch     string
}

type fakeAPIError struct{ code string }

func (e *fakeAPIError) Error() string                 { return e.code }
func (e *fakeAPIError) ErrorCode() string             { return e.code }
func (e *fakeAPIError) ErrorMessage() string          { return e.code }
func (e *fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }

func (f *fakeS3) PutObject(_ context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	body, _ := io.ReadAll(in.Body)
	f.puts = append(f.puts, capturedPut{
		Bucket:          deref(in.Bucket),
		Key:             deref(in.Key),
		Body:            body,
		ContentType:     deref(in.ContentType),
		ContentEncoding: deref(in.ContentEncoding),
		IfNoneMatch:     deref(in.IfNoneMatch),
	})
	if f.apiCode != "" {
		return nil, &fakeAPIError{code: f.apiCode}
	}
	if f.putErr != nil {
		return nil, f.putErr
	}
	return &s3.PutObjectOutput{}, nil
}

func deref(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func newSilentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func newArchiver(t *testing.T, client s3CallArchiveAPI) *CallArchiver {
	t.Helper()
	a, err := newCallArchiverWithClient(client, "raw-sc-calls", newSilentLogger())
	if err != nil {
		t.Fatalf("newCallArchiverWithClient: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func sampleBatch() outbound.CallBatch {
	return outbound.CallBatch{
		ChainID:      1,
		BlockNumber:  21500042,
		BlockVersion: 0,
		BuildID:      42,
		Source:       "oracle-price",
		Multicaller:  common.HexToAddress("0xca11bde05779b6216e94d2f0a4d57f1ddee6e6"),
		Timestamp:    time.Date(2026, 6, 4, 10, 30, 45, 118_000_000, time.UTC),
		Records: []outbound.CallRecord{
			{
				ContractAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
				Method:          "latestRoundData",
				CallData:        []byte{0xfe, 0xaf, 0x96, 0x8c},
				Response:        []byte{0xde, 0xad, 0xbe, 0xef},
				Success:         true,
			},
		},
	}
}

func TestArchiveBatchWritesZstdJSONL(t *testing.T) {
	fs := &fakeS3{}
	a := newArchiver(t, fs)

	batch := sampleBatch()
	if err := a.ArchiveBatch(context.Background(), batch); err != nil {
		t.Fatalf("ArchiveBatch: %v", err)
	}
	if len(fs.puts) != 1 {
		t.Fatalf("PutObject calls = %d, want 1", len(fs.puts))
	}
	put := fs.puts[0]

	wantKey := "raw-sc-calls/chain_id=1/block=21500000-21500999/bv=0/build_id=42/oracle-price_latestRoundData_20260604T103045Z.jsonl.zst"
	if put.Key != wantKey {
		t.Errorf("key = %q, want %q", put.Key, wantKey)
	}
	if put.Bucket != "raw-sc-calls" {
		t.Errorf("bucket = %q", put.Bucket)
	}
	if put.IfNoneMatch != "*" {
		t.Errorf("IfNoneMatch = %q, want *", put.IfNoneMatch)
	}
	if put.ContentEncoding != "zstd" {
		t.Errorf("ContentEncoding = %q, want zstd", put.ContentEncoding)
	}
	if put.ContentType != "application/x-ndjson" {
		t.Errorf("ContentType = %q, want application/x-ndjson", put.ContentType)
	}

	dec, err := zstd.NewReader(bytes.NewReader(put.Body))
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	raw, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("zstd read: %v", err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("jsonl lines = %d, want 1", len(lines))
	}
	var line map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &line); err != nil {
		t.Fatalf("json decode: %v", err)
	}
	if line["call_data"] != "0xfeaf968c" {
		t.Errorf("call_data = %v", line["call_data"])
	}
	if line["response"] != "0xdeadbeef" {
		t.Errorf("response = %v", line["response"])
	}
	if line["method"] != "latestRoundData" {
		t.Errorf("method = %v", line["method"])
	}
	if line["chain_id"].(float64) != 1 {
		t.Errorf("chain_id = %v", line["chain_id"])
	}
}

func TestArchiveBatchKeyUsesMixedForHeterogeneousMethods(t *testing.T) {
	fs := &fakeS3{}
	a := newArchiver(t, fs)
	batch := sampleBatch()
	batch.Records = append(batch.Records, outbound.CallRecord{
		ContractAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Method:          "decimals",
		Success:         true,
	})
	if err := a.ArchiveBatch(context.Background(), batch); err != nil {
		t.Fatalf("ArchiveBatch: %v", err)
	}
	if !strings.Contains(fs.puts[0].Key, "/oracle-price_mixed_") {
		t.Errorf("expected mixed method in key, got %q", fs.puts[0].Key)
	}
}

func TestArchiveBatchEmptyRecordsIsNoOp(t *testing.T) {
	fs := &fakeS3{}
	a := newArchiver(t, fs)
	batch := sampleBatch()
	batch.Records = nil
	if err := a.ArchiveBatch(context.Background(), batch); err != nil {
		t.Fatalf("ArchiveBatch: %v", err)
	}
	if len(fs.puts) != 0 {
		t.Fatalf("PutObject calls = %d, want 0", len(fs.puts))
	}
}

func TestArchiveBatchMissingSourceReturnsError(t *testing.T) {
	a := newArchiver(t, &fakeS3{})
	batch := sampleBatch()
	batch.Source = ""
	if err := a.ArchiveBatch(context.Background(), batch); err == nil {
		t.Fatal("expected error for missing Source")
	}
}

func TestArchiveBatchFailOpenOnS3Error(t *testing.T) {
	fs := &fakeS3{putErr: errors.New("network down")}
	a := newArchiver(t, fs)
	if err := a.ArchiveBatch(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("ArchiveBatch must swallow S3 errors: %v", err)
	}
}

func TestArchiveBatchFailOpenOnPreconditionFailed(t *testing.T) {
	fs := &fakeS3{apiCode: "PreconditionFailed"}
	a := newArchiver(t, fs)
	if err := a.ArchiveBatch(context.Background(), sampleBatch()); err != nil {
		t.Fatalf("412 should be a benign no-op: %v", err)
	}
}

func TestNewCallArchiverRejectsEmptyBucket(t *testing.T) {
	if _, err := newCallArchiverWithClient(&fakeS3{}, "", newSilentLogger()); err == nil {
		t.Fatal("expected error for empty bucket")
	}
}
