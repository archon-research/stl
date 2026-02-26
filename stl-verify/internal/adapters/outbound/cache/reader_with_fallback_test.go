package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// --- hand-written mocks ---

type mockCacheReader struct {
	block       json.RawMessage
	blockErr    error
	receipts    json.RawMessage
	receiptsErr error
	traces      json.RawMessage
	tracesErr   error
	blobs       json.RawMessage
	blobsErr    error

	blockCalled    bool
	receiptsCalled bool
	tracesCalled   bool
	blobsCalled    bool
}

func (m *mockCacheReader) GetBlock(_ context.Context, _ int64, _ int64, _ int) (json.RawMessage, error) {
	m.blockCalled = true
	return m.block, m.blockErr
}
func (m *mockCacheReader) GetReceipts(_ context.Context, _ int64, _ int64, _ int) (json.RawMessage, error) {
	m.receiptsCalled = true
	return m.receipts, m.receiptsErr
}
func (m *mockCacheReader) GetTraces(_ context.Context, _ int64, _ int64, _ int) (json.RawMessage, error) {
	m.tracesCalled = true
	return m.traces, m.tracesErr
}
func (m *mockCacheReader) GetBlobs(_ context.Context, _ int64, _ int64, _ int) (json.RawMessage, error) {
	m.blobsCalled = true
	return m.blobs, m.blobsErr
}
func (m *mockCacheReader) Close() error {
	return nil
}

// mockS3Reader implements outbound.S3Reader with configurable behaviour.
type mockS3Reader struct {
	data    []byte
	err     error
	called  bool
	lastKey string
}

func (m *mockS3Reader) ListFiles(_ context.Context, _, _ string) ([]outbound.S3File, error) {
	return nil, nil
}

// StreamFile is the only method used by ReaderWithFallback.
func (m *mockS3Reader) StreamFile(_ context.Context, _ string, key string) (io.ReadCloser, error) {
	m.called = true
	m.lastKey = key
	if m.err != nil {
		return nil, m.err
	}
	return io.NopCloser(strings.NewReader(string(m.data))), nil
}

// ListPrefix satisfies the full S3Reader interface.
func (m *mockS3Reader) ListPrefix(_ context.Context, _, _ string) ([]string, error) {
	return nil, nil
}

// --- helpers ---

func newTestReader(t *testing.T, redis *mockCacheReader, s3 *mockS3Reader) *ReaderWithFallback {
	t.Helper()
	r, err := NewReaderWithFallback(redis, s3, "test-bucket", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewReaderWithFallback unexpected error: %v", err)
	}
	return r
}

// --- NewReaderWithFallback ---

func TestNewReaderWithFallback_EmptyBucket(t *testing.T) {
	_, err := NewReaderWithFallback(&mockCacheReader{}, &mockS3Reader{}, "", slog.Default())
	if err == nil {
		t.Fatal("expected error for empty bucket, got nil")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("expected error to mention 'bucket', got: %v", err)
	}
}

func TestNewReaderWithFallback_Success(t *testing.T) {
	r, err := NewReaderWithFallback(&mockCacheReader{}, &mockS3Reader{}, "my-bucket", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("expected non-nil reader")
	}
	if r.bucket != "my-bucket" {
		t.Errorf("bucket = %q, want %q", r.bucket, "my-bucket")
	}
	if r.logger == nil {
		t.Error("expected default logger to be set when nil is passed")
	}
}

// --- table-driven tests for all four Get methods ---

type getCase struct {
	name string
	// Redis configuration
	redisData json.RawMessage
	redisErr  error
	// S3 configuration
	s3Data []byte
	s3Err  error
	// Expected outcomes
	wantData   json.RawMessage
	wantErr    bool
	wantS3Call bool
}

func runGetCases(t *testing.T, cases []getCase, invoke func(r *ReaderWithFallback, redis *mockCacheReader, s3 *mockS3Reader) (json.RawMessage, error)) {
	t.Helper()
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			redis := &mockCacheReader{
				block:       tt.redisData,
				blockErr:    tt.redisErr,
				receipts:    tt.redisData,
				receiptsErr: tt.redisErr,
				traces:      tt.redisData,
				tracesErr:   tt.redisErr,
				blobs:       tt.redisData,
				blobsErr:    tt.redisErr,
			}
			s3mock := &mockS3Reader{data: tt.s3Data, err: tt.s3Err}
			reader := newTestReader(t, redis, s3mock)

			got, err := invoke(reader, redis, s3mock)

			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if string(got) != string(tt.wantData) {
				t.Errorf("data = %s, want %s", got, tt.wantData)
			}
			if s3mock.called != tt.wantS3Call {
				t.Errorf("S3 called = %v, want %v", s3mock.called, tt.wantS3Call)
			}
		})
	}
}

func sharedCases() []getCase {
	return []getCase{
		{
			name:       "redis hit: returns data, S3 never called",
			redisData:  json.RawMessage(`{"hit":true}`),
			wantData:   json.RawMessage(`{"hit":true}`),
			wantErr:    false,
			wantS3Call: false,
		},
		{
			name:       "redis error: returns error immediately, S3 never called",
			redisErr:   fmt.Errorf("redis connection refused"),
			wantData:   nil,
			wantErr:    true,
			wantS3Call: false,
		},
		{
			name:       "redis miss + S3 hit: returns S3 data",
			redisData:  nil,
			s3Data:     []byte(`{"s3":true}`),
			wantData:   json.RawMessage(`{"s3":true}`),
			wantErr:    false,
			wantS3Call: true,
		},
		{
			name:       "redis miss + S3 not-found: returns nil, nil",
			redisData:  nil,
			s3Err:      &types.NoSuchKey{},
			wantData:   nil,
			wantErr:    false,
			wantS3Call: true,
		},
		{
			name:       "redis miss + S3 transient error: returns error",
			redisData:  nil,
			s3Err:      fmt.Errorf("connection reset by peer"),
			wantData:   nil,
			wantErr:    true,
			wantS3Call: true,
		},
	}
}

func TestGetBlock(t *testing.T) {
	runGetCases(t, sharedCases(), func(r *ReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
		return r.GetBlock(context.Background(), 1, 21000000, 0)
	})
}

func TestGetReceipts(t *testing.T) {
	runGetCases(t, sharedCases(), func(r *ReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
		return r.GetReceipts(context.Background(), 1, 21000000, 0)
	})
}

func TestGetTraces(t *testing.T) {
	runGetCases(t, sharedCases(), func(r *ReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
		return r.GetTraces(context.Background(), 1, 21000000, 0)
	})
}

func TestGetBlobs(t *testing.T) {
	runGetCases(t, sharedCases(), func(r *ReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
		return r.GetBlobs(context.Background(), 1, 21000000, 0)
	})
}

// TestGetBlock_S3KeyFormat verifies that the S3 key uses the correct format
// (no chain ID prefix — chain isolation is done via the bucket name).
func TestGetBlock_S3KeyFormat(t *testing.T) {
	redis := &mockCacheReader{} // cache miss
	s3mock := &mockS3Reader{data: []byte(`{}`)}
	reader := newTestReader(t, redis, s3mock)

	var blockNumber int64 = 21000000
	version := 1

	_, err := reader.GetBlock(context.Background(), 1, blockNumber, version)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedKey := s3key.Build(blockNumber, version, s3key.Block)
	if s3mock.lastKey != expectedKey {
		t.Errorf("S3 key = %q, want %q", s3mock.lastKey, expectedKey)
	}
	if strings.Contains(s3mock.lastKey, "1/") {
		t.Errorf("S3 key should NOT contain chain ID prefix, got %q", s3mock.lastKey)
	}
}
