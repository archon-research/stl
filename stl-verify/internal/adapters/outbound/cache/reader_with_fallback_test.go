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

func newTestReader(t *testing.T, redis *mockCacheReader, s3 *mockS3Reader) *BlockCacheReaderWithFallback {
	t.Helper()
	r, err := NewReaderWithFallback(redis, s3, 1, "staging", "stl-sentinelstaging-ethereum-raw", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewReaderWithFallback unexpected error: %v", err)
	}
	return r
}

// --- NewReaderWithFallback ---

func TestNewReaderWithFallback_EmptyBucket(t *testing.T) {
	_, err := NewReaderWithFallback(&mockCacheReader{}, &mockS3Reader{}, 1, "staging", "", slog.Default())
	if err == nil {
		t.Fatal("expected error for empty bucket, got nil")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("expected error to mention 'bucket', got: %v", err)
	}
}

func TestNewReaderWithFallback_Success(t *testing.T) {
	r, err := NewReaderWithFallback(&mockCacheReader{}, &mockS3Reader{}, 1, "staging", "stl-sentinelstaging-ethereum-raw", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r == nil {
		t.Fatal("expected non-nil reader")
	}
	if r.chainID != 1 {
		t.Errorf("chainID = %d, want 1", r.chainID)
	}
	if r.bucket != "stl-sentinelstaging-ethereum-raw" {
		t.Errorf("bucket = %q, want %q", r.bucket, "stl-sentinelstaging-ethereum-raw")
	}
	if r.logger == nil {
		t.Error("expected default logger to be set when nil is passed")
	}
}

func TestNewReaderWithFallback_NilRedis(t *testing.T) {
	_, err := NewReaderWithFallback(nil, &mockS3Reader{}, 1, "staging", "stl-sentinelstaging-ethereum-raw", slog.Default())
	if err == nil {
		t.Fatal("expected error for nil redis, got nil")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("expected error to mention 'redis', got: %v", err)
	}
}

func TestNewReaderWithFallback_NilS3(t *testing.T) {
	_, err := NewReaderWithFallback(&mockCacheReader{}, nil, 1, "staging", "stl-sentinelstaging-ethereum-raw", slog.Default())
	if err == nil {
		t.Fatal("expected error for nil s3, got nil")
	}
	if !strings.Contains(err.Error(), "s3") {
		t.Errorf("expected error to mention 's3', got: %v", err)
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

func runGetCases(t *testing.T, cases []getCase, invoke func(r *BlockCacheReaderWithFallback, redis *mockCacheReader, s3 *mockS3Reader) (json.RawMessage, error)) {
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
	runGetCases(t, sharedCases(), func(r *BlockCacheReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
		return r.GetBlock(context.Background(), 1, 21000000, 0)
	})
}

func TestGetReceipts(t *testing.T) {
	runGetCases(t, sharedCases(), func(r *BlockCacheReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
		return r.GetReceipts(context.Background(), 1, 21000000, 0)
	})
}

func TestGetTraces(t *testing.T) {
	runGetCases(t, sharedCases(), func(r *BlockCacheReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
		return r.GetTraces(context.Background(), 1, 21000000, 0)
	})
}

func TestGetBlobs(t *testing.T) {
	runGetCases(t, sharedCases(), func(r *BlockCacheReaderWithFallback, _ *mockCacheReader, _ *mockS3Reader) (json.RawMessage, error) {
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
	if strings.HasPrefix(s3mock.lastKey, "1/") {
		t.Errorf("S3 key should NOT contain chain ID prefix, got %q", s3mock.lastKey)
	}
}

// TestGetMethods_WrongChainID verifies that all Get* methods reject a chainID
// that differs from the one the reader was constructed with.
func TestGetMethods_WrongChainID(t *testing.T) {
	redis := &mockCacheReader{}
	s3mock := &mockS3Reader{}
	// Reader is configured for chain 1 (Ethereum).
	reader := newTestReader(t, redis, s3mock)

	// Call each method with chain 43114 (Avalanche) — must be rejected.
	wrongChain := int64(43114)

	tests := []struct {
		name   string
		invoke func() (json.RawMessage, error)
	}{
		{"GetBlock", func() (json.RawMessage, error) {
			return reader.GetBlock(context.Background(), wrongChain, 21000000, 0)
		}},
		{"GetReceipts", func() (json.RawMessage, error) {
			return reader.GetReceipts(context.Background(), wrongChain, 21000000, 0)
		}},
		{"GetTraces", func() (json.RawMessage, error) {
			return reader.GetTraces(context.Background(), wrongChain, 21000000, 0)
		}},
		{"GetBlobs", func() (json.RawMessage, error) {
			return reader.GetBlobs(context.Background(), wrongChain, 21000000, 0)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.invoke()
			if err == nil {
				t.Fatal("expected chainID mismatch error, got nil")
			}
			if !strings.Contains(err.Error(), "chainID mismatch") {
				t.Errorf("expected 'chainID mismatch' in error, got: %v", err)
			}
			if got != nil {
				t.Errorf("expected nil data on error, got %s", got)
			}
			if redis.blockCalled || redis.receiptsCalled || redis.tracesCalled || redis.blobsCalled {
				t.Error("Redis must not be called when chainID does not match")
			}
			if s3mock.called {
				t.Error("S3 must not be called when chainID does not match")
			}
		})
	}
}
