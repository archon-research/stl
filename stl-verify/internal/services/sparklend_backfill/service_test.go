package sparklend_backfill_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_backfill"
)

// failingReader is an io.ReadCloser whose Read always returns an error.
// It is used to exercise the io.ReadAll error path in processBlockFromS3.
type failingReader struct{}

func (r *failingReader) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("simulated read error")
}

func (r *failingReader) Close() error { return nil }

// mockS3Reader is a configurable mock for outbound.S3Reader.
// streamFn, if set, is called for StreamFile; otherwise returns an empty JSON array.
// listPrefixFn, if set, is called for ListPrefix; otherwise returns nil, nil.
// Both are set once at construction and never mutated, so no mutex is needed on the fns themselves.
type mockS3Reader struct {
	streamFn     func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	listPrefixFn func(ctx context.Context, bucket, prefix string) ([]string, error)
}

func (m *mockS3Reader) ListFiles(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error) {
	return nil, nil
}

func (m *mockS3Reader) ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
	if m.listPrefixFn != nil {
		return m.listPrefixFn(ctx, bucket, prefix)
	}
	return nil, nil
}

func (m *mockS3Reader) StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	if m.streamFn != nil {
		return m.streamFn(ctx, bucket, key)
	}
	// Default: valid empty JSON array.
	return io.NopCloser(strings.NewReader("[]")), nil
}

// mockProcessor records all (chainID, blockNumber) pairs it is called with.
// errFn, if set, is called to produce an error for a given call.
type mockProcessor struct {
	mu    sync.Mutex
	calls []processCall
	errFn func(chainID, blockNumber int64) error
}

type processCall struct {
	chainID     int64
	blockNumber int64
	version     int
}

func (m *mockProcessor) ProcessReceipts(ctx context.Context, chainID, blockNumber int64, version int, receipts []sparklend.TransactionReceipt) error {
	m.mu.Lock()
	m.calls = append(m.calls, processCall{chainID: chainID, blockNumber: blockNumber, version: version})
	m.mu.Unlock()
	if m.errFn != nil {
		return m.errFn(chainID, blockNumber)
	}
	return nil
}

func (m *mockProcessor) calledWith() []processCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]processCall, len(m.calls))
	copy(out, m.calls)
	return out
}

func newTestService(t *testing.T, s3 outbound.S3Reader, proc sparklend_backfill.ReceiptProcessor) *sparklend_backfill.Service {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, err := sparklend_backfill.NewService(
		sparklend_backfill.Config{Concurrency: 2, Logger: logger},
		s3,
		proc,
		"test-bucket",
		1,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func TestNewService_Validation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	tests := []struct {
		name      string
		config    sparklend_backfill.Config
		s3Reader  outbound.S3Reader
		processor sparklend_backfill.ReceiptProcessor
		bucket    string
		chainID   int64
		wantErr   bool
	}{
		{
			name:      "nil logger",
			config:    sparklend_backfill.Config{Concurrency: 1, Logger: nil},
			s3Reader:  &mockS3Reader{},
			processor: &mockProcessor{},
			bucket:    "test-bucket",
			chainID:   1,
			wantErr:   true,
		},
		{
			name:      "nil s3Reader",
			config:    sparklend_backfill.Config{Concurrency: 1, Logger: logger},
			s3Reader:  nil,
			processor: &mockProcessor{},
			bucket:    "test-bucket",
			chainID:   1,
			wantErr:   true,
		},
		{
			name:      "nil processor",
			config:    sparklend_backfill.Config{Concurrency: 1, Logger: logger},
			s3Reader:  &mockS3Reader{},
			processor: nil,
			bucket:    "test-bucket",
			chainID:   1,
			wantErr:   true,
		},
		{
			name:      "empty bucket",
			config:    sparklend_backfill.Config{Concurrency: 1, Logger: logger},
			s3Reader:  &mockS3Reader{},
			processor: &mockProcessor{},
			bucket:    "",
			chainID:   1,
			wantErr:   true,
		},
		{
			name:      "zero concurrency defaults to 1",
			config:    sparklend_backfill.Config{Concurrency: 0, Logger: logger},
			s3Reader:  &mockS3Reader{},
			processor: &mockProcessor{},
			bucket:    "test-bucket",
			chainID:   1,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := sparklend_backfill.NewService(tt.config, tt.s3Reader, tt.processor, tt.bucket, tt.chainID)
			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.wantErr && svc == nil {
				t.Errorf("expected non-nil service")
			}
		})
	}
}

func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		fromBlock  int64
		toBlock    int64
		cancelCtx  bool
		buildS3    func() *mockS3Reader
		buildProc  func() *mockProcessor
		wantErr    bool
		checkErr   func(t *testing.T, err error)
		checkCalls func(t *testing.T, calls []processCall)
	}{
		{
			name:      "success: multiple blocks processed",
			fromBlock: 100,
			toBlock:   104,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{
							"0-999/100_0_receipts.json.gz",
							"0-999/101_0_receipts.json.gz",
							"0-999/102_0_receipts.json.gz",
							"0-999/103_0_receipts.json.gz",
							"0-999/104_0_receipts.json.gz",
						}, nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   false,
			checkCalls: func(t *testing.T, calls []processCall) {
				t.Helper()
				if len(calls) != 5 {
					t.Errorf("expected 5 processor calls, got %d", len(calls))
				}
				seen := make(map[int64]bool)
				for _, c := range calls {
					if c.chainID != 1 {
						t.Errorf("unexpected chainID %d", c.chainID)
					}
					seen[c.blockNumber] = true
				}
				for blk := int64(100); blk <= 104; blk++ {
					if !seen[blk] {
						t.Errorf("block %d was not processed", blk)
					}
				}
			},
		},
		{
			name:      "single block range processes exactly one block",
			fromBlock: 200,
			toBlock:   200,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{"0-999/200_0_receipts.json.gz"}, nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   false,
			checkCalls: func(t *testing.T, calls []processCall) {
				t.Helper()
				if len(calls) != 1 {
					t.Errorf("expected 1 processor call, got %d", len(calls))
					return
				}
				if calls[0].blockNumber != 200 {
					t.Errorf("expected block 200, got %d", calls[0].blockNumber)
				}
				if calls[0].chainID != 1 {
					t.Errorf("expected chainID 1, got %d", calls[0].chainID)
				}
			},
		},
		{
			name:      "fromBlock > toBlock returns error immediately",
			fromBlock: 200,
			toBlock:   100,
			buildS3:   func() *mockS3Reader { return &mockS3Reader{} },
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				want := "toBlock (100) must be >= fromBlock (200)"
				if err == nil || err.Error() != want {
					t.Errorf("expected error %q, got %v", want, err)
				}
			},
			checkCalls: func(t *testing.T, calls []processCall) {
				t.Helper()
				if len(calls) != 0 {
					t.Errorf("expected no processor calls, got %d", len(calls))
				}
			},
		},
		{
			name:      "cancelled context returns cancellation error",
			fromBlock: 1,
			toBlock:   1000,
			cancelCtx: true,
			buildS3:   func() *mockS3Reader { return &mockS3Reader{} },
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, context.Canceled) {
					t.Errorf("expected context.Canceled wrapped in error, got %v", err)
				}
			},
		},
		{
			name:      "S3 error for a block causes Run to return an error",
			fromBlock: 500,
			toBlock:   502,
			buildS3: func() *mockS3Reader {
				var callCount atomic.Int32
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{
							"0-999/500_0_receipts.json.gz",
							"0-999/501_0_receipts.json.gz",
							"0-999/502_0_receipts.json.gz",
						}, nil
					},
					streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
						if callCount.Add(1) == 2 {
							return nil, fmt.Errorf("simulated S3 error")
						}
						return io.NopCloser(strings.NewReader("[]")), nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
		},
		{
			name:      "S3 ReadCloser read error causes Run to return an error",
			fromBlock: 600,
			toBlock:   600,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{"0-999/600_0_receipts.json.gz"}, nil
					},
					streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
						return &failingReader{}, nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
		},
		{
			name:      "processor error for a block causes Run to return an error",
			fromBlock: 300,
			toBlock:   302,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{
							"0-999/300_0_receipts.json.gz",
							"0-999/301_0_receipts.json.gz",
							"0-999/302_0_receipts.json.gz",
						}, nil
					},
				}
			},
			buildProc: func() *mockProcessor {
				return &mockProcessor{
					errFn: func(chainID, blockNumber int64) error {
						if blockNumber == 301 {
							return errors.New("processor failure")
						}
						return nil
					},
				}
			},
			wantErr: true,
		},
		{
			name:      "uses version from scan — version 0",
			fromBlock: 100,
			toBlock:   100,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{"0-999/100_0_receipts.json.gz"}, nil
					},
					streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
						// Assert the key uses version 0
						wantKey := "0-999/100_0_receipts.json.gz"
						if key != wantKey {
							return nil, fmt.Errorf("unexpected key %q, want %q", key, wantKey)
						}
						return io.NopCloser(strings.NewReader("[]")), nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   false,
			checkCalls: func(t *testing.T, calls []processCall) {
				t.Helper()
				if len(calls) != 1 {
					t.Fatalf("expected 1 call, got %d", len(calls))
				}
				if calls[0].version != 0 {
					t.Errorf("expected version 0, got %d", calls[0].version)
				}
			},
		},
		{
			name:      "uses highest version when multiple exist",
			fromBlock: 100,
			toBlock:   100,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{
							"0-999/100_0_receipts.json.gz",
							"0-999/100_1_receipts.json.gz",
							"0-999/100_2_receipts.json.gz",
						}, nil
					},
					streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
						wantKey := "0-999/100_2_receipts.json.gz"
						if key != wantKey {
							return nil, fmt.Errorf("unexpected key %q, want %q", key, wantKey)
						}
						return io.NopCloser(strings.NewReader("[]")), nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   false,
			checkCalls: func(t *testing.T, calls []processCall) {
				t.Helper()
				if len(calls) != 1 {
					t.Fatalf("expected 1 call, got %d", len(calls))
				}
				if calls[0].version != 2 {
					t.Errorf("expected version 2, got %d", calls[0].version)
				}
			},
		},
		{
			name:      "block absent from S3 causes Run to return error",
			fromBlock: 100,
			toBlock:   102,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						// Only block 101 exists in S3; blocks 100 and 102 are missing
						return []string{"0-999/101_0_receipts.json.gz"}, nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), "not found in S3") {
					t.Errorf("expected 'not found in S3' error, got: %v", err)
				}
			},
		},
		{
			name:      "ScanVersions error causes Run to return error",
			fromBlock: 100,
			toBlock:   100,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return nil, fmt.Errorf("S3 scan failed")
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), "S3 scan failed") {
					t.Errorf("expected S3 scan error, got: %v", err)
				}
			},
		},
		{
			name:      "single block absent from S3 returns error",
			fromBlock: 100,
			toBlock:   100,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						// No receipts for block 100 in S3
						return nil, nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil || !strings.Contains(err.Error(), "block 100 not found in S3") {
					t.Errorf("expected 'block 100 not found in S3' error, got: %v", err)
				}
			},
		},
		{
			name:      "S3 block uses S3 version",
			fromBlock: 100,
			toBlock:   100,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
						return []string{
							"0-999/100_2_receipts.json.gz", // block 100 in S3 with version 2
						}, nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   false,
			checkCalls: func(t *testing.T, calls []processCall) {
				t.Helper()
				if len(calls) != 1 {
					t.Fatalf("expected 1 processor call, got %d", len(calls))
				}
				if calls[0].version != 2 {
					t.Errorf("block 100: expected version 2 (from S3), got %d", calls[0].version)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s3 := tt.buildS3()
			proc := tt.buildProc()
			svc := newTestService(t, s3, proc)

			ctx := context.Background()
			if tt.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			err := svc.Run(ctx, tt.fromBlock, tt.toBlock)

			if tt.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.checkErr != nil {
				tt.checkErr(t, err)
			}
			if tt.checkCalls != nil {
				tt.checkCalls(t, proc.calledWith())
			}
		})
	}
}

func TestRun_FailFastOnFirstProcessingError(t *testing.T) {
	s3 := &mockS3Reader{
		listPrefixFn: func(ctx context.Context, bucket, prefix string) ([]string, error) {
			return []string{
				"0-999/300_0_receipts.json.gz",
				"0-999/301_0_receipts.json.gz",
				"0-999/302_0_receipts.json.gz",
			}, nil
		},
	}

	proc := &mockProcessor{
		errFn: func(chainID, blockNumber int64) error {
			if blockNumber == 300 {
				return errors.New("processor failure")
			}
			return nil
		},
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc, err := sparklend_backfill.NewService(
		sparklend_backfill.Config{Concurrency: 1, Logger: logger},
		s3,
		proc,
		"test-bucket",
		1,
	)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	err = svc.Run(context.Background(), 300, 302)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	calls := proc.calledWith()
	if len(calls) != 1 {
		t.Fatalf("expected fail-fast processing to stop after first error, got %d calls", len(calls))
	}
	if calls[0].blockNumber != 300 {
		t.Fatalf("expected first failed block 300, got %d", calls[0].blockNumber)
	}
}
