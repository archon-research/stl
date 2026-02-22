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
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_backfill"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_position_tracker"
)

// failingReader is an io.ReadCloser whose Read always returns an error.
// It is used to exercise the io.ReadAll error path in processBlock.
type failingReader struct{}

func (r *failingReader) Read(p []byte) (int, error) {
	return 0, fmt.Errorf("simulated read error")
}

func (r *failingReader) Close() error { return nil }

// mockS3Reader is a configurable mock for outbound.S3Reader.
// streamFn, if set, is called for StreamFile; otherwise returns an empty JSON array.
// streamFn is set once at construction and never mutated, so no mutex is needed.
type mockS3Reader struct {
	streamFn func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

func (m *mockS3Reader) ListFiles(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error) {
	return nil, nil
}

func (m *mockS3Reader) ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
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
}

func (m *mockProcessor) ProcessReceipts(ctx context.Context, chainID, blockNumber int64, version int, receipts []sparklend_position_tracker.TransactionReceipt) error {
	m.mu.Lock()
	m.calls = append(m.calls, processCall{chainID: chainID, blockNumber: blockNumber})
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

func TestBuildVersionMap(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want map[int64]int
	}{
		{
			name: "empty input returns empty map",
			keys: nil,
			want: map[int64]int{},
		},
		{
			name: "single key version 0",
			keys: []string{"0-999/100_0_receipts.json.gz"},
			want: map[int64]int{100: 0},
		},
		{
			name: "single key version 1",
			keys: []string{"0-999/100_1_receipts.json.gz"},
			want: map[int64]int{100: 1},
		},
		{
			name: "keeps highest version when multiple exist",
			keys: []string{
				"0-999/100_0_receipts.json.gz",
				"0-999/100_1_receipts.json.gz",
				"0-999/100_2_receipts.json.gz",
			},
			want: map[int64]int{100: 2},
		},
		{
			name: "multiple blocks across same partition",
			keys: []string{
				"0-999/100_0_receipts.json.gz",
				"0-999/200_1_receipts.json.gz",
			},
			want: map[int64]int{100: 0, 200: 1},
		},
		{
			name: "ignores non-receipts files",
			keys: []string{
				"0-999/100_0_block.json.gz",
				"0-999/100_0_traces.json.gz",
				"0-999/100_1_receipts.json.gz",
			},
			want: map[int64]int{100: 1},
		},
		{
			name: "ignores malformed keys",
			keys: []string{
				"not-a-valid-key",
				"0-999/abc_0_receipts.json.gz",
				"0-999/100_xyz_receipts.json.gz",
				"0-999/100_1_receipts.json.gz",
			},
			want: map[int64]int{100: 1},
		},
		{
			name: "blocks across multiple partitions",
			keys: []string{
				"0-999/500_0_receipts.json.gz",
				"1000-1999/1500_2_receipts.json.gz",
			},
			want: map[int64]int{500: 0, 1500: 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sparklend_backfill.BuildVersionMap(tt.keys)
			if len(got) != len(tt.want) {
				t.Fatalf("map length: got %d, want %d\ngot:  %v\nwant: %v", len(got), len(tt.want), got, tt.want)
			}
			for blockNum, wantVer := range tt.want {
				gotVer, ok := got[blockNum]
				if !ok {
					t.Errorf("missing block %d in result", blockNum)
					continue
				}
				if gotVer != wantVer {
					t.Errorf("block %d: got version %d, want %d", blockNum, gotVer, wantVer)
				}
			}
		})
	}
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
			name:      "zero concurrency defaults to 10",
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
			buildS3:   func() *mockS3Reader { return &mockS3Reader{} },
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
			buildS3:   func() *mockS3Reader { return &mockS3Reader{} },
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
				// Fail on the 2nd call regardless of key format.
				var callCount atomic.Int32
				return &mockS3Reader{
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
			buildS3:   func() *mockS3Reader { return &mockS3Reader{} },
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
