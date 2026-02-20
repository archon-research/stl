package sparklend_backfill_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_backfill"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_position_tracker"
)

// mockS3Reader is a configurable mock for outbound.S3Reader.
// streamFn, if set, is called for StreamFile; otherwise returns empty JSON array.
// errorOnBlock, if non-zero, causes StreamFile to return an error for that block number.
type mockS3Reader struct {
	mu           sync.Mutex
	errorOnBlock int64
	streamFn     func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

func (m *mockS3Reader) ListFiles(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error) {
	return nil, nil
}

func (m *mockS3Reader) ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
	return nil, nil
}

func (m *mockS3Reader) StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.streamFn != nil {
		return m.streamFn(ctx, bucket, key)
	}
	// Return a valid empty JSON array for all blocks.
	return io.NopCloser(strings.NewReader("[]")), nil
}

// mockProcessor records all (chainID, blockNumber) pairs it is called with.
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
				// All calls must be for chainID=1 and blocks 100–104.
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
			name:      "fromBlock > toBlock returns error immediately",
			fromBlock: 200,
			toBlock:   100,
			buildS3:   func() *mockS3Reader { return &mockS3Reader{} },
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("expected an error for fromBlock > toBlock")
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
			buildS3:   func() *mockS3Reader { return &mockS3Reader{} },
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if !errors.Is(err, context.Canceled) {
					t.Errorf("expected context.Canceled, got %v", err)
				}
			},
		},
		{
			name:      "S3 error for a block causes Run to return an error",
			fromBlock: 500,
			toBlock:   502,
			buildS3: func() *mockS3Reader {
				return &mockS3Reader{
					streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
						// Fail the middle block.
						if strings.Contains(key, fmt.Sprintf("%d_1_receipts", 501)) {
							return nil, errors.New("s3 read failure")
						}
						return io.NopCloser(strings.NewReader("[]")), nil
					},
				}
			},
			buildProc: func() *mockProcessor { return &mockProcessor{} },
			wantErr:   true,
			checkErr: func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("expected a non-nil error when S3 fails")
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
			if tt.name == "cancelled context returns cancellation error" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel() // cancel immediately before Run
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
