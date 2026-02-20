package sparklend_backfill_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_backfill"
	"github.com/archon-research/stl/stl-verify/internal/services/sparklend_position_tracker"
)

type mockS3Reader struct{}

func (m *mockS3Reader) ListFiles(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error) {
	return nil, nil
}

func (m *mockS3Reader) ListPrefix(ctx context.Context, bucket, prefix string) ([]string, error) {
	return nil, nil
}

func (m *mockS3Reader) StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return nil, nil
}

type mockProcessor struct{}

func (m *mockProcessor) ProcessReceipts(ctx context.Context, chainID, blockNumber int64, version int, receipts []sparklend_position_tracker.TransactionReceipt) error {
	return nil
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
