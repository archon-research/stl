package block_meta_loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// tsBase makes each block's synthetic on-chain timestamp a deterministic function of its number,
// so a test can assert the loader wrote the right block_timestamp without an S3 fixture.
const tsBase = int64(1_700_000_000)

// mockS3Reader implements outbound.S3Reader. Only StreamFile is exercised; it returns plain JSON
// (the real adapter auto-decompresses .gz, so the port contract yields already-decompressed bytes).
type mockS3Reader struct {
	streamFn func(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

func (m *mockS3Reader) ListFiles(context.Context, string, string) ([]outbound.S3File, error) {
	return nil, nil
}
func (m *mockS3Reader) ListPrefix(context.Context, string, string) ([]string, error) { return nil, nil }
func (m *mockS3Reader) StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	return m.streamFn(ctx, bucket, key)
}

// streamTimestampByBlock returns a reader whose header timestamp encodes tsBase + blockNumber.
func streamTimestampByBlock(_ context.Context, _ string, key string) (io.ReadCloser, error) {
	parsed, ok := s3key.Parse(key)
	if !ok {
		return nil, fmt.Errorf("unparseable key %q", key)
	}
	body := fmt.Sprintf(`{"timestamp":"0x%x"}`, tsBase+parsed.BlockNumber)
	return io.NopCloser(strings.NewReader(body)), nil
}

// mockBlockMetaRepo implements outbound.BlockMetaRepository over an in-memory universe of pending
// blocks, honouring the keyset cursor + limit exactly as the SQL adapter would.
type mockBlockMetaRepo struct {
	universe  []outbound.BlockRef // sorted by (Number, Version)
	upserted  []outbound.BlockMetaRow
	upsertErr error
	calls     int
}

func (m *mockBlockMetaRepo) PendingBlocks(_ context.Context, _ int64, limit int, afterNumber int64, afterVersion int) ([]outbound.BlockRef, error) {
	m.calls++
	var out []outbound.BlockRef
	for _, b := range m.universe {
		if b.Number > afterNumber || (b.Number == afterNumber && b.Version > afterVersion) {
			out = append(out, b)
			if len(out) == limit {
				break
			}
		}
	}
	return out, nil
}

func (m *mockBlockMetaRepo) Upsert(_ context.Context, rows []outbound.BlockMetaRow) (int64, error) {
	if m.upsertErr != nil {
		return 0, m.upsertErr
	}
	m.upserted = append(m.upserted, rows...)
	// Consume from the universe so the keyset loop terminates as the real anti-join would.
	m.universe = m.universe[len(rows):]
	return int64(len(rows)), nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestService(t *testing.T, repo outbound.BlockMetaRepository, reader outbound.S3Reader, batch int) *Service {
	t.Helper()
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: batch}, repo, reader, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return svc
}

func TestRun_FillsAllPendingBlocksAcrossBatches(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{
		{Number: 10, Version: 0}, {Number: 20, Version: 0}, {Number: 20, Version: 1}, {Number: 30, Version: 0},
	}}
	reader := &mockS3Reader{streamFn: streamTimestampByBlock}
	svc := newTestService(t, repo, reader, 2) // batch size 2 -> multiple iterations + cursor advance

	total, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if total != 4 {
		t.Errorf("total upserted = %d, want 4", total)
	}
	if len(repo.upserted) != 4 {
		t.Fatalf("upserted rows = %d, want 4", len(repo.upserted))
	}
	// Every row carries the chain and the block-derived timestamp.
	for _, row := range repo.upserted {
		if row.ChainID != 1 {
			t.Errorf("row chain_id = %d, want 1", row.ChainID)
		}
		if want := tsBase + row.BlockNumber; row.BlockTimestamp.Unix() != want {
			t.Errorf("block %d/%d timestamp = %d, want %d", row.BlockNumber, row.BlockVersion, row.BlockTimestamp.Unix(), want)
		}
	}
	if repo.calls < 2 {
		t.Errorf("expected at least 2 PendingBlocks calls (batched), got %d", repo.calls)
	}
}

func TestRun_NoPendingBlocksIsNoop(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: nil}
	svc := newTestService(t, repo, &mockS3Reader{streamFn: streamTimestampByBlock}, 500)

	total, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if len(repo.upserted) != 0 {
		t.Errorf("upserted %d rows, want 0", len(repo.upserted))
	}
}

func TestRun_FailsHardOnMissingArchivedBlock(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 42, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		return nil, errors.New("NoSuchKey")
	}}
	svc := newTestService(t, repo, reader, 500)

	total, err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to fail hard on a missing archived block, got nil")
	}
	if !strings.Contains(err.Error(), "block 42/0") {
		t.Errorf("error = %v, want it to identify block 42/0", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0 (nothing upserted before the failure)", total)
	}
	if len(repo.upserted) != 0 {
		t.Errorf("upserted %d rows, want 0", len(repo.upserted))
	}
}

func TestRun_SurfacesUpsertError(t *testing.T) {
	repo := &mockBlockMetaRepo{
		universe:  []outbound.BlockRef{{Number: 1, Version: 0}},
		upsertErr: errors.New("deadlock detected"),
	}
	svc := newTestService(t, repo, &mockS3Reader{streamFn: streamTimestampByBlock}, 500)

	_, err := svc.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "upserting block_meta") {
		t.Fatalf("expected an upsert error, got %v", err)
	}
}

func TestRun_StopsOnCancelledContext(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 1, Version: 0}}}
	svc := newTestService(t, repo, &mockS3Reader{streamFn: streamTimestampByBlock}, 500)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	total, err := svc.Run(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if repo.calls != 0 {
		t.Errorf("expected no PendingBlocks calls after cancellation, got %d", repo.calls)
	}
}

func TestNew_Validation(t *testing.T) {
	repo := &mockBlockMetaRepo{}
	reader := &mockS3Reader{streamFn: streamTimestampByBlock}
	tests := []struct {
		name    string
		cfg     Config
		repo    outbound.BlockMetaRepository
		reader  outbound.S3Reader
		wantErr string
	}{
		{"valid", Config{ChainID: 1, Bucket: "b"}, repo, reader, ""},
		{"zero chain", Config{ChainID: 0, Bucket: "b"}, repo, reader, "chain id"},
		{"negative chain", Config{ChainID: -1, Bucket: "b"}, repo, reader, "chain id"},
		{"empty bucket", Config{ChainID: 1, Bucket: ""}, repo, reader, "bucket"},
		{"nil repo", Config{ChainID: 1, Bucket: "b"}, nil, reader, "repository"},
		{"nil reader", Config{ChainID: 1, Bucket: "b"}, repo, nil, "s3 reader"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg, tt.repo, tt.reader, testLogger())
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}

func TestNew_DefaultsBatchSize(t *testing.T) {
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: 0}, &mockBlockMetaRepo{}, &mockS3Reader{streamFn: streamTimestampByBlock}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.cfg.BatchSize != 500 {
		t.Errorf("BatchSize default = %d, want 500", svc.cfg.BatchSize)
	}
}
