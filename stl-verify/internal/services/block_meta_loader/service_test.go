package block_meta_loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	metricsdk "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

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

// fakeArchive implements outbound.ArchiveReader. highestVersionFn is nil in every test that never
// hits the archive-version fallback (the requested version always reads successfully), matching a
// real ArchiveReader that a run pinging cleanly at startup never needs to call again mid-run.
// BlockHashAt is unused by this service — resolving a timestamp needs no hash proof, unlike
// internal/pkg/blockversion's replay use case — so it is a fixed stub, never exercised.
type fakeArchive struct {
	highestVersionFn func(ctx context.Context, blockNumber int64) (int, bool, error)
}

func (a *fakeArchive) HighestVersion(ctx context.Context, blockNumber int64) (int, bool, error) {
	if a.highestVersionFn == nil {
		return 0, false, nil
	}
	return a.highestVersionFn(ctx, blockNumber)
}

func (a *fakeArchive) BlockHashAt(context.Context, int64, int) (string, bool, error) {
	return "", false, fmt.Errorf("BlockHashAt: unused by block_meta_loader, must not be called")
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
// blocks. Like the SQL adapter, the universe is snapshotted when the work list is opened and paged
// with a keyset cursor, so a block that appears mid-run is not picked up until the next run.
type mockBlockMetaRepo struct {
	universe  []outbound.BlockRef // sorted by (Number, Version)
	upserted  []outbound.BlockMetaRow
	upsertErr error
	openErr   error
	nextErr   error
	calls     int
	opened    int
	closed    int
}

type mockWorkList struct {
	repo  *mockBlockMetaRepo
	snap  []outbound.BlockRef
	after outbound.BlockRef
}

func (m *mockBlockMetaRepo) OpenWorkList(_ context.Context, _ int64, _ int64) (outbound.BlockWorkList, error) {
	if m.openErr != nil {
		return nil, m.openErr
	}
	m.opened++
	snap := make([]outbound.BlockRef, len(m.universe))
	copy(snap, m.universe)
	return &mockWorkList{repo: m, snap: snap, after: outbound.BlockRef{Number: -1, Version: -1}}, nil
}

func (w *mockWorkList) Next(_ context.Context, limit int) ([]outbound.BlockRef, error) {
	if w.repo.nextErr != nil {
		return nil, w.repo.nextErr
	}
	w.repo.calls++
	var out []outbound.BlockRef
	for _, b := range w.snap {
		if b.Number > w.after.Number || (b.Number == w.after.Number && b.Version > w.after.Version) {
			out = append(out, b)
			if len(out) == limit {
				break
			}
		}
	}
	if len(out) > 0 {
		w.after = out[len(out)-1]
	}
	return out, nil
}

func (w *mockWorkList) Close(context.Context) { w.repo.closed++ }

func (m *mockBlockMetaRepo) Upsert(_ context.Context, rows []outbound.BlockMetaRow) (int64, error) {
	if m.upsertErr != nil {
		return 0, m.upsertErr
	}
	m.upserted = append(m.upserted, rows...)
	// Consume from the universe, as the real anti-join against block_meta does, so a service that
	// re-opened the work list per batch terminates with a wrong open count instead of spinning.
	if len(rows) <= len(m.universe) {
		m.universe = m.universe[len(rows):]
	}
	return int64(len(rows)), nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestService builds a Service with a fakeArchive that never resolves anything (found=false),
// matching every test that never drives a block's requested read into a 404 in the first place.
func newTestService(t *testing.T, repo outbound.BlockMetaRepository, reader outbound.S3Reader, batch int) *Service {
	t.Helper()
	return newTestServiceWithArchive(t, repo, reader, &fakeArchive{}, batch)
}

func newTestServiceWithArchive(t *testing.T, repo outbound.BlockMetaRepository, reader outbound.S3Reader, archive outbound.ArchiveReader, batch int) *Service {
	t.Helper()
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: batch}, repo, reader, archive, testLogger())
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
	archive := &fakeArchive{}
	tests := []struct {
		name    string
		cfg     Config
		repo    outbound.BlockMetaRepository
		reader  outbound.S3Reader
		archive outbound.ArchiveReader
		wantErr string
	}{
		{"valid", Config{ChainID: 1, Bucket: "b"}, repo, reader, archive, ""},
		{"zero chain", Config{ChainID: 0, Bucket: "b"}, repo, reader, archive, "chain id"},
		{"negative chain", Config{ChainID: -1, Bucket: "b"}, repo, reader, archive, "chain id"},
		{"empty bucket", Config{ChainID: 1, Bucket: ""}, repo, reader, archive, "bucket"},
		{"nil repo", Config{ChainID: 1, Bucket: "b"}, nil, reader, archive, "repository"},
		{"nil reader", Config{ChainID: 1, Bucket: "b"}, repo, nil, archive, "s3 reader"},
		{"nil archive", Config{ChainID: 1, Bucket: "b"}, repo, reader, nil, "archive reader"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg, tt.repo, tt.reader, tt.archive, testLogger())
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
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: 0}, &mockBlockMetaRepo{}, &mockS3Reader{streamFn: streamTimestampByBlock}, &fakeArchive{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.cfg.BatchSize != 500 {
		t.Errorf("BatchSize default = %d, want 500", svc.cfg.BatchSize)
	}
}

// The work list is opened once per run and closed, even when the run fails: the referenced set is
// expensive to compute (measured ~7s per evaluation against staging's 1.4M referenced blocks), so
// re-evaluating it per batch is what this shape exists to avoid.
func TestRunOpensTheWorkListOnceAndAlwaysClosesIt(t *testing.T) {
	universe := []outbound.BlockRef{{Number: 1}, {Number: 2}, {Number: 3}, {Number: 4}, {Number: 5}}
	for _, c := range []struct {
		name    string
		repo    *mockBlockMetaRepo
		wantErr bool
	}{
		{name: "a clean run", repo: &mockBlockMetaRepo{universe: universe}},
		{name: "a run whose upsert fails",
			repo:    &mockBlockMetaRepo{universe: universe, upsertErr: errors.New("boom")},
			wantErr: true},
	} {
		t.Run(c.name, func(t *testing.T) {
			svc := newTestService(t, c.repo, &mockS3Reader{streamFn: streamTimestampByBlock}, 2)
			_, err := svc.Run(context.Background())
			if (err != nil) != c.wantErr {
				t.Fatalf("Run error = %v, wantErr %v", err, c.wantErr)
			}
			if c.repo.opened != 1 {
				t.Errorf("work list opened %d times, want exactly 1 -- the referenced set must not be re-evaluated per batch", c.repo.opened)
			}
			if c.repo.closed != 1 {
				t.Errorf("work list closed %d times, want 1 -- it holds a pooled connection and an open transaction", c.repo.closed)
			}
		})
	}
}

// A failure opening the work list is reported, not silently treated as an empty run.
func TestRunReportsAWorkListOpenFailure(t *testing.T) {
	repo := &mockBlockMetaRepo{openErr: errors.New("no connection")}
	svc := newTestService(t, repo, &mockS3Reader{streamFn: streamTimestampByBlock}, 2)
	n, err := svc.Run(context.Background())
	if err == nil {
		t.Fatalf("Run succeeded with %d rows; want the open failure surfaced", n)
	}
	if !strings.Contains(err.Error(), "opening the work list") {
		t.Errorf("error = %v; want it to name the work-list open", err)
	}
	if repo.closed != 0 {
		t.Errorf("closed %d work lists after a failed open, want 0", repo.closed)
	}
}

// A hole no longer stops the chain. The list is paged ascending and the read used to return on the
// first absent object, so one deep-tail hole left every later block unloaded and every re-run
// stopped in the same place. The misses are carried to the end instead: what could be read is
// committed, and the run still fails naming what was absent.
func TestRun_AbsentObjectsAreCarriedToTheEndNotFatalMidRun(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{
		{Number: 10, Version: 0}, {Number: 20, Version: 0}, {Number: 30, Version: 0}, {Number: 40, Version: 0},
	}}
	// Block 20 is the hole; everything after it must still load.
	reader := &mockS3Reader{streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
		parsed, ok := s3key.Parse(key)
		if !ok {
			return nil, fmt.Errorf("unparseable key %q", key)
		}
		if parsed.BlockNumber == 20 {
			return nil, outbound.ErrObjectNotFound
		}
		return streamTimestampByBlock(ctx, bucket, key)
	}}
	svc := newTestService(t, repo, reader, 2)

	total, err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("want a failure naming the absent block, got none")
	}
	if !strings.Contains(err.Error(), "20/0") {
		t.Errorf("error %q does not name the absent block", err)
	}
	if total != 3 {
		t.Errorf("loaded %d rows, want 3 — the blocks after the hole must still be committed", total)
	}
	for _, r := range repo.upserted {
		if r.BlockNumber == 20 {
			t.Error("the absent block was upserted")
		}
	}
}

// A transport error is not a miss: it says nothing about whether the block exists, so it must fail
// the run rather than be recorded as an absent object and leave a real hole hidden in a list.
func TestRun_TransportErrorFailsRatherThanCountingAsAMiss(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 10, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		return nil, fmt.Errorf("connection reset")
	}}
	svc := newTestService(t, repo, reader, 2)

	if _, err := svc.Run(context.Background()); err == nil || strings.Contains(err.Error(), "absent from the archive") {
		t.Fatalf("want the transport error surfaced, got %v", err)
	}
}

// Concurrency is the reason the run fits in an hour rather than seven, so the reads have to actually
// overlap. Asserted by holding every read until the expected number are in flight at once: a
// sequential implementation never reaches the barrier and the test times out on its own context.
func TestRun_ReadsWithinABatchOverlap(t *testing.T) {
	const batch = 8
	universe := make([]outbound.BlockRef, 0, batch)
	for i := range batch {
		universe = append(universe, outbound.BlockRef{Number: int64(10 + i), Version: 0})
	}
	repo := &mockBlockMetaRepo{universe: universe}

	var mu sync.Mutex
	inFlight, peak := 0, 0
	release := make(chan struct{})
	reader := &mockS3Reader{streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		reached := inFlight >= batch
		mu.Unlock()
		if reached {
			close(release) // everyone is in flight; let them all finish
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		mu.Lock()
		inFlight--
		mu.Unlock()
		return streamTimestampByBlock(ctx, bucket, key)
	}}

	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: batch, Concurrency: batch}, repo, reader, &fakeArchive{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := svc.Run(ctx); err != nil {
		t.Fatalf("Run: %v (a sequential reader never reaches the barrier)", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if peak < batch {
		t.Errorf("peak concurrent reads = %d, want %d", peak, batch)
	}
}

// nextErr was declared and honoured by the mock but never set, so Run's "loading pending blocks"
// branch was unexercised: a paging failure mid-run has to surface, not read as an empty page and
// end the run reporting success.
func TestRun_PagingFailureSurfaces(t *testing.T) {
	repo := &mockBlockMetaRepo{
		universe: []outbound.BlockRef{{Number: 10, Version: 0}},
		nextErr:  fmt.Errorf("connection reset"),
	}
	reader := &mockS3Reader{streamFn: streamTimestampByBlock}
	svc := newTestService(t, repo, reader, 2)

	total, err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("a paging failure ended the run cleanly; a partial pass would look complete")
	}
	if !strings.Contains(err.Error(), "loading pending blocks") {
		t.Errorf("error %q does not name the paging step", err)
	}
	if total != 0 {
		t.Errorf("reported %d rows loaded after a paging failure, want 0", total)
	}
}

// The growth tripwire reads this counter, so it has to carry the run's real pending-set size and it
// has to exist before the first run: an unseeded counter first appears at its first increment, and
// rate() never observes the 0->1.
func TestRun_RecordsTheWorkListRowsItPages(t *testing.T) {
	reader := metricsdk.NewManualReader()
	mp := metricsdk.NewMeterProvider(metricsdk.WithReader(reader))
	prev := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prev)
		_ = mp.Shutdown(context.Background())
	})

	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{
		{Number: 10, Version: 0}, {Number: 20, Version: 0}, {Number: 30, Version: 0},
	}}
	// New resolves the global meter, so it must run after SetMeterProvider above.
	svc := newTestService(t, repo, &mockS3Reader{streamFn: streamTimestampByBlock}, 2)

	if got, ok := collectPagedRows(t, reader); !ok || got != 0 {
		t.Fatalf("before the run the counter reads %d (present=%t), want 0 and present", got, ok)
	}
	if _, err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Three blocks over two batches: the counter is the pending set, not the number of batches.
	if got, ok := collectPagedRows(t, reader); !ok || got != 3 {
		t.Errorf("block_meta.worklist.rows.paged = %d (present=%t), want 3", got, ok)
	}
}

// A December raw-block backfill hardcoded key version 1 for a swath of deep history regardless of
// what a referencing table's own block_version column says, so a read pinned to the requested
// version 404s even though the archive holds the block under a different version. This must fail on
// the code before it: without the fallback, a 404 at the requested version was reported as an
// absent object with no second attempt.
func TestRun_RecoversAVersionMismatchFromTheArchive(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 100, Version: 0}}}
	reader := &mockS3Reader{
		streamFn: func(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
			parsed, ok := s3key.Parse(key)
			if !ok {
				return nil, fmt.Errorf("unparseable key %q", key)
			}
			// Only version 1 is actually archived; the work list (mirroring the source
			// table's own bookkeeping) still asks for version 0.
			if parsed.Version != 1 {
				return nil, outbound.ErrObjectNotFound
			}
			return streamTimestampByBlock(ctx, bucket, key)
		},
	}
	archive := &fakeArchive{highestVersionFn: func(_ context.Context, blockNumber int64) (int, bool, error) {
		if blockNumber != 100 {
			t.Fatalf("resolved block %d, want 100", blockNumber)
		}
		return 1, true, nil
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	total, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v (want the version mismatch recovered via the archive)", err)
	}
	if total != 1 {
		t.Fatalf("total = %d, want 1", total)
	}
	if len(repo.upserted) != 1 {
		t.Fatalf("upserted %d rows, want 1", len(repo.upserted))
	}
	got := repo.upserted[0]
	if got.BlockVersion != 1 {
		t.Errorf("BlockVersion = %d, want 1 (the archive's real version, not the requested 0)", got.BlockVersion)
	}
	if want := tsBase + 100; got.BlockTimestamp.Unix() != want {
		t.Errorf("BlockTimestamp = %d, want %d", got.BlockTimestamp.Unix(), want)
	}
}

// A height the archive holds nothing for at all is still a genuine hole, not a version mismatch —
// the fallback must not invent occupancy the archive does not report.
func TestRun_GenuineArchiveHoleStillReportsAsAMiss(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 200, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		return nil, outbound.ErrObjectNotFound
	}}
	archive := &fakeArchive{} // highestVersionFn nil -> found=false, matching "nothing archived"
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	total, err := svc.Run(context.Background())
	requireReportedAsMiss(t, err, "200/0")
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
}

// requireReportedAsMiss asserts err is Run's own miss-aggregation error naming ref, not some other
// hard failure that happens to mention the same block/version pair: readOne's non-miss error paths
// ("resolving the archived version", "at resolved version") also embed the pair, so a bare substring
// check on ref alone cannot tell a miss from a bug that turned it into a hard failure.
func requireReportedAsMiss(t *testing.T, err error, ref string) {
	t.Helper()
	if err == nil {
		t.Fatal("want a failure naming the absent block, got none")
	}
	if !strings.Contains(err.Error(), "referenced block(s) absent from the archive") || !strings.Contains(err.Error(), ref) {
		t.Fatalf("error %q is not Run's miss-aggregation error naming %s", err, ref)
	}
}

// The archive resolving to the SAME version that already 404ed is also a genuine hole: there is
// nothing else to try, and readAt must not be called a second time at an identical coordinate.
func TestRun_ArchiveAgreeingWithTheFailedReadIsAMissNotARetry(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 250, Version: 0}}}
	var reads int
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		reads++
		return nil, outbound.ErrObjectNotFound
	}}
	archive := &fakeArchive{highestVersionFn: func(context.Context, int64) (int, bool, error) {
		return 0, true, nil // agrees with the version already requested and already absent
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	total, err := svc.Run(context.Background())
	requireReportedAsMiss(t, err, "250/0")
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
	if reads != 1 {
		t.Errorf("StreamFile called %d times, want exactly 1 (no retry at an identical version)", reads)
	}
}

// A failure resolving the archive's version says nothing about whether the block exists, so it must
// surface as a hard error rather than be swallowed as a miss.
func TestRun_ArchiveResolutionFailureDuringFallbackSurfaces(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 300, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		return nil, outbound.ErrObjectNotFound
	}}
	archive := &fakeArchive{highestVersionFn: func(context.Context, int64) (int, bool, error) {
		return 0, false, fmt.Errorf("connection reset")
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	if _, err := svc.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "resolving the archived version") {
		t.Fatalf("want the resolution failure surfaced naming the resolution step, got %v", err)
	}
}

// The version the archive resolves to can hold receipts/traces with no block object at all — a
// raw-block-bulk-downloader partial upload, not a corrupted read. This must still be a miss, not a
// hard failure: treating it as a failure would stall an entire chain's load on an ordinary partial-
// archive state the codebase already anticipates elsewhere (internal/pkg/blockversion).
func TestRun_ResolvedVersionWithNoBlockObjectIsAMissNotAFailure(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 400, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		return nil, outbound.ErrObjectNotFound // every version: the resolved one holds no block object
	}}
	archive := &fakeArchive{highestVersionFn: func(context.Context, int64) (int, bool, error) {
		return 1, true, nil // occupied by receipts/traces only
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	total, err := svc.Run(context.Background())
	requireReportedAsMiss(t, err, "400/0")
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
}

func collectPagedRows(t *testing.T, r *metricsdk.ManualReader) (int64, bool) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "block_meta.worklist.rows.paged" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric is %T, want Sum[int64]", m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total, true
		}
	}
	return 0, false
}
