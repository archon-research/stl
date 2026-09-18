package block_meta_loader

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
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
	universe   []outbound.BlockRef // sorted by (Number, Version)
	loaded     map[outbound.BlockRef]bool
	upserted   []outbound.BlockMetaRow
	upsertErr  error
	openErr    error
	nextErr    error
	headMargin int64
	calls      int
	opened     int
	closed     int
}

type mockWorkList struct {
	repo  *mockBlockMetaRepo
	snap  []outbound.BlockRef
	after outbound.BlockRef
}

func (m *mockBlockMetaRepo) OpenWorkList(_ context.Context, _ int64, headMargin int64) (outbound.BlockWorkList, error) {
	if m.openErr != nil {
		return nil, m.openErr
	}
	m.opened++
	m.headMargin = headMargin
	// The real OpenWorkList enumerates the referenced set and then DELETEs every row block_meta
	// already holds at that exact (chain_id, block_number, block_version), so the filter belongs
	// here, at open time -- not in Upsert, which would hand out an already-loaded block as pending.
	var snap []outbound.BlockRef
	for _, b := range m.universe {
		if !m.loaded[b] {
			snap = append(snap, b)
		}
	}
	sort.Slice(snap, func(i, j int) bool {
		if snap[i].Number != snap[j].Number {
			return snap[i].Number < snap[j].Number
		}
		return snap[i].Version < snap[j].Version
	})
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
	if m.loaded == nil {
		m.loaded = map[outbound.BlockRef]bool{}
	}
	// ON CONFLICT DO NOTHING: a row whose exact key is already present inserts nothing.
	var inserted int64
	for _, r := range rows {
		k := outbound.BlockRef{Number: r.BlockNumber, Version: r.BlockVersion}
		if m.loaded[k] {
			continue
		}
		m.loaded[k] = true
		inserted++
	}
	return inserted, nil
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

// A block the archive does not hold fails the run naming it. The reader must return the
// ErrObjectNotFound sentinel: a bare error is a transport failure and takes a different branch
// entirely, which is what this test used to assert while claiming to cover the miss path.
func TestRun_FailsHardOnMissingArchivedBlock(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 42, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		return nil, outbound.ErrObjectNotFound
	}}
	svc := newTestService(t, repo, reader, 500)

	total, err := svc.Run(context.Background())
	requireReportedAsMiss(t, err, "42/0")
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
	universe := func() []outbound.BlockRef {
		return []outbound.BlockRef{{Number: 1}, {Number: 2}, {Number: 3}, {Number: 4}, {Number: 5}}
	}
	for _, c := range []struct {
		name    string
		repo    *mockBlockMetaRepo
		wantErr bool
	}{
		{name: "a clean run", repo: &mockBlockMetaRepo{universe: universe()}},
		{name: "a run whose upsert fails",
			repo:    &mockBlockMetaRepo{universe: universe(), upsertErr: errors.New("boom")},
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

// A December backfill hardcoded key version 1 for deep history whatever the referencing table says,
// so a read pinned to the requested version 404s though the archive holds the block. Without the
// fallback that was reported as absent with no second attempt.
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
	if got.BlockVersion != 0 {
		t.Errorf("BlockVersion = %d, want 0 -- the row is filed under the version that REFERENCED it, "+
			"so the work-list anti-join clears and an observation table's join finds it", got.BlockVersion)
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

// requireReportedAsMiss asserts err is Run's miss-aggregation error naming ref, not a hard failure
// mentioning the same pair: readOne's non-miss paths embed it too, so checking ref alone cannot tell
// a miss from a bug that turned one into a hard failure.
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

// Every occupied version holding no block object is still a miss, not a hard failure: a partial
// upload is an ordinary archive state, and failing on it would stall the whole chain's load.
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

// versionedReader serves a header only at the versions a height actually has archived, so a test can
// state "this height exists at v3 and nowhere else" the way the real bucket does. Absent coordinates
// return ErrObjectNotFound, which is the only thing the fallback is allowed to act on.
func versionedReader(t *testing.T, archived map[int64]map[int]int64) *mockS3Reader {
	t.Helper()
	return &mockS3Reader{streamFn: func(_ context.Context, _ string, key string) (io.ReadCloser, error) {
		parsed, ok := s3key.Parse(key)
		if !ok {
			return nil, fmt.Errorf("unparseable key %q", key)
		}
		ts, ok := archived[parsed.BlockNumber][parsed.Version]
		if !ok {
			return nil, outbound.ErrObjectNotFound
		}
		return io.NopCloser(strings.NewReader(fmt.Sprintf(`{"timestamp":"0x%x"}`, ts))), nil
	}}
}

// readBatch writes each result into a per-index slot, so a mixed batch catches one landing in the
// wrong slot -- invisible in the single-block fallback tests. This does NOT force overlap; it passes
// sequentially, and TestRun_ReadsWithinABatchOverlap is what pins the concurrency.
func TestRun_MixedBatchKeepsResultsWithTheirOwnBlocks(t *testing.T) {
	archived := map[int64]map[int]int64{
		10: {0: tsBase + 10}, // clean read at the requested version
		20: {3: tsBase + 20}, // requested v0 absent; archive resolves v3
		40: {0: tsBase + 40}, // clean read
		50: {2: tsBase + 50}, // requested v0 absent; archive resolves v2
		60: {1: tsBase + 60}, // archive claims v0 (agrees with the 404) -> miss despite v1 existing
	}
	highest := map[int64]int{20: 3, 50: 2, 60: 0} // 30 absent entirely -> found=false
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{
		{Number: 10, Version: 0}, {Number: 20, Version: 0}, {Number: 30, Version: 0},
		{Number: 40, Version: 0}, {Number: 50, Version: 0}, {Number: 60, Version: 0},
	}}
	archive := &fakeArchive{highestVersionFn: func(_ context.Context, bn int64) (int, bool, error) {
		v, ok := highest[bn]
		return v, ok, nil
	}}
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: 6, Concurrency: 6},
		repo, versionedReader(t, archived), archive, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	total, runErr := svc.Run(context.Background())
	requireReportedAsMiss(t, runErr, "30/0")
	if !strings.Contains(runErr.Error(), "60/0") {
		t.Errorf("error %q does not also name the archive-agrees miss 60/0", runErr)
	}
	if total != 4 {
		t.Fatalf("loaded %d rows, want 4", total)
	}
	// Each row must carry ITS OWN block's version and timestamp: a slot mix-up shows up here and
	// nowhere else, because every value is a distinct function of the block number.
	want := map[int64]struct {
		version int
		ts      int64
	}{10: {0, tsBase + 10}, 20: {0, tsBase + 20}, 40: {0, tsBase + 40}, 50: {0, tsBase + 50}}
	for _, got := range repo.upserted {
		w, ok := want[got.BlockNumber]
		if !ok {
			t.Errorf("upserted unexpected block %d", got.BlockNumber)
			continue
		}
		if got.BlockVersion != w.version {
			t.Errorf("block %d: version = %d, want %d", got.BlockNumber, got.BlockVersion, w.version)
		}
		if got.BlockTimestamp.Unix() != w.ts {
			t.Errorf("block %d: timestamp = %d, want %d (a result landed on the wrong block)",
				got.BlockNumber, got.BlockTimestamp.Unix(), w.ts)
		}
		delete(want, got.BlockNumber)
	}
	if len(want) != 0 {
		t.Errorf("never upserted: %v", want)
	}
}

// runCancelledDuringFallback drives one block into the archive fallback and cancels the run's context
// at cancelAt: "resolve" cancels inside HighestVersion, "retry" lets resolution succeed and cancels
// before the read at the resolved version.
func runCancelledDuringFallback(t *testing.T, cancelAt string) (int64, error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 700, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(rctx context.Context, _ string, _ string) (io.ReadCloser, error) {
		if rctx.Err() != nil {
			return nil, rctx.Err()
		}
		return nil, outbound.ErrObjectNotFound
	}}
	archive := &fakeArchive{highestVersionFn: func(actx context.Context, _ int64) (int, bool, error) {
		cancel()
		if cancelAt == "resolve" {
			return 0, false, actx.Err()
		}
		return 1, true, nil // resolves fine; the cancelled retry read is what must surface
	}}
	return newTestServiceWithArchive(t, repo, reader, archive, 500).Run(ctx)
}

// Cancellation during the fallback must abort, not be mistaken for an absent block. The existing
// cancellation test cancels before the run starts, so neither point inside readOne's fallback was
// covered: a context error swallowed here would record a real block as permanently missing.
func TestRun_CancellationDuringTheFallbackAbortsRatherThanRecordingAMiss(t *testing.T) {
	// errors.Is(err, context.Canceled) alone cannot tell the fallback surfacing the cancellation from
	// Run's loop-top check reporting it after the block was quietly missed: both wrap context.Canceled
	// and neither mentions the miss list. Only the stage name proves the fallback refused.
	for _, c := range []struct{ name, when, wantStage string }{
		{"while resolving the version", "resolve", "resolving the archived version"},
		{"between resolving and the retry read", "retry", "at archived version"},
	} {
		t.Run(c.name, func(t *testing.T) {
			total, err := runCancelledDuringFallback(t, c.when)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want it to wrap context.Canceled", err)
			}
			if strings.Contains(err.Error(), "absent from the archive") {
				t.Errorf("cancellation was recorded as an absent block: %v", err)
			}
			if !strings.Contains(err.Error(), c.wantStage) {
				t.Errorf("error %q does not name %q -- the fallback swallowed the cancellation and Run "+
					"reported its own loop-top context check instead", err, c.wantStage)
			}
			if total != 0 {
				t.Errorf("total = %d, want 0", total)
			}
		})
	}
}

// A corrupt payload at the resolved version must fail the run, not count as a miss. blockheader
// enforces block_meta_ts_sane_chk's [2009, 2100) window, but only its own package tested that -- a
// refusal folded into the miss list would look like an ordinary archive hole forever.
func TestRun_CorruptHeaderAtTheResolvedVersionFailsRatherThanCountingAsAMiss(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 800, Version: 0}}}
	reader := &mockS3Reader{streamFn: func(_ context.Context, _ string, key string) (io.ReadCloser, error) {
		parsed, ok := s3key.Parse(key)
		if !ok {
			return nil, fmt.Errorf("unparseable key %q", key)
		}
		if parsed.Version != 1 {
			return nil, outbound.ErrObjectNotFound
		}
		return io.NopCloser(strings.NewReader(`{"timestamp":"0x1"}`)), nil // 1970: below the floor
	}}
	archive := &fakeArchive{highestVersionFn: func(context.Context, int64) (int, bool, error) {
		return 1, true, nil
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	total, err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("a corrupt header at the resolved version was accepted")
	}
	if strings.Contains(err.Error(), "absent from the archive") {
		t.Fatalf("corrupt payload was recorded as an absent block: %v", err)
	}
	if !strings.Contains(err.Error(), "outside") {
		t.Errorf("error %q does not name the sanity-window refusal", err)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0", total)
	}
}

// Reorg semantics from staging: 521 chain-1 heights hold both v0 and v1 and every disagreement is
// exactly 12 seconds, one slot. Resolving must take the LATER time; the earlier one is the orphan.
func TestRun_ResolvingAReorgedHeightTakesTheReplacementBlocksTimestamp(t *testing.T) {
	const height = 24484220
	const orphanTs = int64(1771423703) // v0
	const canonicalTs = orphanTs + 12  // v1, one slot later
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: height, Version: 0}}}
	// The December backfill left this height's v0 key absent while v1 holds the canonical block.
	reader := versionedReader(t, map[int64]map[int]int64{height: {1: canonicalTs}})
	archive := &fakeArchive{highestVersionFn: func(context.Context, int64) (int, bool, error) {
		return 1, true, nil
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	if _, err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(repo.upserted) != 1 {
		t.Fatalf("upserted %d rows, want 1", len(repo.upserted))
	}
	got := repo.upserted[0]
	if got.BlockVersion != 0 {
		t.Errorf("BlockVersion = %d, want 0 (the referencing version)", got.BlockVersion)
	}
	if got.BlockTimestamp.Unix() != canonicalTs {
		t.Errorf("BlockTimestamp = %d, want %d (wrote the orphaned block's time)", got.BlockTimestamp.Unix(), canonicalTs)
	}
}

// A resolved read has to CLEAR its work-list row, not just stop failing. The exact staging state:
// 469 stuck rows request version 0 at heights block_meta already holds at version 1, where filing
// under the resolved version inserts nothing and re-reads them from S3 every run, reporting success.
func TestRun_AResolvedBlockClearsItsWorkListRowAndDoesNotRecur(t *testing.T) {
	const height = 23419201
	repo := &mockBlockMetaRepo{
		universe: []outbound.BlockRef{{Number: height, Version: 0}},
		// block_meta already holds this height at the version the archive actually has.
		loaded: map[outbound.BlockRef]bool{{Number: height, Version: 1}: true},
	}
	reader := versionedReader(t, map[int64]map[int]int64{height: {1: tsBase + height}})
	archive := &fakeArchive{highestVersionFn: func(context.Context, int64) (int, bool, error) {
		return 1, true, nil
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	inserted, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if inserted != 1 {
		t.Fatalf("first run inserted %d rows, want 1 -- the row was filed under a key block_meta "+
			"already had, so nothing was written and the work list cannot clear", inserted)
	}
	if !repo.loaded[outbound.BlockRef{Number: height, Version: 0}] {
		t.Error("no block_meta row exists at the REQUESTED version, so the anti-join still finds " +
			"nothing and an observation table joining on its own block_version still misses this height")
	}

	// The run has to converge: a second pass must find nothing left to do.
	again, err := svc.Run(context.Background())
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	if again != 0 {
		t.Errorf("second run inserted %d rows, want 0", again)
	}
	// Asserted through a fresh work list rather than the mock's raw universe: the anti-join is what
	// has to clear, and it is evaluated at open time against what block_meta now holds.
	list, err := repo.OpenWorkList(context.Background(), 1, 0)
	if err != nil {
		t.Fatalf("reopening the work list: %v", err)
	}
	defer list.Close(context.Background())
	refs, err := list.Next(context.Background(), 500)
	if err != nil {
		t.Fatalf("paging the reopened work list: %v", err)
	}
	if len(refs) != 0 {
		t.Errorf("%d block(s) still pending after a successful run -- they will be re-read from S3 "+
			"on every run indefinitely", len(refs))
	}
}

// The top occupied version can hold receipts/traces with no block object while a LOWER version holds
// the block, because a slot counts as occupied whatever fills it. Stopping at the first 404 reports
// a block the archive holds as absent, fails the run, and never converges.
func TestRun_FallsBackPastAVersionHoldingNoBlockObject(t *testing.T) {
	const height = 900
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: height, Version: 0}}}
	// v2 is occupied by receipts only; v1 holds the block; v0 (requested) holds nothing.
	reader := versionedReader(t, map[int64]map[int]int64{height: {1: tsBase + height}})
	archive := &fakeArchive{highestVersionFn: func(context.Context, int64) (int, bool, error) {
		return 2, true, nil
	}}
	svc := newTestServiceWithArchive(t, repo, reader, archive, 500)

	if _, err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v (v1 holds the block and was never tried)", err)
	}
	if len(repo.upserted) != 1 {
		t.Fatalf("upserted %d rows, want 1", len(repo.upserted))
	}
	if got := repo.upserted[0]; got.BlockVersion != 0 || got.BlockTimestamp.Unix() != tsBase+height {
		t.Errorf("row = v%d/%d, want v0 carrying v1's header time %d",
			got.BlockVersion, got.BlockTimestamp.Unix(), tsBase+height)
	}
}

// HeadMargin holds back the newest blocks while the archive catches up. Dropped, every scheduled run
// reports normal archive lag as missing objects and fails.
func TestRun_PassesTheHeadMarginToTheWorkList(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{{Number: 10, Version: 0}}}
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: 500, HeadMargin: 300},
		repo, &mockS3Reader{streamFn: streamTimestampByBlock}, &fakeArchive{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if repo.headMargin != 300 {
		t.Errorf("work list opened with head margin %d, want 300", repo.headMargin)
	}
}

// OnProgress is the Temporal activity's heartbeat. Silently dropped, a long run looks dead.
func TestRun_ReportsProgressAfterEveryBatch(t *testing.T) {
	repo := &mockBlockMetaRepo{universe: []outbound.BlockRef{
		{Number: 10, Version: 0}, {Number: 20, Version: 0}, {Number: 30, Version: 0},
	}}
	var seen []int64
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: 2,
		OnProgress: func(total int64) { seen = append(seen, total) }},
		repo, &mockS3Reader{streamFn: streamTimestampByBlock}, &fakeArchive{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := svc.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Three blocks over two batches, reporting the running total each time.
	want := []int64{2, 3}
	if len(seen) != len(want) {
		t.Fatalf("progress reported %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Errorf("progress[%d] = %d, want %d", i, seen[i], want[i])
		}
	}
}

// Every header in a batch is held in memory before one INSERT, so an operator-set BATCH_SIZE in the
// tens of thousands is clamped rather than honoured.
func TestNew_ClampsAnOversizedBatchSize(t *testing.T) {
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: 10 * maxBatchSize},
		&mockBlockMetaRepo{}, &mockS3Reader{streamFn: streamTimestampByBlock}, &fakeArchive{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.cfg.BatchSize != maxBatchSize {
		t.Errorf("BatchSize = %d, want it clamped to %d", svc.cfg.BatchSize, maxBatchSize)
	}
}

// The concurrency default is the difference between a one-hour first pass and a seven-hour one.
func TestNew_DefaultsConcurrency(t *testing.T) {
	svc, err := New(Config{ChainID: 1, Bucket: "b", Concurrency: 0},
		&mockBlockMetaRepo{}, &mockS3Reader{streamFn: streamTimestampByBlock}, &fakeArchive{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if svc.cfg.Concurrency != defaultConcurrency {
		t.Errorf("Concurrency = %d, want %d", svc.cfg.Concurrency, defaultConcurrency)
	}
}

// The miss list is a lead for a bulk-download, not a manifest: the count carries the scale and the
// names are capped, so a sparse deep-tail run cannot emit a megabyte-long error.
func TestRun_CapsTheBlocksItNamesButNotTheCount(t *testing.T) {
	const misses = maxNamedMisses + 12
	universe := make([]outbound.BlockRef, 0, misses)
	for i := range misses {
		universe = append(universe, outbound.BlockRef{Number: int64(1000 + i), Version: 0})
	}
	repo := &mockBlockMetaRepo{universe: universe}
	reader := &mockS3Reader{streamFn: func(context.Context, string, string) (io.ReadCloser, error) {
		return nil, outbound.ErrObjectNotFound
	}}
	svc := newTestServiceWithArchive(t, repo, reader, &fakeArchive{}, 500)

	_, err := svc.Run(context.Background())
	if err == nil {
		t.Fatal("want a failure naming the absent blocks, got none")
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("%d referenced block(s)", misses)) {
		t.Errorf("error %q does not carry the full count %d", err, misses)
	}
	if !strings.Contains(err.Error(), "...") {
		t.Errorf("error %q names every miss instead of capping the list", err)
	}
	if named := strings.Count(err.Error(), "/0"); named > maxNamedMisses {
		t.Errorf("named %d blocks, want at most %d", named, maxNamedMisses)
	}
}

// A batch abandoned mid-flight must fail, not commit a short batch: the skipped refs are in neither
// the rows nor the miss list, so reporting success would call a partial pass complete.
func TestRun_CancellationMidBatchFailsRatherThanCommittingAShortBatch(t *testing.T) {
	const batch = 40
	universe := make([]outbound.BlockRef, 0, batch)
	for i := range batch {
		universe = append(universe, outbound.BlockRef{Number: int64(10 + i), Version: 0})
	}
	repo := &mockBlockMetaRepo{universe: universe}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	var reads int
	// The reader never fails, so the only way the run can abort is the skip inside readBatch. A
	// reader that returned ctx.Err() would raise the error itself and mask exactly that path.
	reader := &mockS3Reader{streamFn: func(rctx context.Context, bucket, key string) (io.ReadCloser, error) {
		mu.Lock()
		reads++
		hit := reads
		mu.Unlock()
		if hit == 3 {
			cancel()
		}
		return streamTimestampByBlock(rctx, bucket, key)
	}}
	svc, err := New(Config{ChainID: 1, Bucket: "b", BatchSize: batch, Concurrency: 2},
		repo, reader, &fakeArchive{}, testLogger())
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	total, runErr := svc.Run(ctx)
	if !errors.Is(runErr, context.Canceled) {
		t.Fatalf("error = %v, want it to wrap context.Canceled", runErr)
	}
	if total != 0 {
		t.Errorf("total = %d, want 0 -- a short batch was committed and reported as progress", total)
	}
}
