package archiving

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type stubInner struct {
	results []outbound.Result
	err     error
	addr    common.Address
}

func (s *stubInner) Execute(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	return s.results, s.err
}
func (s *stubInner) Address() common.Address { return s.addr }

type recordingArchiver struct {
	mu      sync.Mutex
	batches []outbound.CallBatchRecord
}

func (r *recordingArchiver) Archive(_ context.Context, rec outbound.CallBatchRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.batches = append(r.batches, rec)
	return nil
}

// errArchiver returns err from Archive; panicArchiver panics. Both exercise the
// fire-and-forget guarantee that archiving never affects the caller.
type errArchiver struct{ err error }

func (a errArchiver) Archive(context.Context, outbound.CallBatchRecord) error { return a.err }

type panicArchiver struct{}

func (panicArchiver) Archive(context.Context, outbound.CallBatchRecord) error {
	panic("archiver boom")
}

func newTestDecorator(inner outbound.Multicaller, arch outbound.CallArchiver, wg *sync.WaitGroup) *Multicaller {
	return NewMulticaller(inner, arch, Config{
		Source:  "oracle-price",
		ChainID: 1,
		BuildID: 47,
		Wait:    wg,
	})
}

// TestExecute covers the parametric forwarding/archiving behaviour against a
// recording archiver. The non-parametric guarantees (survives a failing or
// panicking archiver) are exercised by the focused tests below.
func TestExecute(t *testing.T) {
	errBoom := errors.New("boom")
	errRPCDown := errors.New("rpc down")

	tests := []struct {
		name         string
		innerResults []outbound.Result
		innerErr     error
		blockVersion int // stamped on ctx via WithBlockVersion
		calls        []outbound.Call
		blockNumber  *big.Int
		wantErr      error
		wantResults  int
		wantBatches  int // S3 PUTs: one per Execute, zero when nothing to archive
		wantCalls    int // total CallEntry rows across all archived batches
		// extra runs case-specific assertions after the goroutines are drained.
		extra func(t *testing.T, res []outbound.Result, calls []outbound.Call, rec *recordingArchiver)
	}{
		{
			name:         "forwards results and inner error without archiving",
			innerResults: []outbound.Result{{Success: true}},
			innerErr:     errBoom,
			calls:        []outbound.Call{{CallData: []byte{0x01}}},
			blockNumber:  big.NewInt(10),
			wantErr:      errBoom,
			wantResults:  1,
			wantBatches:  0,
			wantCalls:    0,
			extra: func(t *testing.T, res []outbound.Result, _ []outbound.Call, _ *recordingArchiver) {
				if !res[0].Success {
					t.Fatalf("results not forwarded: %+v", res)
				}
			},
		},
		{
			name: "archives the whole batch as one object with stamped metadata",
			innerResults: []outbound.Result{
				{Success: true, ReturnData: []byte{0xaa}},
				{Success: false, ReturnData: []byte{0xbb}},
			},
			blockVersion: 2,
			calls: []outbound.Call{
				{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
				{Target: common.HexToAddress("0x02"), CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
			},
			blockNumber: big.NewInt(21500042),
			wantResults: 2,
			wantBatches: 1,
			wantCalls:   2,
			extra: func(t *testing.T, _ []outbound.Result, _ []outbound.Call, rec *recordingArchiver) {
				b := rec.batches[0]
				if b.BlockNumber != 21500042 || b.BlockVersion != 2 || b.ChainID != 1 || b.BuildID != 47 {
					t.Fatalf("batch metadata wrong: %+v", b)
				}
				if b.Source != "oracle-price" {
					t.Fatalf("source = %q", b.Source)
				}
				if got := b.Calls[0].Selector; got != "0xfeaf968c" {
					t.Fatalf("Calls[0].Selector = %q", got)
				}
				if !b.Calls[0].Success || b.Calls[1].Success {
					t.Fatalf("per-call Success flags wrong: %+v", b.Calls)
				}
			},
		},
		{
			name:        "does not archive when inner errors",
			innerErr:    errRPCDown,
			calls:       []outbound.Call{{CallData: []byte{0x01}}},
			blockNumber: big.NewInt(1),
			wantErr:     errRPCDown,
			wantResults: 0,
			wantBatches: 0,
			wantCalls:   0,
		},
		{
			name:         "nil block number archives as block 0",
			innerResults: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}},
			calls:        []outbound.Call{{CallData: []byte{0x01}}},
			blockNumber:  nil,
			wantResults:  1,
			wantBatches:  1,
			wantCalls:    1,
			extra: func(t *testing.T, _ []outbound.Result, _ []outbound.Call, rec *recordingArchiver) {
				if rec.batches[0].BlockNumber != 0 {
					t.Fatalf("nil blockNumber should yield BlockNumber 0, got %d", rec.batches[0].BlockNumber)
				}
			},
		},
		{
			name:         "truncates batch to result count when fewer results than calls",
			innerResults: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}},
			calls: []outbound.Call{
				{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
				{Target: common.HexToAddress("0x02"), CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
			},
			blockNumber: big.NewInt(1),
			wantResults: 1,
			wantBatches: 1,
			wantCalls:   1, // only the prefix that matches result count is archived
		},
		{
			name:         "empty batch (no calls) writes no object",
			innerResults: []outbound.Result{},
			calls:        []outbound.Call{},
			blockNumber:  big.NewInt(1),
			wantResults:  0,
			wantBatches:  0,
			wantCalls:    0,
		},
		{
			name:         "defensively copies call data and response",
			innerResults: []outbound.Result{{Success: true, ReturnData: []byte{0xaa, 0xbb}}},
			calls:        []outbound.Call{{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}}},
			blockNumber:  big.NewInt(1),
			wantResults:  1,
			wantBatches:  1,
			wantCalls:    1,
			extra: func(t *testing.T, res []outbound.Result, calls []outbound.Call, rec *recordingArchiver) {
				// Mutate the caller's slices after Execute returned and the
				// goroutines drained; the archived record must be unaffected
				// because buildBatchRecord copies the bytes at capture.
				calls[0].CallData[0] = 0x00
				res[0].ReturnData[0] = 0x00
				entry := rec.batches[0].Calls[0]
				if len(entry.CallData) != 4 || entry.CallData[0] != 0xfe {
					t.Fatalf("archived CallData was not defensively copied: % x", entry.CallData)
				}
				if len(entry.Response) != 2 || entry.Response[0] != 0xaa {
					t.Fatalf("archived Response was not defensively copied: % x", entry.Response)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recordingArchiver{}
			var wg sync.WaitGroup
			d := newTestDecorator(&stubInner{results: tt.innerResults, err: tt.innerErr}, rec, &wg)

			ctx := WithBlockVersion(context.Background(), tt.blockVersion)
			res, err := d.Execute(ctx, tt.calls, tt.blockNumber)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if len(res) != tt.wantResults {
				t.Fatalf("results = %d, want %d", len(res), tt.wantResults)
			}

			d.Close() // drain background goroutines (must not hang or panic)

			if len(rec.batches) != tt.wantBatches {
				t.Fatalf("archived %d batches, want %d", len(rec.batches), tt.wantBatches)
			}
			var totalCalls int
			for _, b := range rec.batches {
				totalCalls += len(b.Calls)
			}
			if totalCalls != tt.wantCalls {
				t.Fatalf("archived %d total calls across batches, want %d", totalCalls, tt.wantCalls)
			}
			if tt.extra != nil {
				tt.extra(t, res, tt.calls, rec)
			}
		})
	}
}

// TestExecuteSucceedsWhenArchiveErrors asserts the fire-and-forget guarantee:
// a failing archiver never affects the results or error the caller sees, and
// Close still drains cleanly.
func TestExecuteSucceedsWhenArchiveErrors(t *testing.T) {
	inner := &stubInner{results: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}}}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, errArchiver{err: errors.New("s3 down")}, &wg)

	res, err := d.Execute(context.Background(), []outbound.Call{{CallData: []byte{0x01}}}, big.NewInt(1))
	if err != nil {
		t.Fatalf("Execute returned err despite fire-and-forget archiving: %v", err)
	}
	if len(res) != 1 || !res[0].Success {
		t.Fatalf("results not forwarded: %+v", res)
	}
	d.Close() // must drain cleanly even though every Archive failed
}

// TestExecuteSurvivesArchivePanic asserts a panic in the archive goroutine is
// recovered rather than propagated (which would crash the process).
func TestExecuteSurvivesArchivePanic(t *testing.T) {
	inner := &stubInner{results: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}}}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, panicArchiver{}, &wg)

	res, err := d.Execute(context.Background(), []outbound.Call{{CallData: []byte{0x01}}}, big.NewInt(1))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(res) != 1 {
		t.Fatalf("results not forwarded: %+v", res)
	}
	d.Close() // must return: the archive-goroutine panic must be recovered
}

// TestExecuteRecordsArchiveWriteStatus verifies that archive.writes.total is
// incremented once per batch archive attempt with the correct status, chain,
// and source labels. (One Execute = one PUT = one increment, regardless of
// how many calls were in the batch.)
func TestExecuteRecordsArchiveWriteStatus(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	tests := []struct {
		name       string
		archiveErr error
		wantStatus string
	}{
		{"success", nil, "success"},
		{"error", errors.New("s3 down"), "error"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			inner := &stubInner{results: []outbound.Result{{Success: true}}}
			arch := errArchiver{err: tc.archiveErr}
			var wg sync.WaitGroup
			m := NewMulticaller(inner, arch, Config{
				Source:        "test-source",
				ChainID:       1,
				Chain:         "mainnet",
				Wait:          &wg,
				MeterProvider: mp,
				Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
			})

			_, err := m.Execute(context.Background(), []outbound.Call{{Target: common.Address{}, CallData: []byte{0x01, 0x02, 0x03, 0x04}}}, big.NewInt(100))
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			m.Close() // drain the fire-and-forget archive write

			got := counterValueForStatus(t, reader, "archive.writes.total", tc.wantStatus)
			if got != 1 {
				t.Errorf("archive.writes.total{status=%q} = %d, want 1", tc.wantStatus, got)
			}
		})
	}
}

// TestExecuteEmptyBatchSkipsMetric pins the contract that empty batches write
// no S3 object and therefore do not increment archive.writes.total.
func TestExecuteEmptyBatchSkipsMetric(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	rec := &recordingArchiver{}
	var wg sync.WaitGroup
	m := NewMulticaller(&stubInner{results: []outbound.Result{}}, rec, Config{
		Source:        "test-source",
		ChainID:       1,
		Chain:         "mainnet",
		Wait:          &wg,
		MeterProvider: mp,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	if _, err := m.Execute(context.Background(), nil, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	m.Close()

	if n := len(rec.batches); n != 0 {
		t.Fatalf("recorded %d batches for empty input, want 0", n)
	}
	if got := counterValueForStatus(t, reader, "archive.writes.total", "success") +
		counterValueForStatus(t, reader, "archive.writes.total", "error"); got != 0 {
		t.Errorf("archive.writes.total incremented %d times on empty batch, want 0", got)
	}
}

// TestExecuteCountsOneBatchOnTruncation pins the contract that truncation
// (len(results) < len(calls)) still produces exactly one archive write, not
// one per call — the metric counts batches, not calls.
func TestExecuteCountsOneBatchOnTruncation(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })

	inner := &stubInner{results: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}}}
	rec := &recordingArchiver{}
	var wg sync.WaitGroup
	m := NewMulticaller(inner, rec, Config{
		Source:        "test-source",
		ChainID:       1,
		Chain:         "mainnet",
		Wait:          &wg,
		MeterProvider: mp,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	calls := []outbound.Call{
		{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
		{Target: common.HexToAddress("0x02"), CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
		{Target: common.HexToAddress("0x03"), CallData: []byte{0xab, 0xcd, 0xef, 0x01}},
	}
	if _, err := m.Execute(context.Background(), calls, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	m.Close()

	if n := len(rec.batches); n != 1 {
		t.Fatalf("recorded %d batches on truncation, want 1", n)
	}
	if n := len(rec.batches[0].Calls); n != 1 {
		t.Fatalf("archived %d calls in truncated batch, want 1 (only the prefix with both call and result)", n)
	}
	if got := counterValueForStatus(t, reader, "archive.writes.total", "success"); got != 1 {
		t.Errorf("archive.writes.total{status=success} = %d on truncation, want 1", got)
	}
}

// TestExecuteCompletesArchiveAfterCallerCancels pins the context.WithoutCancel
// guarantee: cancelling the caller's context immediately after Execute returns
// must not prevent the background archive write from completing.
func TestExecuteCompletesArchiveAfterCallerCancels(t *testing.T) {
	// blockingArchiver releases on Archive entry, then blocks until released.
	// This lets us cancel the caller's context after Execute has scheduled the
	// goroutine but before the archive itself can observe the cancellation.
	entered := make(chan struct{})
	release := make(chan struct{})
	var seenErr error
	arch := blockingArchiver{
		onArchive: func(ctx context.Context) error {
			close(entered)
			<-release
			// Verify the archive's own context was NOT cancelled by the caller.
			if err := ctx.Err(); err != nil {
				seenErr = err
			}
			return nil
		},
	}

	inner := &stubInner{results: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}}}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, arch, &wg)

	ctx, cancel := context.WithCancel(context.Background())
	if _, err := d.Execute(ctx, []outbound.Call{{CallData: []byte{0x01}}}, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	<-entered // archive goroutine is now inside Archive
	cancel()  // cancel the caller's context — must NOT cancel the detached archive ctx
	close(release)

	d.Close()
	if seenErr != nil {
		t.Fatalf("archive ctx was cancelled by caller cancel: %v", seenErr)
	}
}

type blockingArchiver struct {
	onArchive func(ctx context.Context) error
}

func (b blockingArchiver) Archive(ctx context.Context, _ outbound.CallBatchRecord) error {
	return b.onArchive(ctx)
}

// counterValueForStatus returns the int64 counter value whose data point carries
// status=want, asserting chain and source labels are present.
func counterValueForStatus(t *testing.T, reader sdkmetric.Reader, name, want string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("metric %q is %T, want metricdata.Sum[int64]", name, m.Data)
			}
			for _, dp := range sum.DataPoints {
				status, _ := dp.Attributes.Value("status")
				if status.AsString() != want {
					continue
				}
				if c, _ := dp.Attributes.Value("chain"); c.AsString() != "mainnet" {
					t.Errorf("chain label = %q, want mainnet", c.AsString())
				}
				if s, _ := dp.Attributes.Value("source"); s.AsString() != "test-source" {
					t.Errorf("source label = %q, want test-source", s.AsString())
				}
				return dp.Value
			}
		}
	}
	return 0
}
