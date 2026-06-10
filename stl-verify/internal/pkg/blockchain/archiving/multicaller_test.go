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
	records []outbound.CallRecord
}

func (r *recordingArchiver) Archive(_ context.Context, rec outbound.CallRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}

// errArchiver returns err from Archive; panicArchiver panics. Both exercise the
// fire-and-forget guarantee that archiving never affects the caller.
type errArchiver struct{ err error }

func (a errArchiver) Archive(context.Context, outbound.CallRecord) error { return a.err }

type panicArchiver struct{}

func (panicArchiver) Archive(context.Context, outbound.CallRecord) error { panic("archiver boom") }

func newTestDecorator(inner outbound.Multicaller, arch outbound.CallArchiver, wg *sync.WaitGroup) *Multicaller {
	return NewMulticaller(inner, arch, Config{
		Source:  "oracle-price",
		ChainID: 1,
		BuildID: 47,
		Wait:    wg,
	})
}

// TestExecuteBoundedBySemaphore verifies that a shared semaphore caps in-flight
// archive writes: every call is still archived, and all tokens are released
// once the writes drain (no leak that would wedge later calls).
func TestExecuteBoundedBySemaphore(t *testing.T) {
	const numCalls = 5
	results := make([]outbound.Result, numCalls)
	calls := make([]outbound.Call, numCalls)
	for i := range calls {
		results[i] = outbound.Result{Success: true, ReturnData: []byte{byte(i)}}
		calls[i] = outbound.Call{Target: common.HexToAddress("0x01"), CallData: []byte{byte(i)}}
	}

	arch := &recordingArchiver{}
	sem := make(chan struct{}, 2) // cap below numCalls so the bound is exercised
	var wg sync.WaitGroup
	d := NewMulticaller(&stubInner{results: results}, arch, Config{
		Source: "oracle-price", ChainID: 1, BuildID: 47, Wait: &wg, Sem: sem,
	})

	if _, err := d.Execute(context.Background(), calls, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	d.Close()

	if len(arch.records) != numCalls {
		t.Fatalf("archived %d records, want %d", len(arch.records), numCalls)
	}
	if len(sem) != 0 {
		t.Fatalf("semaphore not fully released: %d tokens held", len(sem))
	}
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
		wantArchived int
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
			wantArchived: 0,
			extra: func(t *testing.T, res []outbound.Result, _ []outbound.Call, _ *recordingArchiver) {
				if !res[0].Success {
					t.Fatalf("results not forwarded: %+v", res)
				}
			},
		},
		{
			name: "archives each call with stamped metadata",
			innerResults: []outbound.Result{
				{Success: true, ReturnData: []byte{0xaa}},
				{Success: false, ReturnData: []byte{0xbb}},
			},
			blockVersion: 2,
			calls: []outbound.Call{
				{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
				{Target: common.HexToAddress("0x02"), CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
			},
			blockNumber:  big.NewInt(21500042),
			wantResults:  2,
			wantArchived: 2,
			extra: func(t *testing.T, _ []outbound.Result, _ []outbound.Call, rec *recordingArchiver) {
				for _, r := range rec.records {
					if r.BlockNumber != 21500042 || r.BlockVersion != 2 || r.ChainID != 1 || r.BuildID != 47 {
						t.Fatalf("record metadata wrong: %+v", r)
					}
					if r.Source != "oracle-price" {
						t.Fatalf("source = %q", r.Source)
					}
				}
			},
		},
		{
			name:         "does not archive when inner errors",
			innerErr:     errRPCDown,
			calls:        []outbound.Call{{CallData: []byte{0x01}}},
			blockNumber:  big.NewInt(1),
			wantErr:      errRPCDown,
			wantResults:  0,
			wantArchived: 0,
		},
		{
			name:         "nil block number archives as block 0",
			innerResults: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}},
			calls:        []outbound.Call{{CallData: []byte{0x01}}},
			blockNumber:  nil,
			wantResults:  1,
			wantArchived: 1,
			extra: func(t *testing.T, _ []outbound.Result, _ []outbound.Call, rec *recordingArchiver) {
				if rec.records[0].BlockNumber != 0 {
					t.Fatalf("nil blockNumber should yield BlockNumber 0, got %d", rec.records[0].BlockNumber)
				}
			},
		},
		{
			name:         "truncates to result count when fewer results than calls",
			innerResults: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}},
			calls: []outbound.Call{
				{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
				{Target: common.HexToAddress("0x02"), CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
			},
			blockNumber:  big.NewInt(1),
			wantResults:  1,
			wantArchived: 1,
		},
		{
			name:         "defensively copies call data",
			innerResults: []outbound.Result{{Success: true, ReturnData: []byte{0xaa, 0xbb}}},
			calls:        []outbound.Call{{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}}},
			blockNumber:  big.NewInt(1),
			wantResults:  1,
			wantArchived: 1,
			extra: func(t *testing.T, _ []outbound.Result, calls []outbound.Call, rec *recordingArchiver) {
				// Mutate the caller's slice after Execute returned and the
				// goroutines drained; the archived record must be unaffected
				// because buildRecord copies the bytes at capture.
				calls[0].CallData[0] = 0x00
				got := rec.records[0].CallData
				if len(got) != 4 || got[0] != 0xfe {
					t.Fatalf("archived CallData was not defensively copied: % x", got)
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

			if len(rec.records) != tt.wantArchived {
				t.Fatalf("archived %d records, want %d", len(rec.records), tt.wantArchived)
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
// incremented once per archive attempt with the correct status, chain, and
// source labels.
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
