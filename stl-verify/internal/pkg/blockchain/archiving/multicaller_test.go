package archiving

import (
	"context"
	"errors"
	"log/slog"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// fakeMulticaller implements outbound.Multicaller for tests.
type fakeMulticaller struct {
	addr    common.Address
	results []outbound.Result
	err     error

	mu          sync.Mutex
	gotCalls    []outbound.Call
	gotBlock    *big.Int
	invocations int
}

func (f *fakeMulticaller) Address() common.Address { return f.addr }
func (f *fakeMulticaller) Execute(_ context.Context, calls []outbound.Call, block *big.Int) ([]outbound.Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invocations++
	f.gotCalls = append([]outbound.Call(nil), calls...)
	f.gotBlock = block
	if f.err != nil {
		return nil, f.err
	}
	return f.results, nil
}

// captureArchiver records every batch sent to ArchiveBatch.
type captureArchiver struct {
	mu      sync.Mutex
	batches []outbound.CallBatch
	err     error
}

func (c *captureArchiver) ArchiveBatch(_ context.Context, b outbound.CallBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batches = append(c.batches, b)
	return c.err
}

func newSilentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestMulticallerForwardsAndArchives(t *testing.T) {
	addr := common.HexToAddress("0xca11bde05779b6216e94d2f0a4d57f1ddee6e6")
	target := common.HexToAddress("0xfeedfacefeedfacefeedfacefeedfacefeedface")
	calls := []outbound.Call{{Target: target, CallData: []byte{0x12, 0x34}}}
	results := []outbound.Result{{Success: true, ReturnData: []byte{0xab, 0xcd}}}

	inner := &fakeMulticaller{addr: addr, results: results}
	arc := &captureArchiver{}
	now := time.Date(2026, 6, 4, 10, 30, 45, 0, time.UTC)

	m := New(inner, arc, 1, 42, "oracle-price",
		WithClock(func() time.Time { return now }),
		WithLogger(newSilentLogger()),
	)

	ctx := WithBlockVersion(context.Background(), 2)
	got, err := m.Execute(ctx, calls, big.NewInt(21500042))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(got) != 1 || !got[0].Success {
		t.Fatalf("results not forwarded: %+v", got)
	}
	if inner.invocations != 1 {
		t.Fatalf("inner invocations = %d, want 1", inner.invocations)
	}
	if len(arc.batches) != 1 {
		t.Fatalf("batches archived = %d, want 1", len(arc.batches))
	}
	b := arc.batches[0]
	if b.ChainID != 1 || b.BlockNumber != 21500042 || b.BlockVersion != 2 || b.BuildID != 42 {
		t.Fatalf("batch meta mismatch: %+v", b)
	}
	if b.Source != "oracle-price" || b.Multicaller != addr {
		t.Fatalf("batch identifiers mismatch: %+v", b)
	}
	if !b.Timestamp.Equal(now) {
		t.Fatalf("timestamp = %v, want %v", b.Timestamp, now)
	}
	if len(b.Records) != 1 {
		t.Fatalf("records = %d, want 1", len(b.Records))
	}
	r := b.Records[0]
	if r.ContractAddress != target || !r.Success {
		t.Fatalf("record mismatch: %+v", r)
	}
	if string(r.CallData) != string(calls[0].CallData) || string(r.Response) != string(results[0].ReturnData) {
		t.Fatalf("record bytes mismatch: %+v", r)
	}
}

func TestMulticallerSkipsArchiveOnExecuteError(t *testing.T) {
	inner := &fakeMulticaller{err: errors.New("rpc dead")}
	arc := &captureArchiver{}
	m := New(inner, arc, 1, 1, "src", WithLogger(newSilentLogger()))

	_, err := m.Execute(context.Background(), []outbound.Call{{}}, big.NewInt(1))
	if err == nil {
		t.Fatal("expected error from Execute")
	}
	if len(arc.batches) != 0 {
		t.Fatalf("archive called on Execute failure: %d batches", len(arc.batches))
	}
}

func TestMulticallerFailOpenOnArchiveError(t *testing.T) {
	inner := &fakeMulticaller{results: []outbound.Result{{Success: true}}}
	arc := &captureArchiver{err: errors.New("s3 down")}
	m := New(inner, arc, 1, 1, "src", WithLogger(newSilentLogger()))

	got, err := m.Execute(context.Background(), []outbound.Call{{}}, big.NewInt(1))
	if err != nil {
		t.Fatalf("Execute must not propagate archive error: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("results not forwarded after archive error: %+v", got)
	}
}

func TestMulticallerDefaultsBlockVersionToZero(t *testing.T) {
	inner := &fakeMulticaller{results: []outbound.Result{{Success: true}}}
	arc := &captureArchiver{}
	m := New(inner, arc, 1, 1, "src", WithLogger(newSilentLogger()))

	if _, err := m.Execute(context.Background(), []outbound.Call{{}}, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if arc.batches[0].BlockVersion != 0 {
		t.Fatalf("default block_version = %d, want 0", arc.batches[0].BlockVersion)
	}
}

func TestMulticallerDecoderRecoversFromPanic(t *testing.T) {
	inner := &fakeMulticaller{results: []outbound.Result{{Success: true, ReturnData: []byte{1}}}}
	arc := &captureArchiver{}
	panicking := func(outbound.Call, outbound.Result) (any, string, bool) {
		panic("boom")
	}
	m := New(inner, arc, 1, 1, "src",
		WithDecoder(panicking),
		WithLogger(newSilentLogger()),
	)

	if _, err := m.Execute(context.Background(), []outbound.Call{{}}, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(arc.batches) != 1 || len(arc.batches[0].Records) != 1 {
		t.Fatalf("expected single archived batch with one record")
	}
	rec := arc.batches[0].Records[0]
	if rec.Method != "" || rec.Decoded != nil {
		t.Fatalf("panicking decoder should yield empty method/decoded; got %+v", rec)
	}
}

func TestMulticallerDecoderEnrichesRecord(t *testing.T) {
	inner := &fakeMulticaller{results: []outbound.Result{{Success: true, ReturnData: []byte{1}}}}
	arc := &captureArchiver{}
	dec := func(_ outbound.Call, _ outbound.Result) (any, string, bool) {
		return map[string]int{"price": 42}, "latestRoundData", true
	}
	m := New(inner, arc, 1, 1, "oracle-price",
		WithDecoder(dec),
		WithLogger(newSilentLogger()),
	)

	if _, err := m.Execute(context.Background(), []outbound.Call{{}}, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	rec := arc.batches[0].Records[0]
	if rec.Method != "latestRoundData" || rec.Decoded == nil {
		t.Fatalf("decoder not applied: %+v", rec)
	}
}

func TestMulticallerSkipsEmptyBatch(t *testing.T) {
	inner := &fakeMulticaller{results: []outbound.Result{}}
	arc := &captureArchiver{}
	m := New(inner, arc, 1, 1, "src", WithLogger(newSilentLogger()))

	if _, err := m.Execute(context.Background(), nil, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(arc.batches) != 0 {
		t.Fatalf("empty call slice should not archive: %d batches", len(arc.batches))
	}
}

func TestBlockVersionContextRoundTrip(t *testing.T) {
	ctx := WithBlockVersion(context.Background(), 7)
	v, ok := BlockVersionFromContext(ctx)
	if !ok || v != 7 {
		t.Fatalf("BlockVersionFromContext = (%d, %v), want (7, true)", v, ok)
	}

	v, ok = BlockVersionFromContext(context.Background())
	if ok || v != 0 {
		t.Fatalf("missing value = (%d, %v), want (0, false)", v, ok)
	}
}

func TestMulticallerAddressForwarded(t *testing.T) {
	addr := common.HexToAddress("0xca11bde05779b6216e94d2f0a4d57f1ddee6e6")
	inner := &fakeMulticaller{addr: addr}
	m := New(inner, &captureArchiver{}, 1, 1, "src", WithLogger(newSilentLogger()))
	if m.Address() != addr {
		t.Fatalf("Address() = %s, want %s", m.Address(), addr)
	}
}
