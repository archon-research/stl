package archiving

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
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

func newTestDecorator(inner outbound.Multicaller, arch outbound.CallArchiver, wg *sync.WaitGroup) *Multicaller {
	return NewMulticaller(inner, arch, Config{
		Source:  "oracle-price",
		ChainID: 1,
		BuildID: 47,
		Wait:    wg,
	})
}

func TestExecuteForwardsResultsAndError(t *testing.T) {
	wantErr := errors.New("boom")
	inner := &stubInner{results: []outbound.Result{{Success: true}}, err: wantErr}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, &recordingArchiver{}, &wg)

	res, err := d.Execute(context.Background(), []outbound.Call{{CallData: []byte{0x01}}}, big.NewInt(10))
	if !errors.Is(err, wantErr) {
		t.Fatalf("err = %v, want %v", err, wantErr)
	}
	if len(res) != 1 || !res[0].Success {
		t.Fatalf("results not forwarded: %+v", res)
	}
}

func TestExecuteArchivesEachCall(t *testing.T) {
	inner := &stubInner{results: []outbound.Result{
		{Success: true, ReturnData: []byte{0xaa}},
		{Success: false, ReturnData: []byte{0xbb}},
	}}
	arch := &recordingArchiver{}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, arch, &wg)

	ctx := WithBlockVersion(context.Background(), 2)
	calls := []outbound.Call{
		{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
		{Target: common.HexToAddress("0x02"), CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
	}
	if _, err := d.Execute(ctx, calls, big.NewInt(21500042)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	d.Close() // drain background goroutines

	if len(arch.records) != 2 {
		t.Fatalf("archived %d records, want 2", len(arch.records))
	}
	for _, r := range arch.records {
		if r.BlockNumber != 21500042 || r.BlockVersion != 2 || r.ChainID != 1 || r.BuildID != 47 {
			t.Fatalf("record metadata wrong: %+v", r)
		}
		if r.Source != "oracle-price" {
			t.Fatalf("source = %q", r.Source)
		}
	}
}

func TestExecuteDoesNotArchiveOnInnerError(t *testing.T) {
	inner := &stubInner{err: errors.New("rpc down")}
	arch := &recordingArchiver{}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, arch, &wg)

	_, _ = d.Execute(context.Background(), []outbound.Call{{CallData: []byte{0x01}}}, big.NewInt(1))
	d.Close()
	if len(arch.records) != 0 {
		t.Fatalf("archived %d records on error, want 0", len(arch.records))
	}
}

func TestExecuteHandlesNilBlockNumber(t *testing.T) {
	inner := &stubInner{results: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}}}
	arch := &recordingArchiver{}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, arch, &wg)

	if _, err := d.Execute(context.Background(), []outbound.Call{{CallData: []byte{0x01}}}, nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	d.Close()

	if len(arch.records) != 1 {
		t.Fatalf("archived %d records, want 1", len(arch.records))
	}
	if arch.records[0].BlockNumber != 0 {
		t.Fatalf("nil blockNumber should yield BlockNumber 0, got %d", arch.records[0].BlockNumber)
	}
}

func TestExecuteTruncatesWhenFewerResultsThanCalls(t *testing.T) {
	// Inner returns only one result for two calls; only the first is archived.
	inner := &stubInner{results: []outbound.Result{{Success: true, ReturnData: []byte{0xaa}}}}
	arch := &recordingArchiver{}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, arch, &wg)

	calls := []outbound.Call{
		{Target: common.HexToAddress("0x01"), CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
		{Target: common.HexToAddress("0x02"), CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
	}
	if _, err := d.Execute(context.Background(), calls, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	d.Close()

	if len(arch.records) != 1 {
		t.Fatalf("archived %d records, want 1 (truncated to results length)", len(arch.records))
	}
}

func TestExecuteDefensivelyCopiesCallData(t *testing.T) {
	inner := &stubInner{results: []outbound.Result{{Success: true, ReturnData: []byte{0xaa, 0xbb}}}}
	arch := &recordingArchiver{}
	var wg sync.WaitGroup
	d := newTestDecorator(inner, arch, &wg)

	callData := []byte{0xfe, 0xaf, 0x96, 0x8c}
	calls := []outbound.Call{{Target: common.HexToAddress("0x01"), CallData: callData}}
	if _, err := d.Execute(context.Background(), calls, big.NewInt(1)); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	d.Close()

	// Mutate the caller's slice after Execute returned. The archived record must
	// be unaffected because buildRecord copies the bytes.
	callData[0] = 0x00

	if len(arch.records) != 1 {
		t.Fatalf("archived %d records, want 1", len(arch.records))
	}
	got := arch.records[0].CallData
	if len(got) != 4 || got[0] != 0xfe {
		t.Fatalf("archived CallData was not defensively copied: % x", got)
	}
}
