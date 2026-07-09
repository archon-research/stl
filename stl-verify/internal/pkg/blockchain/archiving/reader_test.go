package archiving

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type stubStateReader struct {
	results []outbound.Result
	err     error
}

func (s *stubStateReader) Read(_ context.Context, _ outbound.BlockPin, _ []outbound.Call) ([]outbound.Result, error) {
	return s.results, s.err
}

type capturingArchiver struct {
	mu      sync.Mutex
	records []outbound.CallBatchRecord
}

func (a *capturingArchiver) Archive(_ context.Context, r outbound.CallBatchRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, r)
	return nil
}

func newTestReader(inner outbound.StateReader, arch outbound.CallArchiver, wg *sync.WaitGroup) *Reader {
	return NewReader(inner, common.HexToAddress("0xcA11"), arch, Config{
		Source: "test", ChainID: 1, Chain: "mainnet", Wait: wg,
	})
}

func TestReader_ArchivesWithPinNumberAndVersion(t *testing.T) {
	arch := &capturingArchiver{}
	wg := &sync.WaitGroup{}
	inner := &stubStateReader{results: []outbound.Result{{Success: true}}}
	pin := outbound.PinForSettledBlock(23145001, 2)
	calls := []outbound.Call{{Target: common.HexToAddress("0x1"), CallData: []byte{0xaa, 0xbb, 0xcc, 0xdd}}}

	if _, err := newTestReader(inner, arch, wg).Read(context.Background(), pin, calls); err != nil {
		t.Fatalf("Read: %v", err)
	}
	wg.Wait()

	if len(arch.records) != 1 {
		t.Fatalf("records = %d, want 1", len(arch.records))
	}
	r := arch.records[0]
	if r.BlockNumber != 23145001 || r.BlockVersion != 2 {
		t.Fatalf("record keyed at block %d v%d, want 23145001 v2 (from the pin, not context)", r.BlockNumber, r.BlockVersion)
	}
}

func TestReader_InnerError_ArchivesNothing(t *testing.T) {
	arch := &capturingArchiver{}
	wg := &sync.WaitGroup{}
	inner := &stubStateReader{err: errors.New("rpc down")}

	_, err := newTestReader(inner, arch, wg).Read(context.Background(), outbound.PinForSettledBlock(100, 0), []outbound.Call{{}})
	wg.Wait()

	if err == nil {
		t.Fatal("expected inner error to propagate")
	}
	if len(arch.records) != 0 {
		t.Fatalf("records = %d, want 0", len(arch.records))
	}
}

func TestReader_EmptyBatch_ArchivesNothing(t *testing.T) {
	arch := &capturingArchiver{}
	wg := &sync.WaitGroup{}
	inner := &stubStateReader{results: nil}

	if _, err := newTestReader(inner, arch, wg).Read(context.Background(), outbound.PinForSettledBlock(100, 0), nil); err != nil {
		t.Fatalf("Read: %v", err)
	}
	wg.Wait()

	if len(arch.records) != 0 {
		t.Fatalf("records = %d, want 0 (no phantom empty object)", len(arch.records))
	}
}
