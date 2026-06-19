package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type fakeProtocolRepo struct {
	outbound.ProtocolRepository
	id    int64
	err   error
	calls atomic.Int64

	gotChain int64
	gotAddr  common.Address
	gotName  string
	gotType  string
	gotBlock int64
}

func (r *fakeProtocolRepo) GetOrCreateProtocol(_ context.Context, _ pgx.Tx, chainID int64, address common.Address, name, protocolType string, createdAtBlock int64) (int64, error) {
	r.calls.Add(1)
	r.gotChain, r.gotAddr, r.gotName, r.gotType, r.gotBlock = chainID, address, name, protocolType, createdAtBlock
	if r.err != nil {
		return 0, r.err
	}
	return r.id, nil
}

type fakeEventRepo struct {
	err   error
	saved []*entity.ProtocolEvent
}

func (r *fakeEventRepo) SaveEvent(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
	if r.err != nil {
		return r.err
	}
	r.saved = append(r.saved, e)
	return nil
}

func curveDescriptor() ProtocolDescriptor {
	return ProtocolDescriptor{
		Address:      common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Name:         "Curve",
		ProtocolType: "dex",
		DeployBlock:  9456293,
	}
}

func TestProtocolIDResolver_CreatesThenCaches(t *testing.T) {
	repo := &fakeProtocolRepo{id: 7}
	r := NewProtocolIDResolver(repo, curveDescriptor())

	id, err := r.Resolve(context.Background(), nil, 1)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if id != 7 {
		t.Errorf("id = %d, want 7", id)
	}
	if repo.gotChain != 1 || repo.gotName != "Curve" || repo.gotType != "dex" || repo.gotBlock != 9456293 {
		t.Errorf("GetOrCreateProtocol got (chain=%d,name=%q,type=%q,block=%d), want (1,Curve,dex,9456293)",
			repo.gotChain, repo.gotName, repo.gotType, repo.gotBlock)
	}
	if repo.gotAddr != curveDescriptor().Address {
		t.Errorf("GetOrCreateProtocol address = %s, want %s", repo.gotAddr, curveDescriptor().Address)
	}

	id2, err := r.Resolve(context.Background(), nil, 1)
	if err != nil {
		t.Fatalf("second Resolve: %v", err)
	}
	if id2 != 7 {
		t.Errorf("cached id = %d, want 7", id2)
	}
	if got := repo.calls.Load(); got != 1 {
		t.Errorf("GetOrCreateProtocol called %d times, want 1 (result must be cached)", got)
	}
}

func TestProtocolIDResolver_PropagatesRepoError(t *testing.T) {
	sentinel := errors.New("db down")
	repo := &fakeProtocolRepo{err: sentinel}
	r := NewProtocolIDResolver(repo, curveDescriptor())

	_, err := r.Resolve(context.Background(), nil, 1)
	if err == nil {
		t.Fatal("expected error when the repo fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the repo error", err)
	}
}

func TestProtocolIDResolver_ResolvesOnceUnderConcurrency(t *testing.T) {
	repo := &fakeProtocolRepo{id: 42}
	r := NewProtocolIDResolver(repo, curveDescriptor())

	const goroutines = 50
	var wg sync.WaitGroup
	ids := make([]int64, goroutines)
	for i := range goroutines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := r.Resolve(context.Background(), nil, 1)
			if err != nil {
				t.Errorf("Resolve: %v", err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()

	if got := repo.calls.Load(); got != 1 {
		t.Errorf("GetOrCreateProtocol called %d times under concurrency, want exactly 1", got)
	}
	for i, id := range ids {
		if id != 42 {
			t.Errorf("goroutine %d got id %d, want 42", i, id)
		}
	}
}

// blockingProtocolRepo holds the mutex inside GetOrCreateProtocol until released,
// so a second resolver caller is forced to park on the lock and then return via
// the double-checked inner branch rather than calling the repo again.
type blockingProtocolRepo struct {
	outbound.ProtocolRepository
	id      int64
	calls   atomic.Int64
	entered chan struct{}
	release chan struct{}
}

func (r *blockingProtocolRepo) GetOrCreateProtocol(_ context.Context, _ pgx.Tx, _ int64, _ common.Address, _, _ string, _ int64) (int64, error) {
	r.calls.Add(1)
	close(r.entered)
	<-r.release
	return r.id, nil
}

func TestProtocolIDResolver_SecondCallerThroughLockUsesCache(t *testing.T) {
	repo := &blockingProtocolRepo{id: 99, entered: make(chan struct{}), release: make(chan struct{})}
	r := NewProtocolIDResolver(repo, curveDescriptor())

	resA := make(chan int64, 1)
	go func() {
		id, err := r.Resolve(context.Background(), nil, 1)
		if err != nil {
			t.Errorf("first Resolve: %v", err)
		}
		resA <- id
	}()
	<-repo.entered // first caller holds the mutex inside GetOrCreateProtocol; id still unset

	resB := make(chan int64, 1)
	go func() {
		id, err := r.Resolve(context.Background(), nil, 1)
		if err != nil {
			t.Errorf("second Resolve: %v", err)
		}
		resB <- id
	}()

	// The second caller reads id==0 on the outer fast-path (the first caller has
	// not stored yet) and parks on the mutex. Releasing the first caller now lets
	// it store the id; the second caller then acquires the lock and must return
	// the cached id via the inner re-check, without a second repo call.
	time.Sleep(50 * time.Millisecond)
	close(repo.release)

	if id := <-resA; id != 99 {
		t.Errorf("first caller got %d, want 99", id)
	}
	if id := <-resB; id != 99 {
		t.Errorf("second caller got %d, want 99", id)
	}
	if got := repo.calls.Load(); got != 1 {
		t.Errorf("GetOrCreateProtocol called %d times, want 1 (second caller must hit the cache through the lock)", got)
	}
}

func TestProtocolEventWriter_BuildsAndPersistsEvent(t *testing.T) {
	repo := &fakeProtocolRepo{id: 5}
	resolver := NewProtocolIDResolver(repo, curveDescriptor())
	events := &fakeEventRepo{}
	w := NewProtocolEventWriter(resolver, events)

	in := ProtocolEventInput{
		ContractAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		ChainID:         8453,
		BlockNumber:     1234,
		BlockVersion:    1,
		BlockTimestamp:  time.Unix(1_700_000_000, 0).UTC(),
		TxHash:          common.HexToHash("0xdeadbeef"),
		LogIndex:        9,
		EventName:       "TokenExchange",
		Payload:         json.RawMessage(`{"k":"v"}`),
	}
	if err := w.Save(context.Background(), nil, in); err != nil {
		t.Fatalf("Save: %v", err)
	}

	if len(events.saved) != 1 {
		t.Fatalf("saved %d events, want 1", len(events.saved))
	}
	got := events.saved[0]
	if got.ChainID != 8453 || got.ProtocolID != 5 || got.BlockNumber != 1234 || got.BlockVersion != 1 {
		t.Errorf("event header = (chain=%d,protocol=%d,block=%d,version=%d), want (8453,5,1234,1)",
			got.ChainID, got.ProtocolID, got.BlockNumber, got.BlockVersion)
	}
	if got.LogIndex != 9 || got.EventName != "TokenExchange" {
		t.Errorf("event = (logIndex=%d,name=%q), want (9,TokenExchange)", got.LogIndex, got.EventName)
	}
	if string(got.TxHash) != string(in.TxHash.Bytes()) {
		t.Errorf("tx hash bytes mismatch")
	}
	if string(got.ContractAddress) != string(in.ContractAddress.Bytes()) {
		t.Errorf("contract address bytes mismatch")
	}
	if string(got.EventData) != `{"k":"v"}` {
		t.Errorf("event data = %s, want the supplied payload", got.EventData)
	}
}

func TestProtocolEventWriter_PropagatesResolverError(t *testing.T) {
	sentinel := errors.New("resolve failed")
	resolver := NewProtocolIDResolver(&fakeProtocolRepo{err: sentinel}, curveDescriptor())
	w := NewProtocolEventWriter(resolver, &fakeEventRepo{})

	err := w.Save(context.Background(), nil, validInput())
	if err == nil {
		t.Fatal("expected error when protocol-id resolution fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the resolver error", err)
	}
}

func TestProtocolEventWriter_PropagatesSaveError(t *testing.T) {
	sentinel := errors.New("insert failed")
	resolver := NewProtocolIDResolver(&fakeProtocolRepo{id: 1}, curveDescriptor())
	w := NewProtocolEventWriter(resolver, &fakeEventRepo{err: sentinel})

	err := w.Save(context.Background(), nil, validInput())
	if err == nil {
		t.Fatal("expected error when SaveEvent fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the SaveEvent error", err)
	}
}

func TestProtocolEventWriter_InvalidEventIsRejected(t *testing.T) {
	resolver := NewProtocolIDResolver(&fakeProtocolRepo{id: 1}, curveDescriptor())
	events := &fakeEventRepo{}
	w := NewProtocolEventWriter(resolver, events)

	in := validInput()
	in.BlockTimestamp = time.Time{} // zero CreatedAt fails entity validation

	err := w.Save(context.Background(), nil, in)
	if err == nil {
		t.Fatal("expected error building an invalid protocol_event")
	}
	if !strings.Contains(err.Error(), "building protocol_event") {
		t.Errorf("error %q should describe the event-build step", err)
	}
	if len(events.saved) != 0 {
		t.Error("an invalid event must not be persisted")
	}
}

func validInput() ProtocolEventInput {
	return ProtocolEventInput{
		ContractAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		ChainID:         1,
		BlockNumber:     1,
		BlockVersion:    0,
		BlockTimestamp:  time.Unix(1_700_000_000, 0).UTC(),
		TxHash:          common.HexToHash("0xabc"),
		LogIndex:        0,
		EventName:       "Swap",
		Payload:         json.RawMessage(`{}`),
	}
}
