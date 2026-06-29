package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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

func (r *fakeEventRepo) SaveBatch(_ context.Context, _ pgx.Tx, evts []*entity.ProtocolEvent) error {
	if r.err != nil {
		return r.err
	}
	r.saved = append(r.saved, evts...)
	return nil
}

// fakeTxManager runs the function with a nil tx (the fakes ignore it), standing
// in for the committed transaction ResolveProtocolID opens.
type fakeTxManager struct{ err error }

func (m fakeTxManager) WithTransaction(_ context.Context, fn func(pgx.Tx) error) error {
	if m.err != nil {
		return m.err
	}
	return fn(nil)
}

func curveDescriptor() ProtocolDescriptor {
	return ProtocolDescriptor{
		Address:      common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Name:         "Curve",
		ProtocolType: "dex",
		DeployBlock:  9456293,
	}
}

func TestResolveProtocolID(t *testing.T) {
	repo := &fakeProtocolRepo{id: 7}
	id, err := ResolveProtocolID(context.Background(), fakeTxManager{}, repo, curveDescriptor(), 1)
	if err != nil {
		t.Fatalf("ResolveProtocolID: %v", err)
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
	if got := repo.calls.Load(); got != 1 {
		t.Errorf("GetOrCreateProtocol called %d times, want 1", got)
	}
}

func TestResolveProtocolID_PropagatesRepoError(t *testing.T) {
	sentinel := errors.New("db down")
	_, err := ResolveProtocolID(context.Background(), fakeTxManager{}, &fakeProtocolRepo{err: sentinel}, curveDescriptor(), 1)
	if err == nil {
		t.Fatal("expected error when the repo fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the repo error", err)
	}
}

func TestResolveProtocolID_PropagatesTxError(t *testing.T) {
	sentinel := errors.New("tx begin failed")
	_, err := ResolveProtocolID(context.Background(), fakeTxManager{err: sentinel}, &fakeProtocolRepo{id: 1}, curveDescriptor(), 1)
	if err == nil {
		t.Fatal("expected error when the transaction fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the tx error", err)
	}
}

func TestProtocolEventWriter_BuildsAndPersistsEvent(t *testing.T) {
	events := &fakeEventRepo{}
	w := NewProtocolEventWriter(5, events)

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

func TestProtocolEventWriter_PropagatesSaveError(t *testing.T) {
	sentinel := errors.New("insert failed")
	w := NewProtocolEventWriter(1, &fakeEventRepo{err: sentinel})

	err := w.Save(context.Background(), nil, validInput())
	if err == nil {
		t.Fatal("expected error when SaveEvent fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the SaveEvent error", err)
	}
}

func TestProtocolEventWriter_InvalidEventIsRejected(t *testing.T) {
	events := &fakeEventRepo{}
	w := NewProtocolEventWriter(1, events)

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

func TestProtocolEventWriter_SaveBatch_EmptyInputIsNoOp(t *testing.T) {
	events := &fakeEventRepo{}
	w := NewProtocolEventWriter(1, events)

	if err := w.SaveBatch(context.Background(), nil, nil); err != nil {
		t.Fatalf("SaveBatch(empty): %v", err)
	}
	if len(events.saved) != 0 {
		t.Errorf("empty input persisted %d events, want 0", len(events.saved))
	}
}

func TestProtocolEventWriter_SaveBatch_BuildsAndPersistsAll(t *testing.T) {
	events := &fakeEventRepo{}
	w := NewProtocolEventWriter(5, events)

	in1 := validInput()
	in2 := validInput()
	in2.LogIndex = 1

	if err := w.SaveBatch(context.Background(), nil, []ProtocolEventInput{in1, in2}); err != nil {
		t.Fatalf("SaveBatch: %v", err)
	}
	if len(events.saved) != 2 {
		t.Fatalf("saved %d events, want 2", len(events.saved))
	}
	if events.saved[0].ProtocolID != 5 {
		t.Errorf("protocol id = %d, want 5", events.saved[0].ProtocolID)
	}
}

func TestProtocolEventWriter_SaveBatch_InvalidEventIsRejected(t *testing.T) {
	events := &fakeEventRepo{}
	w := NewProtocolEventWriter(1, events)

	bad := validInput()
	bad.BlockTimestamp = time.Time{} // zero CreatedAt fails entity validation

	err := w.SaveBatch(context.Background(), nil, []ProtocolEventInput{validInput(), bad})
	if err == nil {
		t.Fatal("expected error building an invalid protocol_event")
	}
	if !strings.Contains(err.Error(), "building protocol_event") {
		t.Errorf("error %q should describe the event-build step", err)
	}
	if len(events.saved) != 0 {
		t.Error("a batch containing an invalid event must persist nothing")
	}
}

func TestProtocolEventWriter_SaveBatch_PropagatesRepoError(t *testing.T) {
	sentinel := errors.New("batch insert failed")
	w := NewProtocolEventWriter(1, &fakeEventRepo{err: sentinel})

	err := w.SaveBatch(context.Background(), nil, []ProtocolEventInput{validInput()})
	if err == nil {
		t.Fatal("expected error when the repo SaveBatch fails")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %v does not wrap the repo error", err)
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
