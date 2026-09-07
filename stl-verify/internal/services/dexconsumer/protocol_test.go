package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

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
