package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// runningTxManager runs fn with a nil pgx.Tx (the fake repos ignore it),
// mirroring a committed transaction; a non-nil fn error propagates as a
// rolled-back transaction would.
type runningTxManager struct {
	beginErr error
	ran      bool
}

func (m *runningTxManager) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	if m.beginErr != nil {
		return m.beginErr
	}
	m.ran = true
	return fn(nil)
}

func TestRawCapturedPayload_MirrorsTopicsAndData(t *testing.T) {
	log := shared.Log{
		Topics: []string{"0xaa", "0xbb"},
		Data:   "0xdeadbeef",
	}

	payload, err := RawCapturedPayload(log)
	if err != nil {
		t.Fatalf("RawCapturedPayload: %v", err)
	}

	var got struct {
		Topics []string `json:"topics"`
		Data   string   `json:"data"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("unmarshalling payload: %v", err)
	}
	if len(got.Topics) != 2 || got.Topics[0] != "0xaa" || got.Topics[1] != "0xbb" {
		t.Errorf("topics = %v, want [0xaa 0xbb]", got.Topics)
	}
	if got.Data != "0xdeadbeef" {
		t.Errorf("data = %q, want 0xdeadbeef", got.Data)
	}
}

func TestNewRawCapturedLog_SetsFieldsAndEventName(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	txHash := common.HexToHash("0xabc")
	log := shared.Log{Topics: nil, Data: "0x01"}

	got, err := NewRawCapturedLog(addr, 7, txHash, AnonymousLogEventName, log)
	if err != nil {
		t.Fatalf("NewRawCapturedLog: %v", err)
	}
	if got.Address != addr {
		t.Errorf("Address = %s, want %s", got.Address, addr)
	}
	if got.LogIndex != 7 {
		t.Errorf("LogIndex = %d, want 7", got.LogIndex)
	}
	if got.TxHash != txHash {
		t.Errorf("TxHash = %s, want %s", got.TxHash, txHash)
	}
	if got.EventName != AnonymousLogEventName {
		t.Errorf("EventName = %q, want %q", got.EventName, AnonymousLogEventName)
	}
}

// TestAnonymousLogEventName_SurvivesProtocolEventValidation guards the invariant
// that made an empty event_name poison-stall the worker: NewProtocolEvent
// rejects an empty EventName, so the zero-topic sentinel must pass validation.
func TestAnonymousLogEventName_SurvivesProtocolEventValidation(t *testing.T) {
	_, err := entity.NewProtocolEvent(
		1, 1, 1, 0,
		[]byte{0x01}, 0, []byte{0x02},
		AnonymousLogEventName,
		json.RawMessage(`{}`),
		time.Unix(1, 0).UTC(),
	)
	if err != nil {
		t.Fatalf("AnonymousLogEventName must survive protocol_event validation: %v", err)
	}
}

func TestToProtocolEventInputs_EmptyInput(t *testing.T) {
	got := ToProtocolEventInputs(nil, 1, 100, 0, time.Unix(1, 0).UTC())
	if len(got) != 0 {
		t.Errorf("ToProtocolEventInputs(nil) = %d inputs, want 0", len(got))
	}
}

func TestToProtocolEventInputs_MapsEveryFieldAndStampsBlockIdentity(t *testing.T) {
	ts := time.Unix(1_700_000_000, 0).UTC()
	addr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	txHash := common.HexToHash("0xfeed")
	captured := []CapturedLog{{
		Address:   addr,
		LogIndex:  3,
		TxHash:    txHash,
		EventName: "Swap",
		Payload:   json.RawMessage(`{"k":1}`),
	}}

	got := ToProtocolEventInputs(captured, 42, 555, 2, ts)
	if len(got) != 1 {
		t.Fatalf("got %d inputs, want 1", len(got))
	}
	in := got[0]
	want := ProtocolEventInput{
		ContractAddress: addr,
		ChainID:         42,
		BlockNumber:     555,
		BlockVersion:    2,
		BlockTimestamp:  ts,
		TxHash:          txHash,
		LogIndex:        3,
		EventName:       "Swap",
		Payload:         json.RawMessage(`{"k":1}`),
	}
	if in.ContractAddress != want.ContractAddress || in.ChainID != want.ChainID ||
		in.BlockNumber != want.BlockNumber || in.BlockVersion != want.BlockVersion ||
		!in.BlockTimestamp.Equal(want.BlockTimestamp) || in.TxHash != want.TxHash ||
		in.LogIndex != want.LogIndex || in.EventName != want.EventName ||
		string(in.Payload) != string(want.Payload) {
		t.Errorf("mapped input = %+v, want %+v", in, want)
	}
}

func TestPersistBlock_SavesBlockThenCapturedEvents(t *testing.T) {
	events := &fakeEventRepo{}
	writer := NewProtocolEventWriter(1, events)
	txMgr := &runningTxManager{}

	var saveCalled bool
	save := func(_ context.Context, _ pgx.Tx) (int64, error) {
		saveCalled = true
		return 4, nil
	}
	captured := ToProtocolEventInputs([]CapturedLog{{
		Address:   common.HexToAddress("0x3333333333333333333333333333333333333333"),
		LogIndex:  0,
		TxHash:    common.HexToHash("0xabc"),
		EventName: "Swap",
		Payload:   json.RawMessage(`{}`),
	}}, 1, 10, 0, time.Unix(1, 0).UTC())

	rows, err := PersistBlock(context.Background(), txMgr, writer, save, captured, 10)
	if err != nil {
		t.Fatalf("PersistBlock: %v", err)
	}
	if rows != 4 {
		t.Errorf("state rows = %d, want 4 (from save closure)", rows)
	}
	if !saveCalled {
		t.Error("save closure was not called")
	}
	if len(events.saved) != 1 {
		t.Errorf("persisted %d captured events, want 1", len(events.saved))
	}
}

func TestPersistBlock_SaveErrorAbortsBeforeCapturedWrite(t *testing.T) {
	events := &fakeEventRepo{}
	writer := NewProtocolEventWriter(1, events)
	txMgr := &runningTxManager{}

	sentinel := errors.New("save block failed")
	save := func(_ context.Context, _ pgx.Tx) (int64, error) {
		return 0, sentinel
	}

	_, err := PersistBlock(context.Background(), txMgr, writer, save, []ProtocolEventInput{validInput()}, 10)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v does not wrap the save error", err)
	}
	if len(events.saved) != 0 {
		t.Error("captured events must not be written when SaveBlock fails")
	}
}

func TestPersistBlock_CapturedWriteErrorPropagates(t *testing.T) {
	sentinel := errors.New("batch insert failed")
	writer := NewProtocolEventWriter(1, &fakeEventRepo{err: sentinel})
	txMgr := &runningTxManager{}

	save := func(_ context.Context, _ pgx.Tx) (int64, error) { return 1, nil }

	_, err := PersistBlock(context.Background(), txMgr, writer, save, []ProtocolEventInput{validInput()}, 10)
	if !errors.Is(err, sentinel) {
		t.Fatalf("error %v does not wrap the event-writer error", err)
	}
}
