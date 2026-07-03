package dexconsumer

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// hugeUint256 is uint256 max — far beyond a float64's 2^53 lossless range, so a
// bare-number JSON encoding would silently truncate it.
func hugeUint256(t *testing.T) *big.Int {
	t.Helper()
	bi, ok := new(big.Int).SetString(
		"115792089237316195423570985008687907853269984665640564039457584007913129639935", 10)
	if !ok {
		t.Fatal("parsing uint256 max")
	}
	return bi
}

// TestNewDecodedCapturedLog_StringifiesBigInts proves the shared decoded
// capture-net helper encodes every *big.Int as a lossless JSON string — both at
// the top level and nested inside a slice (Curve NG uint256[] arrays) — while
// leaving non-big-int scalars in their natural JSON form (PR#519 review #5).
func TestNewDecodedCapturedLog_StringifiesBigInts(t *testing.T) {
	huge := hugeUint256(t)
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	txHash := common.HexToHash("0xabc")
	eventData := map[string]any{
		"amount":        huge,
		"token_amounts": []*big.Int{huge, big.NewInt(200)},
		"provider":      common.HexToAddress("0x2222222222222222222222222222222222222222"),
	}

	got, err := NewDecodedCapturedLog(addr, 7, txHash, "AddLiquidity", eventData)
	if err != nil {
		t.Fatalf("NewDecodedCapturedLog: %v", err)
	}
	if got.Address != addr || got.LogIndex != 7 || got.TxHash != txHash || got.EventName != "AddLiquidity" {
		t.Errorf("metadata = {%s,%d,%s,%s}, want {%s,7,%s,AddLiquidity}",
			got.Address, got.LogIndex, got.TxHash, got.EventName, addr, txHash)
	}

	var payload map[string]any
	if err := json.Unmarshal(got.Payload, &payload); err != nil {
		t.Fatalf("unmarshalling payload: %v", err)
	}
	if s, ok := payload["amount"].(string); !ok || s != huge.String() {
		t.Errorf("amount = %v (%T), want string %q", payload["amount"], payload["amount"], huge.String())
	}
	arr, ok := payload["token_amounts"].([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("token_amounts = %v (%T), want 2-elem JSON array", payload["token_amounts"], payload["token_amounts"])
	}
	if s, ok := arr[0].(string); !ok || s != huge.String() {
		t.Errorf("token_amounts[0] = %v (%T), want string %q", arr[0], arr[0], huge.String())
	}
	// Non-big-int scalars keep their natural JSON form (address as its String()).
	if _, ok := payload["provider"].(string); !ok {
		t.Errorf("provider = %v (%T), want its natural string form", payload["provider"], payload["provider"])
	}
}

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
