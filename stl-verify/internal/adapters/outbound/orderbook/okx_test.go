package orderbook

import (
	"errors"
	"strconv"
	"testing"
)

func newOKXHandler() *okxHandler {
	return &okxHandler{books: newBookSet(exchangeOKX), lastSeq: make(map[string]int64)}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestOKXHandlerSnapshotThenUpdate(t *testing.T) {
	h := newOKXHandler()

	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3","0","1"]],"bids":[["100","2","0","1"]],"ts":"1700000000000","seqId":10,"prevSeqId":-1}]}`
	sigs, err := h.handle([]byte(snapshot))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(sigs) != 1 || !sigs[0].isSnapshot {
		t.Fatalf("expected snapshot signal, got %+v", sigs)
	}
	if sz, ok := sizeAt(sigs[0].book.Bids(), "100"); !ok || sz != "2" {
		t.Errorf("bid 100 = %q (ok=%v), want 2", sz, ok)
	}
	if sigs[0].book.LastUpdateID != 10 {
		t.Errorf("LastUpdateID = %d, want 10", sigs[0].book.LastUpdateID)
	}

	// Contiguous update: prevSeqId must equal the last applied seqId.
	update := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["100","0","0","0"],["99","7","0","1"]],"ts":"1700000000001","seqId":11,"prevSeqId":10}]}`
	sigs, err = h.handle([]byte(update))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(sigs) != 1 || sigs[0].isSnapshot {
		t.Fatalf("expected non-snapshot signal, got %+v", sigs)
	}
	book := sigs[0].book
	if _, ok := sizeAt(book.Bids(), "100"); ok {
		t.Error("bid 100 should be removed by zero size")
	}
	if sz, ok := sizeAt(book.Bids(), "99"); !ok || sz != "7" {
		t.Errorf("bid 99 after update = %q (ok=%v), want 7", sz, ok)
	}
}

// TestOKXHandlerPeriodicResnapshot covers OKX's self-heal: the books channel
// periodically re-sends a full snapshot action mid-stream. The re-snapshot must
// replace the book (old levels gone), flag isSnapshot, and advance the sequence
// so subsequent contiguous updates continue to apply cleanly.
func TestOKXHandlerPeriodicResnapshot(t *testing.T) {
	h := newOKXHandler()

	// 1. Initial snapshot (seqId 10) builds the book.
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3","0","1"]],"bids":[["100","2","0","1"]],"ts":"1700000000000","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("initial snapshot: %v", err)
	}

	// 2. Contiguous update (seqId 11, prevSeqId 10).
	update := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["99","7","0","1"]],"ts":"1700000000001","seqId":11,"prevSeqId":10}]}`
	if _, err := h.handle([]byte(update)); err != nil {
		t.Fatalf("update: %v", err)
	}

	// 3. A second snapshot (seqId 20) with different levels self-heals the book.
	resnapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["201","4","0","1"]],"bids":[["200","5","0","1"]],"ts":"1700000000002","seqId":20,"prevSeqId":-1}]}`
	sigs, err := h.handle([]byte(resnapshot))
	if err != nil {
		t.Fatalf("re-snapshot: %v", err)
	}
	if len(sigs) != 1 || !sigs[0].isSnapshot {
		t.Fatalf("expected a snapshot signal, got %+v", sigs)
	}
	book := sigs[0].book
	// Only the new snapshot levels remain; the old ones are gone.
	if _, ok := sizeAt(book.Bids(), "100"); ok {
		t.Error("old bid 100 should be cleared by the re-snapshot")
	}
	if _, ok := sizeAt(book.Bids(), "99"); ok {
		t.Error("old bid 99 should be cleared by the re-snapshot")
	}
	if _, ok := sizeAt(book.Asks(), "101"); ok {
		t.Error("old ask 101 should be cleared by the re-snapshot")
	}
	if sz, ok := sizeAt(book.Bids(), "200"); !ok || sz != "5" {
		t.Errorf("bid 200 after re-snapshot = %q (ok=%v), want 5", sz, ok)
	}
	if sz, ok := sizeAt(book.Asks(), "201"); !ok || sz != "4" {
		t.Errorf("ask 201 after re-snapshot = %q (ok=%v), want 4", sz, ok)
	}
	if book.LastUpdateID != 20 {
		t.Errorf("LastUpdateID = %d, want 20", book.LastUpdateID)
	}

	// 4. A follow-up update (seqId 21, prevSeqId 20) applies cleanly: continuity
	// continues from the re-snapshot's sequence.
	followUp := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["199","6","0","1"]],"ts":"1700000000003","seqId":21,"prevSeqId":20}]}`
	sigs, err = h.handle([]byte(followUp))
	if err != nil {
		t.Fatalf("follow-up update after re-snapshot: %v", err)
	}
	if len(sigs) != 1 || sigs[0].isSnapshot {
		t.Fatalf("expected a non-snapshot signal, got %+v", sigs)
	}
	book = sigs[0].book
	if sz, ok := sizeAt(book.Bids(), "199"); !ok || sz != "6" {
		t.Errorf("bid 199 after follow-up = %q (ok=%v), want 6", sz, ok)
	}
	if book.LastUpdateID != 21 {
		t.Errorf("LastUpdateID = %d, want 21", book.LastUpdateID)
	}
}

func TestOKXHandlerSequenceGap(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// prevSeqId 99 does not match last applied seqId 10.
	gap := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"bids":[["100","1"]],"ts":"2","seqId":100,"prevSeqId":99}]}`
	_, err := h.handle([]byte(gap))
	if !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap", err)
	}
}

func TestOKXHandlerUpdateBeforeSnapshotIsRejected(t *testing.T) {
	h := newOKXHandler()
	// An update arriving with no prior snapshot must not be applied to an empty
	// book; it must force a re-sync.
	update := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"bids":[["100","1"]],"ts":"1","seqId":11,"prevSeqId":10}]}`
	_, err := h.handle([]byte(update))
	if !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap", err)
	}
}

func TestOKXHandlerNoOpRefreshSkipped(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// A no-change push (seqId == prevSeqId) is skipped without error or signal.
	noop := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[],"ts":"2","seqId":20,"prevSeqId":20}]}`
	sigs, err := h.handle([]byte(noop))
	if err != nil {
		t.Fatalf("no-op: %v", err)
	}
	if len(sigs) != 0 {
		t.Errorf("no-op refresh should emit no signals, got %d", len(sigs))
	}
}

// TestOKXHandlerNoOpAdvancesSequence covers Issue 1: a no-change push carries a
// new seqId (seqId == prevSeqId but != last applied). The handler must record
// that seqId so the next real update, whose prevSeqId chains off it, applies
// without a spurious sequence gap.
func TestOKXHandlerNoOpAdvancesSequence(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// No-change push carrying a NEW seqId (15) advances the sequence to 15.
	noop := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[],"ts":"2","seqId":15,"prevSeqId":15}]}`
	sigs, err := h.handle([]byte(noop))
	if err != nil {
		t.Fatalf("no-op: %v", err)
	}
	if len(sigs) != 0 {
		t.Errorf("no-op refresh should emit no signals, got %d", len(sigs))
	}

	// The next real update chains off the no-op's seqId (prevSeqId 15) and must
	// apply with NO gap.
	update := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["99","7"]],"ts":"3","seqId":16,"prevSeqId":15}]}`
	sigs, err = h.handle([]byte(update))
	if err != nil {
		t.Fatalf("update after no-op should not gap: %v", err)
	}
	if len(sigs) != 1 || sigs[0].isSnapshot {
		t.Fatalf("expected one non-snapshot signal, got %+v", sigs)
	}
	if sz, ok := sizeAt(sigs[0].book.Bids(), "99"); !ok || sz != "7" {
		t.Errorf("bid 99 after update = %q (ok=%v), want 7", sz, ok)
	}
	if sigs[0].book.LastUpdateID != 16 {
		t.Errorf("LastUpdateID = %d, want 16", sigs[0].book.LastUpdateID)
	}
}

// TestOKXHandlerConsecutiveNoOpsThenUpdate covers Issue 1 with several no-op
// pushes in a row before a real update: each must advance the sequence so the
// final contiguous update applies cleanly.
func TestOKXHandlerConsecutiveNoOpsThenUpdate(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	for _, seq := range []int64{11, 12, 13} {
		noop := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
			{"asks":[],"bids":[],"ts":"2","seqId":` + itoa(seq) + `,"prevSeqId":` + itoa(seq) + `}]}`
		sigs, err := h.handle([]byte(noop))
		if err != nil {
			t.Fatalf("no-op seqId=%d: %v", seq, err)
		}
		if len(sigs) != 0 {
			t.Errorf("no-op seqId=%d should emit no signals, got %d", seq, len(sigs))
		}
	}

	// Real update chaining off the last no-op (prevSeqId 13).
	update := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["99","7"]],"ts":"3","seqId":14,"prevSeqId":13}]}`
	sigs, err := h.handle([]byte(update))
	if err != nil {
		t.Fatalf("update after consecutive no-ops should not gap: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("expected one signal, got %+v", sigs)
	}
	if sigs[0].book.LastUpdateID != 14 {
		t.Errorf("LastUpdateID = %d, want 14", sigs[0].book.LastUpdateID)
	}
}

// TestOKXHandlerGapAfterNoOpStillRejected pins the surrounding correct behaviour
// for Issue 1: advancing the sequence on a no-op must NOT mask a genuine gap. An
// update whose prevSeqId matches neither the no-op's seqId nor seqId itself must
// still force a re-sync.
func TestOKXHandlerGapAfterNoOpStillRejected(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	noop := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[],"ts":"2","seqId":15,"prevSeqId":15}]}`
	if _, err := h.handle([]byte(noop)); err != nil {
		t.Fatalf("no-op: %v", err)
	}
	// prevSeqId 50 chains off neither the no-op seqId (15) nor itself: a real gap.
	gap := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"bids":[["99","1"]],"ts":"3","seqId":51,"prevSeqId":50}]}`
	if _, err := h.handle([]byte(gap)); !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap", err)
	}
}

// TestOKXHandlerMultiObjectSnapshot covers Issue 4: a snapshot frame carrying
// more than one data object must reset the book ONCE for the frame, not once per
// object, so levels from every object survive.
func TestOKXHandlerMultiObjectSnapshot(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1},
		{"asks":[["103","5"]],"bids":[["98","6"]],"ts":"2","seqId":11,"prevSeqId":-1}]}`
	sigs, err := h.handle([]byte(snapshot))
	if err != nil {
		t.Fatalf("multi-object snapshot: %v", err)
	}
	if len(sigs) != 2 {
		t.Fatalf("expected 2 snapshot signals (one per object), got %d", len(sigs))
	}
	for _, s := range sigs {
		if !s.isSnapshot {
			t.Error("every signal in a snapshot frame should be flagged isSnapshot")
		}
	}
	book := sigs[len(sigs)-1].book
	// Levels from BOTH objects must be present; the second object must not have
	// wiped the first.
	if sz, ok := sizeAt(book.Bids(), "100"); !ok || sz != "2" {
		t.Errorf("bid 100 from first object = %q (ok=%v), want 2", sz, ok)
	}
	if sz, ok := sizeAt(book.Asks(), "101"); !ok || sz != "3" {
		t.Errorf("ask 101 from first object = %q (ok=%v), want 3", sz, ok)
	}
	if sz, ok := sizeAt(book.Bids(), "98"); !ok || sz != "6" {
		t.Errorf("bid 98 from second object = %q (ok=%v), want 6", sz, ok)
	}
	if sz, ok := sizeAt(book.Asks(), "103"); !ok || sz != "5" {
		t.Errorf("ask 103 from second object = %q (ok=%v), want 5", sz, ok)
	}
	if book.LastUpdateID != 11 {
		t.Errorf("LastUpdateID = %d, want 11 (last object)", book.LastUpdateID)
	}
}

func TestOKXHandlerControlFrames(t *testing.T) {
	h := newOKXHandler()
	if sigs, err := h.handle([]byte("pong")); err != nil || sigs != nil {
		t.Errorf("pong: sigs=%v err=%v", sigs, err)
	}
	if sigs, err := h.handle([]byte(`{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"}}`)); err != nil || sigs != nil {
		t.Errorf("subscribe ack: sigs=%v err=%v", sigs, err)
	}
	if _, err := h.handle([]byte(`{"event":"error","code":"60012","msg":"bad request"}`)); err == nil {
		t.Error("error event should return an error")
	}
}

func TestOKXSubscribeMessageAndPing(t *testing.T) {
	e := &okxExchange{wsBase: okxWSBase}
	msgs, err := e.subscribeMessages([]string{"BTC-USDT", "ETH-USDT"})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("subscribeMessages = %v err %v", msgs, err)
	}
	m := msgs[0].(map[string]any)
	if m["op"] != "subscribe" {
		t.Errorf("op = %v", m["op"])
	}
	args := m["args"].([]map[string]string)
	if len(args) != 2 || args[0]["instId"] != "BTC-USDT" || args[0]["channel"] != "books" {
		t.Errorf("args = %v", args)
	}
	if frame, interval := e.appPing(); string(frame) != "ping" || interval <= 0 {
		t.Errorf("appPing = %q %v", frame, interval)
	}
}
