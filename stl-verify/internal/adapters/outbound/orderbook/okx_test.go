package orderbook

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func newOKXHandler() *okxHandler {
	return &okxHandler{
		books:   newBookSet(exchangeOKX),
		lastSeq: make(map[string]int64),
		allowed: symbolSet([]string{"BTC-USDT"}),
		logger:  testLogger(),
	}
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

// TestOKXHandlerNoUpdateSkipped covers OKX's documented no-update message: empty
// asks/bids with seqId == prevSeqId == the last applied seqId. It changes nothing
// and must emit no signal (and must not be re-emitted as a delta).
func TestOKXHandlerNoUpdateSkipped(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// No-update: empty book, seqId == prevSeqId == last (10).
	noUpdate := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[],"ts":"2","seqId":10,"prevSeqId":10}]}`
	sigs, err := h.handle([]byte(noUpdate))
	if err != nil {
		t.Fatalf("no-update: %v", err)
	}
	if len(sigs) != 0 {
		t.Fatalf("no-update must emit no signal, got %d", len(sigs))
	}
	// The next real update still chains off seqId 10 (the no-update left it untouched).
	update := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["99","7"]],"ts":"3","seqId":11,"prevSeqId":10}]}`
	sigs, err = h.handle([]byte(update))
	if err != nil {
		t.Fatalf("update after no-update should not gap: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("expected one signal for the chained update, got %+v", sigs)
	}
	if sz, ok := sizeAt(sigs[0].book.Bids(), "99"); !ok || sz != "7" {
		t.Errorf("bid 99 = %q (ok=%v), want 7", sz, ok)
	}
}

// TestOKXHandlerNonContiguousSeqIsGap: a seqId == prevSeqId that is NOT the last
// applied seqId is not a no-update (OKX's no-update repeats the current sequence,
// it never jumps forward). It does not chain, so it is a gap and forces a re-sync
// rather than silently advancing the sequence.
func TestOKXHandlerNonContiguousSeqIsGap(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	gap := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[],"ts":"2","seqId":20,"prevSeqId":20}]}`
	if _, err := h.handle([]byte(gap)); !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap (a non-contiguous seqId==prevSeqId must not advance)", err)
	}
}

// TestOKXHandlerSeqReset covers OKX's documented sequence reset (e.g. after
// maintenance): the reset update still chains (prevSeqId == the last applied
// seqId) but carries a smaller seqId. It must be applied, and subsequent
// updates chain from the smaller seqId.
func TestOKXHandlerSeqReset(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":15,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Reset: chains off 15, sequence rewinds to 3.
	reset := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["99","7"]],"ts":"2","seqId":3,"prevSeqId":15}]}`
	sigs, err := h.handle([]byte(reset))
	if err != nil {
		t.Fatalf("reset update must be applied, not treated as a gap: %v", err)
	}
	if len(sigs) != 1 || sigs[0].isSnapshot {
		t.Fatalf("expected one non-snapshot signal, got %+v", sigs)
	}
	if sz, ok := sizeAt(sigs[0].book.Bids(), "99"); !ok || sz != "7" {
		t.Errorf("bid 99 after reset = %q (ok=%v), want 7", sz, ok)
	}
	// The next update chains off the smaller seqId.
	next := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[
		{"asks":[],"bids":[["98","4"]],"ts":"3","seqId":5,"prevSeqId":3}]}`
	sigs, err = h.handle([]byte(next))
	if err != nil {
		t.Fatalf("update after reset should chain from the smaller seqId: %v", err)
	}
	if sz, ok := sizeAt(sigs[0].book.Bids(), "98"); !ok || sz != "4" {
		t.Errorf("bid 98 after reset chain = %q (ok=%v), want 4", sz, ok)
	}
}

// TestOKXHandlerMultiObjectSnapshot: a snapshot frame yields a single fully
// aggregated snapshot, never a partial book per data object. The book resets once
// and every object's levels survive.
func TestOKXHandlerMultiObjectSnapshot(t *testing.T) {
	h := newOKXHandler()
	snapshot := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1},
		{"asks":[["103","5"]],"bids":[["98","6"]],"ts":"2","seqId":11,"prevSeqId":-1}]}`
	sigs, err := h.handle([]byte(snapshot))
	if err != nil {
		t.Fatalf("multi-object snapshot: %v", err)
	}
	if len(sigs) != 1 || !sigs[0].isSnapshot {
		t.Fatalf("expected 1 aggregated snapshot signal, got %+v", sigs)
	}
	book := sigs[0].book
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
}

// TestOKXHandlerSubscriptionErrorKeepsConnection covers the poisoned-group bug:
// an error event (e.g. one nonexistent instId among 29 subscribed) is scoped to
// the offending arg; OKX keeps the connection and the other subscriptions alive.
// Treating it as connection-fatal would reconnect with the same subscribe payload
// and permanently cycle every healthy symbol in the group. The handler must
// surface it in the log and carry on.
func TestOKXHandlerSubscriptionErrorKeepsConnection(t *testing.T) {
	logger, sb := captureLogger()
	h := newOKXHandler()
	h.logger = logger
	frame := `{"event":"error","code":"60018","msg":"channel:books,instId:NOPE-USDT doesn't exist"}`
	sigs, err := h.handle([]byte(frame))
	if err != nil {
		t.Fatalf("subscription error must not kill the connection, got %v", err)
	}
	if sigs != nil {
		t.Errorf("error event must emit no book change, got %+v", sigs)
	}
	// Swallowing the error silently is not acceptable; it must be loud in the log.
	if got := sb.String(); !strings.Contains(got, "ERROR") || !strings.Contains(got, "60018") {
		t.Errorf("subscription error must be logged at error level with the venue code, got %q", got)
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
}

func TestOKXHandlerRejectsUnsubscribedSymbol(t *testing.T) {
	// Build through the exchange so newHandler's symbolSet seeding is covered too.
	h := (&okxExchange{}).newHandler([]string{"BTC-USDT"}, testLogger())
	// A snapshot for an instId we never subscribed to must be rejected.
	frame := `{"arg":{"channel":"books","instId":"ETH-USDT"},"action":"snapshot","data":[
		{"asks":[["101","3"]],"bids":[["100","2"]],"ts":"1","seqId":10,"prevSeqId":-1}]}`
	if _, err := h.handle([]byte(frame)); !errors.Is(err, errUnexpectedSymbol) {
		t.Fatalf("err = %v, want errUnexpectedSymbol", err)
	}
}

func TestOKXHandlerRejectsMalformedLevel(t *testing.T) {
	// A non-canonical price/size must fail the frame so the feed resyncs.
	tests := []struct{ name, frame string }{
		{"exponent price", `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[],"bids":[["1e5","1"]],"seqId":1,"prevSeqId":-1}]}`},
		{"negative size", `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["101","-1"]],"bids":[],"seqId":1,"prevSeqId":-1}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newOKXHandler()
			if _, err := h.handle([]byte(tt.frame)); err == nil {
				t.Error("expected an error for a malformed level (forces resync)")
			}
		})
	}
}

func TestOKXHandlerMalformedFrame(t *testing.T) {
	h := newOKXHandler()
	if _, err := h.handle([]byte("{not json")); err == nil {
		t.Fatal("malformed frame must error so the feed resyncs")
	}
}

func TestOKXHandlerIgnoresDatalessFrame(t *testing.T) {
	h := newOKXHandler()
	// A data frame carrying no objects produces no change and no error.
	if sigs, err := h.handle([]byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[]}`)); err != nil || sigs != nil {
		t.Errorf("empty-data frame: sigs=%v err=%v, want nil/nil", sigs, err)
	}
}

func TestOKXSubscribeMessageAndPing(t *testing.T) {
	e := &okxExchange{}
	msgs := e.subscribeMessages([]string{"BTC-USDT", "ETH-USDT"})
	if len(msgs) != 1 {
		t.Fatalf("subscribeMessages = %v", msgs)
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

func TestOKXMaxSymbolsRespectsVenueLimit(t *testing.T) {
	// OKX's order book channel docs recommend fewer than 30 channels per connection.
	if okxMaxSymbols >= 30 {
		t.Errorf("okxMaxSymbols = %d, want < 30 (OKX order book channel recommendation)", okxMaxSymbols)
	}
	// A symbol set larger than the cap splits into multiple sub-limit groups.
	symbols := make([]string, 30)
	for i := range symbols {
		symbols[i] = "S" + itoa(int64(i)) + "-USDT"
	}
	groups := chunkSymbols(symbols, okxMaxSymbols)
	if len(groups) < 2 {
		t.Fatalf("30 symbols at cap %d should split into >=2 groups, got %d", okxMaxSymbols, len(groups))
	}
	for _, g := range groups {
		if len(g) >= 30 {
			t.Errorf("connection group has %d channels, want < 30", len(g))
		}
	}
}

func TestOKXProviderNameAndValidation(t *testing.T) {
	p := NewOKXProvider(testConfig())
	if p.Name() != "okx" {
		t.Errorf("Name = %q", p.Name())
	}
	if _, err := p.Watch(context.Background(), nil); err == nil {
		t.Error("Watch with no symbols should error")
	}
	if _, err := p.Watch(context.Background(), []string{""}); err == nil {
		t.Error("Watch with an empty symbol should error")
	}
}

func TestParseUnixMillisOrZero(t *testing.T) {
	if got := parseUnixMillisOrZero("1700000000000"); !got.Equal(time.UnixMilli(1700000000000)) {
		t.Errorf("parseUnixMillisOrZero = %v", got)
	}
	if got := parseUnixMillisOrZero("1700000000000"); got.Location() != time.UTC {
		t.Errorf("parseUnixMillisOrZero location = %v, want UTC", got.Location())
	}
	// Malformed input returns the zero Time, never the local clock.
	if got := parseUnixMillisOrZero("nope"); !got.IsZero() {
		t.Errorf("parseUnixMillisOrZero(bad) = %v, want zero", got)
	}
}
