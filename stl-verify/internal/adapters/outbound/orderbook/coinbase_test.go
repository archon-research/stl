package orderbook

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// newTestCoinbaseHandler builds a handler subscribed to the products the tests
// exercise, with a quiet logger.
func newTestCoinbaseHandler() *coinbaseHandler {
	return (&coinbaseExchange{}).newHandler([]string{"BTC-USD", "ETH-USD"}, testLogger()).(*coinbaseHandler)
}

func TestCoinbaseHandlerSnapshotThenUpdate(t *testing.T) {
	h := newTestCoinbaseHandler()

	snapshot := `{"channel":"l2_data","sequence_num":0,"timestamp":"2023-02-09T20:32:50Z","events":[
		{"type":"snapshot","product_id":"BTC-USD","updates":[
			{"side":"bid","price_level":"100","new_quantity":"2"},
			{"side":"offer","price_level":"101","new_quantity":"3"}]}]}`
	sigs, err := h.handle([]byte(snapshot))
	if err != nil {
		t.Fatalf("snapshot handle: %v", err)
	}
	if len(sigs) != 1 || !sigs[0].isSnapshot {
		t.Fatalf("expected 1 snapshot signal, got %+v", sigs)
	}
	book := sigs[0].book
	if sz, ok := sizeAt(book.Bids(), "100"); !ok || sz != "2" {
		t.Errorf("bid 100 = %q (ok=%v), want 2", sz, ok)
	}
	if sz, ok := sizeAt(book.Asks(), "101"); !ok || sz != "3" {
		t.Errorf("ask 101 = %q (ok=%v), want 3", sz, ok)
	}

	update := `{"channel":"l2_data","sequence_num":1,"timestamp":"2023-02-09T20:32:51Z","events":[
		{"type":"update","product_id":"BTC-USD","updates":[
			{"side":"bid","price_level":"100","new_quantity":"0"},
			{"side":"bid","price_level":"99","new_quantity":"5"}]}]}`
	sigs, err = h.handle([]byte(update))
	if err != nil {
		t.Fatalf("update handle: %v", err)
	}
	if len(sigs) != 1 || sigs[0].isSnapshot {
		t.Fatalf("expected 1 non-snapshot signal, got %+v", sigs)
	}
	book = sigs[0].book
	if _, ok := sizeAt(book.Bids(), "100"); ok {
		t.Error("bid 100 should be removed by zero quantity")
	}
	if sz, ok := sizeAt(book.Bids(), "99"); !ok || sz != "5" {
		t.Errorf("bid 99 after update = %q (ok=%v), want 5", sz, ok)
	}
}

func TestCoinbaseHandlerSequenceGap(t *testing.T) {
	h := newTestCoinbaseHandler()
	first := `{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[]}]}`
	if _, err := h.handle([]byte(first)); err != nil {
		t.Fatalf("first handle: %v", err)
	}
	// Expected sequence_num 1, but receive 5.
	gap := `{"channel":"l2_data","sequence_num":5,"events":[{"type":"update","product_id":"BTC-USD","updates":[]}]}`
	_, err := h.handle([]byte(gap))
	if !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap", err)
	}
}

func TestCoinbaseHandlerUpdateBeforeSnapshotIsRejected(t *testing.T) {
	h := newTestCoinbaseHandler()
	// An update for a product with no prior snapshot must not be applied to an
	// empty book; it must force a re-sync.
	update := `{"channel":"l2_data","sequence_num":0,"events":[
		{"type":"update","product_id":"BTC-USD","updates":[
			{"side":"bid","price_level":"100","new_quantity":"1"}]}]}`
	if _, err := h.handle([]byte(update)); !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap", err)
	}
}

func TestCoinbaseHandlerIgnoresControlFrames(t *testing.T) {
	h := newTestCoinbaseHandler()
	for _, raw := range []string{
		`{"channel":"subscriptions","sequence_num":0,"events":[]}`,
		`{"channel":"heartbeats","sequence_num":1}`,
	} {
		sigs, err := h.handle([]byte(raw))
		if err != nil || sigs != nil {
			t.Errorf("control frame %s: sigs=%v err=%v", raw, sigs, err)
		}
	}
	if _, err := h.handle([]byte(`{"channel":"error","sequence_num":2,"message":"boom"}`)); err == nil {
		t.Error("error frame should return an error")
	}
}

// TestCoinbaseHandlerAdvancesSequenceAcrossChannels guards the e2e-found bug:
// sequence_num is connection-wide, so a non-l2_data frame between two l2_data
// frames consumes a sequence number and must not be read as a gap.
func TestCoinbaseHandlerAdvancesSequenceAcrossChannels(t *testing.T) {
	h := newTestCoinbaseHandler()
	snap := `{"channel":"l2_data","sequence_num":0,"events":[
		{"type":"snapshot","product_id":"BTC-USD","updates":[
			{"side":"bid","price_level":"100","new_quantity":"1"}]}]}`
	if _, err := h.handle([]byte(snap)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// A heartbeat at seq 1 consumes a sequence number (not l2_data).
	if _, err := h.handle([]byte(`{"channel":"heartbeats","sequence_num":1}`)); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	// The next l2_data is seq 2; it must not be treated as a gap.
	upd := `{"channel":"l2_data","sequence_num":2,"events":[
		{"type":"update","product_id":"BTC-USD","updates":[
			{"side":"bid","price_level":"99","new_quantity":"2"}]}]}`
	if _, err := h.handle([]byte(upd)); err != nil {
		t.Fatalf("update after interleaved heartbeat should not gap: %v", err)
	}
}

func TestCoinbaseSubscribeMessage(t *testing.T) {
	e := &coinbaseExchange{}
	msgs := e.subscribeMessages([]string{"BTC-USD"})
	if len(msgs) != 1 {
		t.Fatalf("subscribeMessages = %v", msgs)
	}
	m := msgs[0].(map[string]any)
	if m["channel"] != "level2" || m["type"] != "subscribe" {
		t.Errorf("subscribe message = %v", m)
	}
	if _, ok := m["jwt"]; ok {
		t.Error("unauthenticated subscribe should not carry a jwt")
	}
}

func TestCoinbaseProviderNameAndValidation(t *testing.T) {
	p := NewCoinbaseProvider(testConfig())
	if p.Name() != "coinbase" {
		t.Errorf("Name = %q", p.Name())
	}
	if _, err := p.Watch(context.Background(), nil); err == nil {
		t.Error("Watch with no symbols should error")
	}
	if _, err := p.Watch(context.Background(), []string{"NOSEP"}); err == nil {
		t.Error("Watch with a malformed symbol should error")
	}
}

func TestParseRFC3339OrZero(t *testing.T) {
	want := time.Date(2023, 2, 9, 20, 32, 50, 0, time.UTC)
	if got := parseRFC3339OrZero("2023-02-09T20:32:50Z"); !got.Equal(want) {
		t.Errorf("parseRFC3339OrZero = %v, want %v", got, want)
	}
	// Malformed or absent input returns the zero Time, never the local clock.
	if got := parseRFC3339OrZero(""); !got.IsZero() {
		t.Errorf("parseRFC3339OrZero(empty) = %v, want zero", got)
	}
	if got := parseRFC3339OrZero("not-a-time"); !got.IsZero() {
		t.Errorf("parseRFC3339OrZero(bad) = %v, want zero", got)
	}
}

func TestCoinbaseSide(t *testing.T) {
	if s, err := coinbaseSide("bid"); err != nil || s != entity.Bid {
		t.Errorf("coinbaseSide(bid) = %v, %v", s, err)
	}
	if s, err := coinbaseSide("offer"); err != nil || s != entity.Ask {
		t.Errorf("coinbaseSide(offer) = %v, %v", s, err)
	}
	// Tokens the documented feed never sends must error so a protocol change is
	// loud rather than silently mis-classified.
	for _, bad := range []string{"buy", "sell", "ask", "weird", ""} {
		if _, err := coinbaseSide(bad); err == nil {
			t.Errorf("coinbaseSide(%q) should error", bad)
		}
	}
}

func TestCoinbaseHandlerRejectsUnsubscribedSymbol(t *testing.T) {
	// Build through the exchange so newHandler's symbolSet seeding is covered too.
	h := (&coinbaseExchange{}).newHandler([]string{"BTC-USD"}, testLogger())
	// A snapshot for a product we never subscribed to is a protocol surprise; the
	// handler must reject it so the engine reconnects rather than emit a phantom book.
	frame := `{"channel":"l2_data","sequence_num":0,"events":[
		{"type":"snapshot","product_id":"ETH-USD","updates":[
			{"side":"bid","price_level":"100","new_quantity":"1"}]}]}`
	if _, err := h.handle([]byte(frame)); !errors.Is(err, errUnexpectedSymbol) {
		t.Fatalf("err = %v, want errUnexpectedSymbol", err)
	}
}

func TestCoinbaseHandlerCoalescesEventsPerProduct(t *testing.T) {
	h := newTestCoinbaseHandler()
	// One frame, two events for the same product (snapshot then update): they must
	// coalesce into a single change flagged isSnapshot, carrying both levels.
	frame := `{"channel":"l2_data","sequence_num":0,"events":[
		{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"1"}]},
		{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"99","new_quantity":"2"}]}]}`
	sigs, err := h.handle([]byte(frame))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(sigs) != 1 || !sigs[0].isSnapshot {
		t.Fatalf("want 1 coalesced snapshot change, got %+v", sigs)
	}
	if sz, ok := sizeAt(sigs[0].book.Bids(), "100"); !ok || sz != "1" {
		t.Errorf("bid 100 = %q (ok=%v), want 1", sz, ok)
	}
	if sz, ok := sizeAt(sigs[0].book.Bids(), "99"); !ok || sz != "2" {
		t.Errorf("bid 99 = %q (ok=%v), want 2", sz, ok)
	}
}

func TestCoinbaseHandlerPreservesProductOrder(t *testing.T) {
	h := newTestCoinbaseHandler()
	// Distinct products in one frame yield one change each, in first-seen order.
	frame := `{"channel":"l2_data","sequence_num":0,"events":[
		{"type":"snapshot","product_id":"ETH-USD","updates":[]},
		{"type":"snapshot","product_id":"BTC-USD","updates":[]}]}`
	sigs, err := h.handle([]byte(frame))
	if err != nil {
		t.Fatalf("handle: %v", err)
	}
	if len(sigs) != 2 || sigs[0].book.Symbol != "ETH-USD" || sigs[1].book.Symbol != "BTC-USD" {
		t.Fatalf("want [ETH-USD, BTC-USD] in order, got %+v", sigs)
	}
}

func TestCoinbaseHandlerWarnsOnceForUnknownChannel(t *testing.T) {
	logger, sb := captureLogger()
	h := (&coinbaseExchange{}).newHandler([]string{"BTC-USD"}, logger).(*coinbaseHandler)
	// Three frames on an unrecognized channel: each is a no-op, but the warning is
	// emitted only once per connection.
	for _, raw := range []string{
		`{"channel":"mystery","sequence_num":0}`,
		`{"channel":"mystery","sequence_num":1}`,
		`{"channel":"mystery","sequence_num":2}`,
	} {
		if sigs, err := h.handle([]byte(raw)); err != nil || sigs != nil {
			t.Fatalf("unknown channel frame: sigs=%v err=%v", sigs, err)
		}
	}
	if n := strings.Count(sb.String(), "unrecognized channel"); n != 1 {
		t.Errorf("warning emitted %d times, want exactly 1 (deduped per connection)", n)
	}
}

func TestCoinbaseHandlerRejectsMalformedLevel(t *testing.T) {
	// A non-canonical price/size must fail the frame so the engine resyncs, rather
	// than be stored verbatim and strand a ghost level.
	tests := []struct{ name, frame string }{
		{"exponent price", `{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"1e5","new_quantity":"1"}]}]}`},
		{"negative size", `{"channel":"l2_data","sequence_num":0,"events":[{"type":"snapshot","product_id":"BTC-USD","updates":[{"side":"bid","price_level":"100","new_quantity":"-1"}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestCoinbaseHandler()
			if _, err := h.handle([]byte(tt.frame)); err == nil {
				t.Error("expected an error for a malformed level (forces resync)")
			}
		})
	}
}
