package orderbook

import (
	"context"
	"errors"
	"testing"
)

func TestCoinbaseHandlerSnapshotThenUpdate(t *testing.T) {
	h := newCoinbaseHandler(nil)

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
	h := newCoinbaseHandler(nil)
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
	h := newCoinbaseHandler(nil)
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
	h := newCoinbaseHandler(nil)
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
	h := newCoinbaseHandler(nil)
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
	e := &coinbaseExchange{wsBase: coinbaseWSBase}
	msgs, err := e.subscribeMessages([]string{"BTC-USD"})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("subscribeMessages = %v, err %v", msgs, err)
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
}
