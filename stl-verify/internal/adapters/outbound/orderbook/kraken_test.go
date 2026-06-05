package orderbook

import (
	"testing"
)

func TestKrakenHandlerSnapshotThenUpdate(t *testing.T) {
	h := &krakenHandler{books: newBookSet(exchangeKraken)}

	// Snapshot frame: [channelID, {as,bs}, channelName, pair].
	snapshot := `[0,{"as":[["101.0","3.0","t"],["102.0","1.0","t"]],"bs":[["100.0","2.0","t"],["99.0","4.0","t"]]},"book-500","XBT/USD"]`
	sigs, err := h.handle([]byte(snapshot))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(sigs) != 1 || !sigs[0].isSnapshot {
		t.Fatalf("expected snapshot signal, got %+v", sigs)
	}
	book := sigs[0].book
	if bb, _ := book.BestBid(); bb.Price != 100 || bb.Size != 2 {
		t.Errorf("best bid = %+v, want {100 2}", bb)
	}
	if ba, _ := book.BestAsk(); ba.Price != 101 || ba.Size != 3 {
		t.Errorf("best ask = %+v, want {101 3}", ba)
	}

	// Update frame: [channelID, {b}, channelName, pair]; zero volume removes.
	update := `[0,{"b":[["100.0","0.00000000","t"],["98.5","5.0","t"]],"c":"1234"},"book-500","XBT/USD"]`
	sigs, err = h.handle([]byte(update))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(sigs) != 1 || sigs[0].isSnapshot {
		t.Fatalf("expected non-snapshot signal, got %+v", sigs)
	}
	book = sigs[0].book
	if _, ok := sizeAt(book.Bids(), 100); ok {
		t.Error("bid 100 should be removed by zero volume")
	}
	if bb, _ := book.BestBid(); bb.Price != 99 || bb.Size != 4 {
		t.Errorf("best bid after update = %+v, want {99 4}", bb)
	}
	if sz, ok := sizeAt(book.Bids(), 98.5); !ok || sz != 5 {
		t.Errorf("bid 98.5 = %v (ok=%v), want 5", sz, ok)
	}
}

func TestKrakenHandlerCombinedUpdate(t *testing.T) {
	h := &krakenHandler{books: newBookSet(exchangeKraken)}
	snapshot := `[0,{"as":[["101","3","t"]],"bs":[["100","2","t"]]},"book-500","XBT/USD"]`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Two data objects (a and b) in one frame.
	combined := `[0,{"a":[["101","0","t"],["103","1","t"]]},{"b":[["100","9","t"]]},"book-500","XBT/USD"]`
	sigs, err := h.handle([]byte(combined))
	if err != nil {
		t.Fatalf("combined: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(sigs))
	}
	book := sigs[0].book
	if _, ok := sizeAt(book.Asks(), 101); ok {
		t.Error("ask 101 should be removed")
	}
	if ba, _ := book.BestAsk(); ba.Price != 103 || ba.Size != 1 {
		t.Errorf("best ask = %+v, want {103 1}", ba)
	}
	if bb, _ := book.BestBid(); bb.Price != 100 || bb.Size != 9 {
		t.Errorf("best bid = %+v, want {100 9}", bb)
	}
}

func TestKrakenHandlerEvents(t *testing.T) {
	h := &krakenHandler{books: newBookSet(exchangeKraken)}
	for _, raw := range []string{
		`{"event":"systemStatus","status":"online"}`,
		`{"event":"heartbeat"}`,
		`{"event":"pong"}`,
		`{"event":"subscriptionStatus","status":"subscribed","pair":"XBT/USD"}`,
	} {
		if sigs, err := h.handle([]byte(raw)); err != nil || sigs != nil {
			t.Errorf("event %s: sigs=%v err=%v", raw, sigs, err)
		}
	}
	if _, err := h.handle([]byte(`{"event":"subscriptionStatus","status":"error","pair":"XBT/USD","errorMessage":"bad pair"}`)); err == nil {
		t.Error("subscription error should return an error")
	}
}

func TestKrakenHandlerMalformedDataIsFatal(t *testing.T) {
	h := &krakenHandler{books: newBookSet(exchangeKraken)}
	// A non-object where a data object is expected must error (forcing reconnect),
	// since Kraken v1 has no sequence backstop to detect a silently dropped frame.
	frame := `[0,"not-an-object","book-500","XBT/USD"]`
	if _, err := h.handle([]byte(frame)); err == nil {
		t.Fatal("expected an error for a malformed data object, got nil")
	}
}

func TestKrakenSubscribeAndPing(t *testing.T) {
	e := &krakenExchange{wsBase: krakenWSBase}
	msgs, err := e.subscribeMessages([]string{"XBT/USD"})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("subscribeMessages = %v err %v", msgs, err)
	}
	m := msgs[0].(map[string]any)
	if m["event"] != "subscribe" {
		t.Errorf("event = %v", m["event"])
	}
	sub := m["subscription"].(map[string]any)
	if sub["name"] != "book" || sub["depth"] != krakenDepth {
		t.Errorf("subscription = %v", sub)
	}
	if frame, interval := e.appPing(); string(frame) != `{"event":"ping"}` || interval <= 0 {
		t.Errorf("appPing = %q %v", frame, interval)
	}
}
