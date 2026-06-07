package orderbook

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/gorilla/websocket"
)

// fakeExchange is a minimal wsSnapshotExchange for engine tests. Frames are
// {"symbol","snapshot","price","size"} JSON objects applied to the bid side.
type fakeExchange struct{ url string }

func (e *fakeExchange) name() string             { return "fake" }
func (e *fakeExchange) endpoint([]string) string { return e.url }

// normalizeSymbol is permissive: it only upper-cases, so the engine test's "X"
// symbol still passes.
func (e *fakeExchange) normalizeSymbol(s string) (string, error) {
	return strings.ToUpper(s), nil
}
func (e *fakeExchange) subscribeMessages(group []string) ([]any, error) {
	return []any{map[string]any{"sub": group}}, nil
}
func (e *fakeExchange) newHandler() frameHandler { return &fakeHandler{books: newBookSet("fake")} }

// appPing exercises the engine's application-level keepalive path. The server's
// keepOpen loop reads and discards these frames, so they never reach the handler.
func (e *fakeExchange) appPing() ([]byte, time.Duration) {
	return []byte(`{"event":"fakeping"}`), 5 * time.Millisecond
}

type fakeHandler struct{ books *bookSet }

func (h *fakeHandler) handle(raw []byte) ([]emitSignal, error) {
	var m struct {
		Symbol   string      `json:"symbol"`
		Snapshot bool        `json:"snapshot"`
		Price    json.Number `json:"price"`
		Size     json.Number `json:"size"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	book := h.books.get(m.Symbol)
	if m.Snapshot {
		book.Reset()
	}
	book.ApplyLevel(entity.Bid, m.Price.String(), m.Size.String())
	return []emitSignal{{book: book, isSnapshot: m.Snapshot, t: time.Now()}}, nil
}

func TestWSSnapshotProviderStreamsAndCloses(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`)
		sendText(t, conn, `{"symbol":"X","snapshot":false,"price":101,"size":2}`)
		keepOpen(conn)
	})

	p := newWSSnapshotProvider(testConfig(), &fakeExchange{url: srv.url}, 10)
	if p.Name() != "fake" {
		t.Errorf("Name = %q", p.Name())
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Watch(ctx, []string{"X"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	u1 := receiveWithin(t, ch, 2*time.Second)
	if !u1.IsSnapshot {
		t.Error("first update should be a snapshot")
	}
	if sz, ok := sizeAt(u1.Book.Bids(), "100"); !ok || sz != "1" {
		t.Errorf("snapshot bid 100 = %q (ok=%v), want 1", sz, ok)
	}

	u2 := receiveWithin(t, ch, 2*time.Second)
	if u2.IsSnapshot {
		t.Error("second update should not be a snapshot")
	}
	if sz, ok := sizeAt(u2.Book.Bids(), "101"); !ok || sz != "2" {
		t.Errorf("update bid 101 = %q (ok=%v), want 2", sz, ok)
	}

	// Cancellation must drain connections and close the channel.
	cancel()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed: success
			}
		case <-time.After(2 * time.Second):
			t.Fatal("channel not closed after cancellation")
		}
	}
}

// readyRecorder counts ready() invocations from any goroutine. ready resets the
// reconnect backoff, so it must only fire once a book has actually synced.
type readyRecorder struct {
	mu    sync.Mutex
	calls int
}

func (r *readyRecorder) ready() {
	r.mu.Lock()
	r.calls++
	r.mu.Unlock()
}

func (r *readyRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// TestWSSnapshotReadyNotCalledWithoutBook covers Issue 2 for the WS-snapshot
// engine: a connection that subscribes successfully but only ever receives an
// error frame (and never a book) must NOT reset the reconnect backoff.
func TestWSSnapshotReadyNotCalledWithoutBook(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		// Subscribe succeeds, then the venue sends a frame the handler rejects;
		// no book is ever produced.
		sendText(t, conn, `not-json`)
		keepOpen(conn)
	})

	p := newWSSnapshotProvider(testConfig(), &fakeExchange{url: srv.url}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &readyRecorder{}
	out := make(chan entity.OrderbookUpdate, 4)
	done := make(chan error, 1)
	go func() { done <- p.runConnection(ctx, []string{"X"}, out, rec.ready) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runConnection did not return after the handler error")
	}
	if rec.count() != 0 {
		t.Errorf("ready called %d times, want 0 (no book was ever synced)", rec.count())
	}
}

// TestWSSnapshotReadyCalledOnceOnFirstBook covers the surrounding correct
// behaviour for Issue 2: ready fires exactly once, when the first book is
// emitted, and not again for subsequent updates on the same connection.
func TestWSSnapshotReadyCalledOnceOnFirstBook(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`)
		sendText(t, conn, `{"symbol":"X","snapshot":false,"price":101,"size":2}`)
		keepOpen(conn)
	})

	p := newWSSnapshotProvider(testConfig(), &fakeExchange{url: srv.url}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &readyRecorder{}
	out := make(chan entity.OrderbookUpdate, 4)
	go func() { _ = p.runConnection(ctx, []string{"X"}, out, rec.ready) }()

	// Wait for both updates so we know the connection went past the first book.
	receiveWithin(t, out, 2*time.Second)
	receiveWithin(t, out, 2*time.Second)
	cancel()

	if got := rec.count(); got != 1 {
		t.Errorf("ready called %d times, want exactly 1 (on first book only)", got)
	}
}

func receiveWithin(t *testing.T, ch <-chan entity.OrderbookUpdate, d time.Duration) entity.OrderbookUpdate {
	t.Helper()
	select {
	case upd, ok := <-ch:
		if !ok {
			t.Fatal("channel closed unexpectedly")
		}
		return upd
	case <-time.After(d):
		t.Fatal("timed out waiting for update")
		return entity.OrderbookUpdate{}
	}
}
