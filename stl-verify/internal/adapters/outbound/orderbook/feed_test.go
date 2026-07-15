package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/wsclient"
	"github.com/gorilla/websocket"
)

// fakeExchange is a minimal exchangeFeed for feed tests. Frames are
// {"symbol","snapshot","price","size"} JSON objects applied to the bid side.
type fakeExchange struct {
	url string

	mu     sync.Mutex
	groups [][]string // symbol groups passed to subscribeMessages
}

func (e *fakeExchange) name() string     { return "fake" }
func (e *fakeExchange) endpoint() string { return e.url }

// normalizeSymbol is permissive: it only upper-cases, so the feed test's "X"
// symbol still passes.
func (e *fakeExchange) normalizeSymbol(s string) (string, error) {
	return strings.ToUpper(s), nil
}
func (e *fakeExchange) subscribeMessages(group []string) []any {
	e.mu.Lock()
	e.groups = append(e.groups, group)
	e.mu.Unlock()
	return []any{map[string]any{"sub": group}}
}

// subscribedGroups returns a copy of the symbol groups subscribed so far.
func (e *fakeExchange) subscribedGroups() [][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([][]string, len(e.groups))
	copy(out, e.groups)
	return out
}

func (e *fakeExchange) newHandler([]string, *slog.Logger) frameHandler {
	return &fakeHandler{books: newBookSet("fake")}
}

// appPing exercises the feed's application-level keepalive path. The server's
// keepOpen loop reads and discards these frames, so they never reach the handler.
func (e *fakeExchange) appPing() ([]byte, time.Duration) {
	return []byte(`{"event":"fakeping"}`), 5 * time.Millisecond
}

type fakeHandler struct{ books *bookSet }

func (h *fakeHandler) handle(raw []byte) ([]bookChange, error) {
	var m struct {
		Symbol   string      `json:"symbol"`
		Snapshot bool        `json:"snapshot"`
		Price    json.Number `json:"price"`
		Size     json.Number `json:"size"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	book := h.books.getOrCreate(m.Symbol)
	if m.Snapshot {
		book.Reset()
	}
	book.ApplyLevel(entity.Bid, m.Price.String(), m.Size.String())
	return []bookChange{{book: book, isSnapshot: m.Snapshot, t: time.Now()}}, nil
}

func TestFeedProviderStreamsAndCloses(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`)
		sendText(t, conn, `{"symbol":"X","snapshot":false,"price":101,"size":2}`)
		keepOpen(conn)
	})

	p := newTestFeedProvider(t, testConfig(), &fakeExchange{url: srv.url}, 10)
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

// TestFeedProviderReadyNotCalledWithoutBook: a connection that subscribes
// successfully but never produces a book (here the venue sends only a frame the
// handler rejects) must NOT reset the reconnect backoff.
func TestFeedProviderReadyNotCalledWithoutBook(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		// Subscribe succeeds, then the venue sends a frame the handler rejects;
		// no book is ever produced.
		sendText(t, conn, `not-json`)
		keepOpen(conn)
	})

	p := newTestFeedProvider(t, testConfig(), &fakeExchange{url: srv.url}, 10)
	ctx := t.Context()

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

// TestFeedProviderReadyCalledOnceOnFirstBook: the backoff reset fires exactly
// once, when the connection's books first sync, and not again for subsequent
// updates on the same connection.
func TestFeedProviderReadyCalledOnceOnFirstBook(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`)
		sendText(t, conn, `{"symbol":"X","snapshot":false,"price":101,"size":2}`)
		keepOpen(conn)
	})

	p := newTestFeedProvider(t, testConfig(), &fakeExchange{url: srv.url}, 10)
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

// TestFeedProviderReadyNotCalledOnPartialGroupSnapshot: with several symbols on
// one connection, a snapshot from only some of them must NOT reset the backoff,
// so a never-syncing symbol can't be masked into a reconnect storm.
func TestFeedProviderReadyNotCalledOnPartialGroupSnapshot(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`) // only X, never Y
		keepOpen(conn)
	})
	p := newTestFeedProvider(t, testConfig(), &fakeExchange{url: srv.url}, 10)
	ctx := t.Context()

	rec := &readyRecorder{}
	out := make(chan entity.OrderbookUpdate, 4)
	go func() { _ = p.runConnection(ctx, []string{"X", "Y"}, out, rec.ready) }()

	receiveWithin(t, out, 2*time.Second) // X's snapshot delivered
	if got := rec.count(); got != 0 {
		t.Errorf("ready called %d times after a partial group snapshot, want 0", got)
	}
}

// TestFeedProviderReadyCalledOnceAfterFullGroupSnapshot: ready fires exactly once,
// after every symbol in the group has snapshotted.
func TestFeedProviderReadyCalledOnceAfterFullGroupSnapshot(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`)
		sendText(t, conn, `{"symbol":"Y","snapshot":true,"price":200,"size":1}`)
		keepOpen(conn)
	})
	p := newTestFeedProvider(t, testConfig(), &fakeExchange{url: srv.url}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &readyRecorder{}
	out := make(chan entity.OrderbookUpdate, 4)
	go func() { _ = p.runConnection(ctx, []string{"X", "Y"}, out, rec.ready) }()

	receiveWithin(t, out, 2*time.Second)
	receiveWithin(t, out, 2*time.Second)
	// ready() is invoked right after the second snapshot is emitted to `out`, so
	// it can lag the receive above — wait for it rather than racing cancel()
	// against it (under -race the test goroutine otherwise wins and sees 0).
	deadline := time.Now().Add(2 * time.Second)
	for rec.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if got := rec.count(); got != 1 {
		t.Errorf("ready called %d times, want exactly 1 (after the full group synced)", got)
	}
}

// TestWatchDedupsNormalizedSymbols: two spellings of one symbol must subscribe
// once, not twice on the same connection (the venue's reaction to a duplicate
// subscription is undefined, and a doubled book stream is never wanted).
func TestWatchDedupsNormalizedSymbols(t *testing.T) {
	srv := newWSTestServer(t, keepOpen)
	ex := &fakeExchange{url: srv.url}
	p := newTestFeedProvider(t, testConfig(), ex, 10)
	ctx := t.Context()

	if _, err := p.Watch(ctx, []string{"btc-usd", "BTC-USD"}); err != nil {
		t.Fatalf("Watch: %v", err)
	}

	deadline := time.After(2 * time.Second)
	for len(ex.subscribedGroups()) == 0 {
		select {
		case <-deadline:
			t.Fatal("no subscribe observed")
		case <-time.After(2 * time.Millisecond):
		}
	}
	groups := ex.subscribedGroups()
	if len(groups) != 1 || len(groups[0]) != 1 || groups[0][0] != "BTC-USD" {
		t.Fatalf("subscribed groups = %v, want one group with the single deduped symbol BTC-USD", groups)
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

// TestStartAppPingTearsDownOnWriteFailure covers the keepalive's failure arm:
// when the app-level ping can no longer be written, the loop must warn and close
// the connection (so it reconnects) rather than idle until the read timeout.
func TestStartAppPingTearsDownOnWriteFailure(t *testing.T) {
	logger, sb := captureLogger()
	cfg := testConfig()
	cfg.Logger = logger
	ex := &fakeExchange{}
	p := newTestFeedProvider(t, cfg, ex, 10)

	srv := newWSTestServer(t, keepOpen)
	ws, err := wsclient.Dial(context.Background(), srv.url, p.cfg.Config)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	ws.Close() // any subsequent app-ping write now fails

	p.startAppPing(context.Background(), ws, ex)

	deadline := time.After(2 * time.Second)
	for !strings.Contains(sb.String(), "app ping failed") {
		select {
		case <-deadline:
			t.Fatal("expected an 'app ping failed' warning after the write failure")
		case <-time.After(2 * time.Millisecond):
		}
	}
}

type zeroPinger struct{}

func (zeroPinger) appPing() ([]byte, time.Duration) { return nil, 0 }

// TestStartAppPingNoOpOnNonPositiveInterval: a pinger returning a non-positive
// interval must disable the keepalive, not panic time.NewTicker(0). nil ws is
// safe because the guard returns before touching the connection; without it,
// time.NewTicker(0) would panic.
func TestStartAppPingNoOpOnNonPositiveInterval(t *testing.T) {
	p := newTestFeedProvider(t, testConfig(), &fakeExchange{}, 10)
	p.startAppPing(context.Background(), nil, zeroPinger{})
}

// TestFeedProviderNoGoroutineLeakAfterShutdown drives repeated reconnects (each
// spawning a read pump and app-ping goroutine) and asserts that, once Watch is
// cancelled and the channel drains, the goroutine count returns to baseline — the
// per-connection context must stop every helper goroutine, with no accumulation.
func TestFeedProviderNoGoroutineLeakAfterShutdown(t *testing.T) {
	srv := newWSTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"symbol":"X","snapshot":true,"price":100,"size":1}`)
		// Return immediately to drop the connection and force a reconnect.
	})

	time.Sleep(100 * time.Millisecond) // let earlier tests' goroutines settle
	base := runtime.NumGoroutine()

	p := newTestFeedProvider(t, testConfig(), &fakeExchange{url: srv.url}, 10)
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := p.Watch(ctx, []string{"X"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	receiveWithin(t, ch, 2*time.Second) // first book up (connection + ping goroutine live)
	time.Sleep(60 * time.Millisecond)   // allow several reconnect cycles
	cancel()
	for range ch { //nolint:revive // drain until the aggregator closes the channel
	}

	if !waitForGoroutines(base+2, 2*time.Second) {
		t.Fatalf("goroutines did not return to baseline after shutdown: got %d, base %d",
			runtime.NumGoroutine(), base)
	}
}

// waitForGoroutines polls until the goroutine count is at or below max, or the
// timeout elapses.
func waitForGoroutines(max int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= max {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return runtime.NumGoroutine() <= max
}

func TestRunConnectionReturnsDialError(t *testing.T) {
	// An unroutable endpoint: Dial fails and runConnection surfaces the error.
	p := newTestFeedProvider(t, testConfig(), &fakeExchange{url: "ws://127.0.0.1:1"}, 10)
	out := make(chan entity.OrderbookUpdate, 1)
	if err := p.runConnection(context.Background(), []string{"X"}, out, func() {}); err == nil {
		t.Fatal("expected a dial error for an unroutable endpoint")
	}
}

func TestEmitterDeliversCloneAndDropsWhenFull(t *testing.T) {
	out := make(chan entity.OrderbookUpdate, 1)
	em := newEmitter(out, testLogger(), testMetrics(t))

	book := entity.NewOrderbook("test", "X")
	book.ApplyLevel(entity.Bid, "100", "1")
	if !em.emit(book, true, time.Unix(1, 0)) {
		t.Fatal("snapshot should be delivered when the buffer has room")
	}

	// Buffer is now full; this emit must be dropped without blocking.
	book.ApplyLevel(entity.Bid, "100", "9")
	if em.emit(book, false, time.Unix(2, 0)) {
		t.Fatal("delta should be dropped when the buffer is full")
	}

	if len(out) != 1 {
		t.Fatalf("expected 1 buffered update (second dropped), got %d", len(out))
	}
	upd := <-out
	if !upd.IsSnapshot {
		t.Error("expected the first (snapshot) update to survive")
	}
	if sz, ok := sizeAt(upd.Book.Bids(), "100"); !ok || sz != "1" {
		t.Errorf("emitted book size at 100 = %q (ok=%v), want 1 (must be a clone taken at emit time)", sz, ok)
	}
}

// TestEmitterDoesNotBlockOnFullBuffer is the regression test for the
// connection-wedge bug: a snapshot that cannot be delivered (consumer stalled,
// buffer full) must NOT block the caller. A blocking snapshot send parks the read
// loop, which then stops draining the socket and never reconnects.
func TestEmitterDoesNotBlockOnFullBuffer(t *testing.T) {
	out := make(chan entity.OrderbookUpdate) // no reader, no capacity
	em := newEmitter(out, testLogger(), testMetrics(t))
	book := entity.NewOrderbook("test", "X")
	book.ApplyLevel(entity.Bid, "100", "1")

	done := make(chan struct{})
	go func() {
		em.emit(book, true, time.Unix(1, 0)) // snapshot would block under the old design
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emit blocked on a snapshot into a full buffer; a stalled consumer would wedge the connection")
	}
}

// TestEmitterDefersDroppedSnapshot: a snapshot dropped because the buffer was full
// must carry its discontinuity flag onto the next delivered update for the symbol,
// so the consumer still learns of the (re)sync.
func TestEmitterDefersDroppedSnapshot(t *testing.T) {
	out := make(chan entity.OrderbookUpdate, 1)
	em := newEmitter(out, testLogger(), testMetrics(t))
	book := entity.NewOrderbook("test", "X")
	book.ApplyLevel(entity.Bid, "100", "1")

	if !em.emit(book, false, time.Unix(1, 0)) {
		t.Fatal("first update should be delivered into the empty buffer")
	}
	if em.emit(book, true, time.Unix(2, 0)) {
		t.Fatal("snapshot should drop when the buffer is full")
	}
	<-out // drain the first update so the buffer has room again
	if !em.emit(book, false, time.Unix(3, 0)) {
		t.Fatal("delta should be delivered once the buffer drains")
	}
	if got := <-out; !got.IsSnapshot {
		t.Error("dropped snapshot's discontinuity must be deferred onto the next delivered update")
	}
}

// TestEmitterLogsDroppedUpdates: dropping updates is deliberate best-effort, but
// it must not be silent. The first drop warns immediately; further drops inside
// the rate-limit window are counted, not logged, so a stalled consumer cannot
// turn the log into the next bottleneck.
func TestEmitterLogsDroppedUpdates(t *testing.T) {
	logger, sb := captureLogger()
	out := make(chan entity.OrderbookUpdate, 1)
	em := newEmitter(out, logger, testMetrics(t))
	book := entity.NewOrderbook("test", "X")
	book.ApplyLevel(entity.Bid, "100", "1")

	if !em.emit(book, false, time.Unix(1, 0)) {
		t.Fatal("first update should be delivered into the empty buffer")
	}
	if strings.Contains(sb.String(), "dropped") {
		t.Fatal("no drop yet, nothing should be logged")
	}
	// Buffer full: both of these drop, but only the first logs within the window.
	em.emit(book, false, time.Unix(2, 0))
	em.emit(book, false, time.Unix(3, 0))
	if n := strings.Count(sb.String(), "orderbook updates dropped"); n != 1 {
		t.Errorf("drop warning logged %d times, want exactly 1 (rate-limited)", n)
	}
}

func TestReconnectLoopRetriesAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := testConfig()
	cfg.InitialBackoff = time.Millisecond
	cfg.MaxBackoff = 2 * time.Millisecond

	var calls atomic.Int32
	m := testMetrics(t) // built here: t.Fatalf must not run on the loop goroutine
	loopDone := make(chan struct{})
	go func() {
		reconnectLoop(ctx, cfg, cfg.Logger, m, func(ctx context.Context, ready func()) error {
			ready()
			if calls.Add(1) >= 3 {
				cancel()
			}
			return errors.New("boom")
		})
		close(loopDone)
	}()

	select {
	case <-loopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnectLoop did not stop after context cancellation")
	}
	if got := calls.Load(); got < 3 {
		t.Errorf("connect called %d times, want >= 3", got)
	}
}

func TestRunConnectionsClosesChannelWhenAllReturn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	groups := [][]string{{"a"}, {"b"}}

	var wg sync.WaitGroup
	wg.Add(len(groups))
	out := runConnections(ctx, groups, 4, func(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate) {
		defer wg.Done()
		<-ctx.Done() // run until cancelled
	})

	cancel()
	wg.Wait()

	// The aggregator goroutine should close out after both connections return.
	select {
	case _, ok := <-out:
		if ok {
			t.Error("expected closed channel, got a value")
		}
	case <-time.After(time.Second):
		t.Fatal("output channel was not closed after all connections returned")
	}
}
