package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/gorilla/websocket"
)

// --- helpers -----------------------------------------------------------------

func bDiff(symbol string, firstID, finalID int64, bids, asks [][]string) binanceDepthDiff {
	return binanceDepthDiff{
		EventType: "depthUpdate",
		EventTime: 1,
		Symbol:    symbol,
		FirstID:   firstID,
		FinalID:   finalID,
		Bids:      bids,
		Asks:      asks,
	}
}

// combinedFrame marshals a diff into a Binance combined-stream frame.
func combinedFrame(t *testing.T, diff binanceDepthDiff) []byte {
	t.Helper()
	data, err := json.Marshal(diff)
	if err != nil {
		t.Fatalf("marshal diff: %v", err)
	}
	raw, err := json.Marshal(binanceCombined{Stream: strings.ToLower(diff.Symbol) + "@depth", Data: data})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return raw
}

func newBinanceRESTServer(t *testing.T, snap binanceSnapshot) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(snap)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func sizeAt(levels []entity.PriceLevel, price string) (string, bool) {
	for _, l := range levels {
		if l.Price == price {
			return l.Size, true
		}
	}
	return "", false
}

// --- sync-state-machine tests ------------------------------------------------

// TestBinanceReconcileBufferedDiffs is the core Snapshot+Delta test: diffs that
// arrived while the REST snapshot was in flight are buffered, then reconciled
// against the snapshot's lastUpdateId and replayed in order.
func TestBinanceReconcileBufferedDiffs(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	ctx := context.Background()
	out := make(chan entity.OrderbookUpdate, 16)
	snapCh := make(chan binanceSnapshotResult, 4)

	st := &binanceState{book: entity.NewOrderbook(exchangeBinance, "BTCUSDT")}
	states := map[string]*binanceState{"BTCUSDT": st}
	st.buffer = []binanceDepthDiff{
		bDiff("BTCUSDT", 98, 100, [][]string{{"99", "1"}}, nil),                        // dropped: u <= 100
		bDiff("BTCUSDT", 101, 105, [][]string{{"100", "2"}}, [][]string{{"110", "3"}}), // first applied: U<=101<=u
		bDiff("BTCUSDT", 106, 110, [][]string{{"100", "0"}}, nil),                      // contiguous; removes bid 100
	}

	res := binanceSnapshotResult{symbol: "BTCUSDT", snap: binanceSnapshot{
		LastUpdateID: 100,
		Bids:         [][]string{{"99", "5"}, {"98", "4"}},
		Asks:         [][]string{{"120", "7"}},
	}}

	if err := p.applySnapshot(ctx, states, res, out, snapCh); err != nil {
		t.Fatalf("applySnapshot: %v", err)
	}

	if !st.synced {
		t.Fatal("state should be synced after applySnapshot")
	}
	if st.lastID != 110 {
		t.Errorf("lastID = %d, want 110", st.lastID)
	}

	upd := <-out
	if !upd.IsSnapshot {
		t.Error("first emitted update should be flagged as a snapshot")
	}

	// Expected: bid 100 added by diff2 then removed by diff3; ask 110 added.
	bids, asks := upd.Book.Bids(), upd.Book.Asks()
	if sz, ok := sizeAt(bids, "99"); !ok || sz != "5" {
		t.Errorf("bid 99 = %q (ok=%v), want 5", sz, ok)
	}
	if _, ok := sizeAt(bids, "100"); ok {
		t.Error("bid 100 should have been removed by the zero-size delta")
	}
	if len(bids) != 2 {
		t.Errorf("bid depth = %d, want 2", len(bids))
	}
	if sz, ok := sizeAt(asks, "110"); !ok || sz != "3" {
		t.Errorf("ask 110 = %q (ok=%v), want 3", sz, ok)
	}
	if sz, ok := sizeAt(asks, "120"); !ok || sz != "7" {
		t.Errorf("ask 120 = %q (ok=%v), want 7", sz, ok)
	}
}

func TestBinanceApplyDiffSequenceRules(t *testing.T) {
	tests := []struct {
		name        string
		lastID      int64
		expectFirst bool
		diff        binanceDepthDiff
		wantErr     error
		wantApplied bool // whether the level change took effect
		wantLastID  int64
	}{
		{
			name:        "stale diff dropped",
			lastID:      100,
			diff:        bDiff("X", 90, 100, [][]string{{"50", "9"}}, nil),
			wantApplied: false,
			wantLastID:  100,
		},
		{
			name:        "first diff accepted at boundary",
			lastID:      100,
			expectFirst: true,
			diff:        bDiff("X", 99, 105, [][]string{{"50", "9"}}, nil),
			wantApplied: true,
			wantLastID:  105,
		},
		{
			name:        "first diff gap rejected",
			lastID:      100,
			expectFirst: true,
			diff:        bDiff("X", 103, 105, [][]string{{"50", "9"}}, nil),
			wantErr:     errSequenceGap,
		},
		{
			name:        "contiguous diff accepted",
			lastID:      105,
			diff:        bDiff("X", 106, 110, [][]string{{"50", "9"}}, nil),
			wantApplied: true,
			wantLastID:  110,
		},
		{
			name:    "non-contiguous diff rejected",
			lastID:  105,
			diff:    bDiff("X", 108, 110, [][]string{{"50", "9"}}, nil),
			wantErr: errSequenceGap,
		},
	}
	p := NewBinanceProvider(testConfig())
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &binanceState{
				book:        entity.NewOrderbook(exchangeBinance, "X"),
				synced:      true,
				expectFirst: tt.expectFirst,
				lastID:      tt.lastID,
			}
			err := p.applyDiff(st, tt.diff)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if st.lastID != tt.wantLastID {
				t.Errorf("lastID = %d, want %d", st.lastID, tt.wantLastID)
			}
			_, ok := sizeAt(st.book.Bids(), "50")
			if ok != tt.wantApplied {
				t.Errorf("level applied = %v, want %v", ok, tt.wantApplied)
			}
		})
	}
}

func TestBinanceHandleDiffBuffersWhenUnsynced(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	ctx := context.Background()
	out := make(chan entity.OrderbookUpdate, 4)
	snapCh := make(chan binanceSnapshotResult, 4)

	st := &binanceState{book: entity.NewOrderbook(exchangeBinance, "BTCUSDT")} // synced == false
	states := map[string]*binanceState{"BTCUSDT": st}

	raw := combinedFrame(t, bDiff("BTCUSDT", 5, 10, [][]string{{"100", "1"}}, nil))
	if err := p.handleDiff(ctx, states, raw, out, snapCh); err != nil {
		t.Fatalf("handleDiff: %v", err)
	}
	if len(st.buffer) != 1 {
		t.Errorf("buffer len = %d, want 1 (unsynced diffs must be buffered)", len(st.buffer))
	}
	if len(out) != 0 {
		t.Error("no update should be emitted while unsynced")
	}
}

func TestBinanceHandleDiffResyncOnGap(t *testing.T) {
	restURL := newBinanceRESTServer(t, binanceSnapshot{LastUpdateID: 500, Bids: [][]string{{"1", "1"}}, Asks: [][]string{{"2", "1"}}})
	p := NewBinanceProvider(testConfig())
	p.restBase = restURL

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan entity.OrderbookUpdate, 4)
	snapCh := make(chan binanceSnapshotResult, 4)

	st := &binanceState{
		book:   entity.NewOrderbook(exchangeBinance, "BTCUSDT"),
		synced: true,
		lastID: 100,
	}
	states := map[string]*binanceState{"BTCUSDT": st}

	// A diff whose FirstID skips ahead of lastID+1 is a gap.
	raw := combinedFrame(t, bDiff("BTCUSDT", 200, 205, [][]string{{"100", "1"}}, nil))
	if err := p.handleDiff(ctx, states, raw, out, snapCh); err != nil {
		t.Fatalf("handleDiff: %v", err)
	}
	if st.synced {
		t.Error("a sequence gap must mark the symbol un-synced")
	}
	if !st.fetching {
		t.Error("a sequence gap must trigger a snapshot re-fetch")
	}
	// The triggering diff is retained for the upcoming reconciliation.
	if len(st.buffer) != 1 {
		t.Errorf("buffer len = %d, want 1 (gap diff retained)", len(st.buffer))
	}

	// Drain the re-fetch result so its goroutine exits cleanly.
	select {
	case <-snapCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a re-fetched snapshot result")
	}
}

func TestBinanceSnapshotFetchErrorIsFatal(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	out := make(chan entity.OrderbookUpdate, 1)
	snapCh := make(chan binanceSnapshotResult, 1)
	st := &binanceState{book: entity.NewOrderbook(exchangeBinance, "BTCUSDT")}
	states := map[string]*binanceState{"BTCUSDT": st}

	res := binanceSnapshotResult{symbol: "BTCUSDT", err: errors.New("network down")}
	if err := p.applySnapshot(context.Background(), states, res, out, snapCh); err == nil {
		t.Fatal("snapshot fetch error should be fatal to the connection")
	}
	if st.fetching {
		t.Error("fetching flag should be cleared on result")
	}
}

// --- end-to-end test ---------------------------------------------------------

// TestBinanceWatchEndToEnd drives the full public Watch path against mock REST
// and WebSocket servers and asserts the book converges to the correct state.
func TestBinanceWatchEndToEnd(t *testing.T) {
	restURL := newBinanceRESTServer(t, binanceSnapshot{
		LastUpdateID: 100,
		Bids:         [][]string{{"99", "5"}, {"98", "4"}},
		Asks:         [][]string{{"120", "7"}},
	})

	wsSrv := newWSTestServer(t, func(conn *websocket.Conn) {
		frames := [][]byte{
			combinedFrame(t, bDiff("BTCUSDT", 101, 105, [][]string{{"100", "2"}}, [][]string{{"110", "3"}})),
			combinedFrame(t, bDiff("BTCUSDT", 106, 110, [][]string{{"100", "0"}}, nil)),
			combinedFrame(t, bDiff("BTCUSDT", 111, 115, [][]string{{"97", "1"}}, [][]string{{"121", "2"}})),
		}
		for _, f := range frames {
			if err := conn.WriteMessage(websocket.TextMessage, f); err != nil {
				return
			}
		}
		keepOpen(conn)
	})

	p := NewBinanceProvider(testConfig())
	p.restBase = restURL
	p.wsBase = wsSrv.url

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ch, err := p.Watch(ctx, []string{"BTCUSDT"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	var (
		sawSnapshot bool
		final       *entity.Orderbook
	)
	deadline := time.After(4 * time.Second)
	for final == nil {
		select {
		case upd, ok := <-ch:
			if !ok {
				t.Fatal("channel closed before book converged")
			}
			if upd.IsSnapshot {
				sawSnapshot = true
			}
			if upd.Book.LastUpdateID == 115 {
				final = upd.Book
			}
		case <-deadline:
			t.Fatal("timed out before book converged to lastUpdateId 115")
		}
	}

	if !sawSnapshot {
		t.Error("expected at least one snapshot-flagged update")
	}
	bids, asks := final.Bids(), final.Asks()
	if len(bids) != 3 {
		t.Errorf("final bid depth = %d, want 3 (99,98,97)", len(bids))
	}
	if _, ok := sizeAt(bids, "100"); ok {
		t.Error("bid 100 should have been removed")
	}
	if sz, ok := sizeAt(bids, "97"); !ok || sz != "1" {
		t.Errorf("bid 97 = %q (ok=%v), want 1", sz, ok)
	}
	if sz, ok := sizeAt(asks, "110"); !ok || sz != "3" {
		t.Errorf("ask 110 = %q (ok=%v), want 3", sz, ok)
	}
	if sz, ok := sizeAt(asks, "121"); !ok || sz != "2" {
		t.Errorf("ask 121 = %q (ok=%v), want 2", sz, ok)
	}
}

// TestBinanceReadyNotCalledOnSnapshotError covers Issue 2 for Binance: a
// connection that dials and subscribes but whose REST snapshot fetch fails must
// NOT reset the reconnect backoff, so a connect-succeeds-but-sync-fails loop
// backs off instead of hammering at the initial interval.
func TestBinanceReadyNotCalledOnSnapshotError(t *testing.T) {
	// REST server returns a snapshot with no lastUpdateId, which fetchSnapshot
	// rejects, making the snapshot fetch fatal.
	restURL := newBinanceRESTServer(t, binanceSnapshot{})
	wsSrv := newWSTestServer(t, func(conn *websocket.Conn) { keepOpen(conn) })

	p := NewBinanceProvider(testConfig())
	p.restBase = restURL
	p.wsBase = wsSrv.url

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var readyCalls atomic.Int32
	out := make(chan entity.OrderbookUpdate, 4)
	done := make(chan error, 1)
	go func() {
		done <- p.connectAndSync(ctx, []string{"BTCUSDT"}, out, func() { readyCalls.Add(1) })
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a fatal snapshot error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("connectAndSync did not return after the snapshot fetch failed")
	}
	if got := readyCalls.Load(); got != 0 {
		t.Errorf("ready called %d times, want 0 (snapshot never applied)", got)
	}
}

// TestBinanceReadyCalledOnFirstSnapshot covers the surrounding correct behaviour
// for Issue 2: once the REST snapshot is applied and the first book is emitted,
// ready fires exactly once to reset the backoff.
func TestBinanceReadyCalledOnFirstSnapshot(t *testing.T) {
	restURL := newBinanceRESTServer(t, binanceSnapshot{
		LastUpdateID: 100,
		Bids:         [][]string{{"99", "5"}},
		Asks:         [][]string{{"120", "7"}},
	})
	wsSrv := newWSTestServer(t, func(conn *websocket.Conn) { keepOpen(conn) })

	p := NewBinanceProvider(testConfig())
	p.restBase = restURL
	p.wsBase = wsSrv.url

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var readyCalls atomic.Int32
	out := make(chan entity.OrderbookUpdate, 4)
	go func() {
		_ = p.connectAndSync(ctx, []string{"BTCUSDT"}, out, func() { readyCalls.Add(1) })
	}()

	// The first emitted update is the snapshot; ready must fire by then.
	upd := receiveWithin(t, out, 2*time.Second)
	if !upd.IsSnapshot {
		t.Fatalf("first update should be a snapshot, got %+v", upd)
	}
	// Allow connectAndSync to run a moment longer to confirm ready stays at 1.
	cancel()
	if got := readyCalls.Load(); got != 1 {
		t.Errorf("ready called %d times, want exactly 1 (on first applied snapshot)", got)
	}
}

// TestBinanceReadyNotCalledWhenFirstSnapshotResyncs covers the Issue 2 edge
// case: if the very first snapshot's buffered-diff replay hits a sequence gap,
// the symbol is left un-synced and re-fetches. ready must NOT fire yet, since no
// usable book was produced.
func TestBinanceReadyNotCalledWhenFirstSnapshotResyncs(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan entity.OrderbookUpdate, 4)
	snapCh := make(chan binanceSnapshotResult, 4)

	st := &binanceState{book: entity.NewOrderbook(exchangeBinance, "BTCUSDT")}
	states := map[string]*binanceState{"BTCUSDT": st}
	// A buffered diff that skips ahead of lastUpdateId+1 forces a resync.
	st.buffer = []binanceDepthDiff{bDiff("BTCUSDT", 200, 205, [][]string{{"99", "1"}}, nil)}

	res := binanceSnapshotResult{symbol: "BTCUSDT", snap: binanceSnapshot{
		LastUpdateID: 100,
		Bids:         [][]string{{"99", "5"}},
		Asks:         [][]string{{"120", "7"}},
	}}
	if err := p.applySnapshot(ctx, states, res, out, snapCh); err != nil {
		t.Fatalf("applySnapshot: %v", err)
	}
	// The gap leaves the symbol un-synced (it re-fetches), so connectAndSync's
	// ready guard (which checks st.synced) would not fire.
	if st.synced {
		t.Error("a first-snapshot replay gap must leave the symbol un-synced")
	}
	if len(out) != 0 {
		t.Error("no book should be emitted when the first snapshot replay gaps")
	}

	// Drain the re-fetch so its goroutine exits cleanly.
	select {
	case <-snapCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a re-fetched snapshot result")
	}
}

func TestBinanceNameAndValidation(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	if p.Name() != "binance" {
		t.Errorf("Name = %q, want binance", p.Name())
	}
	if _, err := p.Watch(context.Background(), nil); err == nil {
		t.Error("Watch with no symbols should error")
	}
}

func TestBinanceWatchRejectsMalformedSymbol(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	// Binance symbols are concatenated (e.g. "BTCUSDT"); a dash-separated pair
	// is the wrong format and must be rejected at Watch entry.
	if _, err := p.Watch(context.Background(), []string{"BTC-USDT"}); err == nil {
		t.Error("Binance Watch should reject a dash-separated symbol")
	}
}

func TestBinanceWatchAcceptsLowercaseSymbol(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// A valid lower-case symbol is accepted (and upper-cased internally).
	if _, err := p.Watch(ctx, []string{"btcusdt"}); err != nil {
		t.Errorf("Binance Watch should accept a valid lower-case symbol, got %v", err)
	}
}

func TestCoinbaseWatchRejectsMalformedSymbol(t *testing.T) {
	p := NewCoinbaseProvider(testConfig())
	if _, err := p.Watch(context.Background(), []string{"BTCUSD"}); err == nil {
		t.Error("Coinbase Watch should reject a symbol with no dash separator")
	}
}

func TestCoinbaseWatchAcceptsLowercaseSymbol(t *testing.T) {
	p := NewCoinbaseProvider(testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := p.Watch(ctx, []string{"btc-usd"}); err != nil {
		t.Errorf("Coinbase Watch should accept a valid lower-case symbol, got %v", err)
	}
}

func TestOKXWatchRejectsMalformedSymbol(t *testing.T) {
	p := NewOKXProvider(testConfig())
	if _, err := p.Watch(context.Background(), []string{"BTCUSDT"}); err == nil {
		t.Error("OKX Watch should reject a symbol with no dash separator")
	}
}

func TestKrakenWatchRejectsMalformedSymbol(t *testing.T) {
	p := NewKrakenProvider(testConfig())
	// Kraken expects a slash separator (e.g. "XBT/USD"); a dash is wrong.
	if _, err := p.Watch(context.Background(), []string{"XBT-USD"}); err == nil {
		t.Error("Kraken Watch should reject a dash-separated symbol")
	}
}

func TestKrakenWatchAcceptsLowercaseSymbol(t *testing.T) {
	p := NewKrakenProvider(testConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := p.Watch(ctx, []string{"xbt/usd"}); err != nil {
		t.Errorf("Kraken Watch should accept a valid lower-case symbol, got %v", err)
	}
}

func TestBinanceStreamURL(t *testing.T) {
	p := NewBinanceProvider(testConfig())
	p.wsBase = "wss://example.test"
	got := p.streamURL([]string{"BTCUSDT", "ethusdt"})
	want := "wss://example.test/stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms"
	if got != want {
		t.Errorf("streamURL = %q, want %q", got, want)
	}
}

func TestDecodeBinanceDiff(t *testing.T) {
	// Valid depth diff.
	raw := combinedFrame(t, bDiff("BTCUSDT", 1, 2, [][]string{{"100", "1"}}, nil))
	diff, ok, err := decodeBinanceDiff(raw)
	if err != nil || !ok || diff.Symbol != "BTCUSDT" {
		t.Fatalf("valid diff: diff=%+v ok=%v err=%v", diff, ok, err)
	}

	// Combined frame with no data (e.g. a subscription ack).
	if _, ok, err := decodeBinanceDiff([]byte(`{"result":null,"id":1}`)); ok || err != nil {
		t.Errorf("ack frame: ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	// A non-depthUpdate event is ignored.
	other := combinedFrame(t, binanceDepthDiff{EventType: "trade", Symbol: "BTCUSDT"})
	if _, ok, err := decodeBinanceDiff(other); ok || err != nil {
		t.Errorf("non-depth event: ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	// Malformed JSON is an error.
	if _, _, err := decodeBinanceDiff([]byte(`{not json`)); err == nil {
		t.Error("malformed frame should error")
	}
}

func TestCoinbaseSideUnknown(t *testing.T) {
	if _, err := coinbaseSide("weird"); err == nil {
		t.Error("unknown side should error")
	}
}

func TestOKXAndKrakenProviderNameAndValidation(t *testing.T) {
	okx := NewOKXProvider(testConfig())
	if okx.Name() != "okx" {
		t.Errorf("OKX Name = %q", okx.Name())
	}
	if _, err := okx.Watch(context.Background(), nil); err == nil {
		t.Error("OKX Watch with no symbols should error")
	}

	kraken := NewKrakenProvider(testConfig())
	if kraken.Name() != "kraken" {
		t.Errorf("Kraken Name = %q", kraken.Name())
	}
	if _, err := kraken.Watch(context.Background(), nil); err == nil {
		t.Error("Kraken Watch with no symbols should error")
	}
}

func TestBinanceParseError(t *testing.T) {
	err := binanceParseError(400, []byte(`{"code":-1121,"msg":"Invalid symbol."}`))
	if err == nil || !strings.Contains(err.Error(), "Invalid symbol") {
		t.Errorf("binanceParseError = %v, want Invalid symbol", err)
	}
	if err := binanceParseError(200, []byte(`{"lastUpdateId":1}`)); err != nil {
		t.Errorf("binanceParseError on success body = %v, want nil", err)
	}
}
