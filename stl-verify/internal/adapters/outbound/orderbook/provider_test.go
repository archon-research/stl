package orderbook

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestValidateSymbols(t *testing.T) {
	tests := []struct {
		name    string
		symbols []string
		wantErr bool
	}{
		{name: "ok", symbols: []string{"BTCUSDT", "ETHUSDT"}, wantErr: false},
		{name: "empty list", symbols: nil, wantErr: true},
		{name: "empty entry", symbols: []string{"BTCUSDT", ""}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSymbols(tt.symbols)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSymbols(%v) err = %v, wantErr %v", tt.symbols, err, tt.wantErr)
			}
		})
	}
}

func TestNormalizeSeparatedPair(t *testing.T) {
	tests := []struct {
		name    string
		symbol  string
		sep     string
		want    string
		wantErr bool
	}{
		{name: "valid upper-cased", symbol: "btc-usd", sep: "-", want: "BTC-USD"},
		{name: "already upper", symbol: "BTC-USD", sep: "-", want: "BTC-USD"},
		{name: "slash separator", symbol: "xbt/usd", sep: "/", want: "XBT/USD"},
		{name: "alphanumeric with digits", symbol: "1inch-usd", sep: "-", want: "1INCH-USD"},
		{name: "empty", symbol: "", sep: "-", wantErr: true},
		{name: "missing separator", symbol: "BTCUSD", sep: "-", wantErr: true},
		{name: "extra separator", symbol: "BTC-USD-X", sep: "-", wantErr: true},
		{name: "empty left part", symbol: "-USD", sep: "-", wantErr: true},
		{name: "empty right part", symbol: "BTC-", sep: "-", wantErr: true},
		{name: "wrong separator", symbol: "BTC/USD", sep: "-", wantErr: true},
		{name: "leading whitespace rejected", symbol: " btc-usd", sep: "-", wantErr: true},
		{name: "trailing whitespace rejected", symbol: "btc-usd ", sep: "-", wantErr: true},
		{name: "whitespace around separator rejected", symbol: " btc - usd ", sep: "-", wantErr: true},
		{name: "internal space in part rejected", symbol: "bt c-usd", sep: "-", wantErr: true},
		{name: "punctuation in part rejected", symbol: "btc!-usd", sep: "-", wantErr: true},
		{name: "tab whitespace rejected", symbol: "btc\t-usd", sep: "-", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeSeparatedPair(tt.symbol, tt.sep)
			if (err != nil) != tt.wantErr {
				t.Fatalf("normalizeSeparatedPair(%q, %q) err = %v, wantErr %v", tt.symbol, tt.sep, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("normalizeSeparatedPair(%q, %q) = %q, want %q", tt.symbol, tt.sep, got, tt.want)
			}
		})
	}
}

func TestChunkSymbols(t *testing.T) {
	tests := []struct {
		name    string
		symbols []string
		size    int
		want    [][]string
	}{
		{name: "single group when size 0", symbols: []string{"a", "b"}, size: 0, want: [][]string{{"a", "b"}}},
		{name: "single group when fits", symbols: []string{"a", "b"}, size: 5, want: [][]string{{"a", "b"}}},
		{name: "even split", symbols: []string{"a", "b", "c", "d"}, size: 2, want: [][]string{{"a", "b"}, {"c", "d"}}},
		{name: "uneven split", symbols: []string{"a", "b", "c"}, size: 2, want: [][]string{{"a", "b"}, {"c"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chunkSymbols(tt.symbols, tt.size)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("chunkSymbols = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name      string
		price     string
		size      string
		wantPrice string
		wantSize  string
		wantErr   bool
	}{
		{name: "ok", price: "100.5", size: "2.25", wantPrice: "100.5", wantSize: "2.25"},
		{name: "zero size ok", price: "100", size: "0", wantPrice: "100", wantSize: "0"},
		{name: "high precision preserved verbatim", price: "0.000000000000001234", size: "12345.678901234567890", wantPrice: "0.000000000000001234", wantSize: "12345.678901234567890"},
		{name: "bad price", price: "abc", size: "1", wantErr: true},
		{name: "bad size", price: "1", size: "xyz", wantErr: true},
		{name: "negative price rejected", price: "-1", size: "1", wantErr: true},
		{name: "zero price rejected", price: "0", size: "1", wantErr: true},
		{name: "negative size rejected", price: "100", size: "-1", wantErr: true},
		{name: "NaN price rejected", price: "NaN", size: "1", wantErr: true},
		{name: "Inf price rejected", price: "Inf", size: "1", wantErr: true},
		{name: "NaN size rejected", price: "100", size: "NaN", wantErr: true},
		{name: "Inf size rejected", price: "100", size: "+Inf", wantErr: true},
		// Scientific notation parses as a float but breaks delete-by-key matching
		// and Kraken's checksum tokeniser, so it must be rejected (kept verbatim
		// would strand a ghost level).
		{name: "exponent price rejected", price: "1e5", size: "1", wantErr: true},
		{name: "exponent size rejected", price: "100", size: "1E3", wantErr: true},
		{name: "signed price rejected", price: "+100", size: "1", wantErr: true},
		{name: "trailing dot price rejected", price: "100.", size: "1", wantErr: true},
		{name: "leading dot price rejected", price: ".5", size: "1", wantErr: true},
		{name: "empty size rejected", price: "100", size: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lvl, err := parseLevel(tt.price, tt.size)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseLevel err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if lvl.Price != tt.wantPrice || lvl.Size != tt.wantSize {
				t.Errorf("parseLevel = %+v, want {%q %q}", lvl, tt.wantPrice, tt.wantSize)
			}
		})
	}
}

func TestConfigWithDefaults(t *testing.T) {
	// Every zero-valued field falls back to the production default.
	c := Config{}.withDefaults()
	d := DefaultConfig()
	if c.Logger == nil || c.InitialBackoff != d.InitialBackoff || c.MaxBackoff != d.MaxBackoff ||
		c.BackoffFactor != d.BackoffFactor || c.HandshakeTimeout != d.HandshakeTimeout ||
		c.ReadTimeout != d.ReadTimeout || c.WriteTimeout != d.WriteTimeout ||
		c.PingInterval != d.PingInterval || c.OutputBuffer != d.OutputBuffer {
		t.Errorf("withDefaults left a zero field unfilled: %+v", c)
	}
	// A negative PingInterval is preserved (it disables pings), not overridden.
	if got := (Config{PingInterval: -1}).withDefaults().PingInterval; got != -1 {
		t.Errorf("negative PingInterval = %v, want -1 preserved", got)
	}
}

func TestSymbolAllowed(t *testing.T) {
	allowed := symbolSet([]string{"BTC-USD", "eth-usd"})
	if !symbolAllowed(allowed, "BTC-USD") {
		t.Error("subscribed symbol should be allowed")
	}
	if !symbolAllowed(allowed, "btc-usd") {
		t.Error("case-insensitive match should be allowed")
	}
	if !symbolAllowed(allowed, "ETH-USD") {
		t.Error("symbol subscribed in lower case should be allowed when echoed upper")
	}
	if symbolAllowed(allowed, "SOL-USD") {
		t.Error("unsubscribed symbol should not be allowed")
	}
	// A nil set (handlers built directly in tests) allows everything.
	if !symbolAllowed(nil, "ANYTHING") {
		t.Error("nil allow-set should allow everything")
	}
}

func TestApplyDeltaLevels(t *testing.T) {
	ob := entity.NewOrderbook("test", "X")
	ob.ApplyLevel(entity.Bid, "100", "5")
	// Update 100 -> 7, add 99 -> 3, remove a non-existent level (no-op).
	err := applyDeltaLevels(ob, entity.Bid, [][]string{{"100", "7"}, {"99", "3"}, {"50", "0"}})
	if err != nil {
		t.Fatalf("applyDeltaLevels err = %v", err)
	}
	if ob.Depth(entity.Bid) != 2 {
		t.Errorf("Depth(Bid) = %d, want 2", ob.Depth(entity.Bid))
	}
	if sz, ok := sizeAt(ob.Bids(), "100"); !ok || sz != "7" {
		t.Errorf("size at 100 = %q (ok=%v), want 7", sz, ok)
	}

	if err := applyDeltaLevels(ob, entity.Bid, [][]string{{"bad"}}); err == nil {
		t.Error("applyDeltaLevels with malformed level should error")
	}
}

func TestEmitterDeliversCloneAndDropsWhenFull(t *testing.T) {
	out := make(chan entity.OrderbookUpdate, 1)
	em := newEmitter(out)

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
	em := newEmitter(out)
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
	em := newEmitter(out)
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

func TestReconnectLoopRetriesAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, BackoffFactor: 2}.withDefaults()

	var calls atomic.Int32
	loopDone := make(chan struct{})
	go func() {
		reconnectLoop(ctx, cfg, cfg.Logger, func(ctx context.Context, ready func()) error {
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

func TestParseTimeHelpers(t *testing.T) {
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
	want := time.Date(2023, 2, 9, 20, 32, 50, 0, time.UTC)
	if got := parseRFC3339OrZero("2023-02-09T20:32:50Z"); !got.Equal(want) {
		t.Errorf("parseRFC3339OrZero = %v, want %v", got, want)
	}
	if got := parseRFC3339OrZero(""); !got.IsZero() {
		t.Errorf("parseRFC3339OrZero(empty) = %v, want zero", got)
	}
}
