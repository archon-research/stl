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

func TestParseLevels(t *testing.T) {
	levels, err := parseLevels([][]string{{"100", "1"}, {"99", "2", "ignored"}})
	if err != nil {
		t.Fatalf("parseLevels err = %v", err)
	}
	if len(levels) != 2 || levels[0].Price != "100" || levels[1].Size != "2" {
		t.Errorf("parseLevels = %+v", levels)
	}

	if _, err := parseLevels([][]string{{"100"}}); err == nil {
		t.Error("parseLevels with one field should error")
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

func TestEmitDeliversCloneAndDropsWhenFull(t *testing.T) {
	ctx := context.Background()
	out := make(chan entity.OrderbookUpdate, 1)

	book := entity.NewOrderbook("test", "X")
	book.ApplyLevel(entity.Bid, "100", "1")

	emit(ctx, out, book, true, time.Unix(1, 0))

	// Buffer is now full; this emit must be dropped without blocking.
	book.ApplyLevel(entity.Bid, "100", "9")
	emit(ctx, out, book, false, time.Unix(2, 0))

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

func TestEmitNeverDropsSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan entity.OrderbookUpdate, 1)

	// Fill the buffer with a delta so it is full.
	book := entity.NewOrderbook("test", "X")
	book.ApplyLevel(entity.Bid, "100", "1")
	emit(ctx, out, book, false, time.Unix(1, 0))
	if len(out) != 1 {
		t.Fatalf("setup: expected buffer full, got len %d", len(out))
	}

	// A snapshot emit into the full buffer must block until a reader drains, not drop.
	delivered := make(chan struct{})
	go func() {
		emit(ctx, out, book, true, time.Unix(2, 0))
		close(delivered)
	}()

	// Drain the pre-existing delta so the snapshot can be delivered.
	first := <-out
	if first.IsSnapshot {
		t.Error("first buffered update should be the delta, not the snapshot")
	}
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("snapshot emit blocked permanently; it should have been delivered after drain")
	}
	got := <-out
	if !got.IsSnapshot {
		t.Error("snapshot update must not be dropped even when the buffer was full")
	}
}

func TestEmitRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Unbuffered channel with no reader: emit must not block once ctx is done.
	out := make(chan entity.OrderbookUpdate)
	book := entity.NewOrderbook("test", "X")
	done := make(chan struct{})
	go func() {
		emit(ctx, out, book, false, time.Unix(1, 0))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("emit blocked despite cancelled context")
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
	if got := parseUnixMillisOrNow("1700000000000"); !got.Equal(time.UnixMilli(1700000000000)) {
		t.Errorf("parseUnixMillisOrNow = %v", got)
	}
	if got := parseUnixMillisOrNow("1700000000000"); got.Location() != time.UTC {
		t.Errorf("parseUnixMillisOrNow location = %v, want UTC", got.Location())
	}
	// Malformed input falls back to ~now.
	if got := parseUnixMillisOrNow("nope"); time.Since(got) > time.Minute {
		t.Errorf("parseUnixMillisOrNow fallback = %v, want ~now", got)
	}
	want := time.Date(2023, 2, 9, 20, 32, 50, 0, time.UTC)
	if got := parseRFC3339OrNow("2023-02-09T20:32:50Z"); !got.Equal(want) {
		t.Errorf("parseRFC3339OrNow = %v, want %v", got, want)
	}
	if got := parseRFC3339OrNow(""); time.Since(got) > time.Minute {
		t.Errorf("parseRFC3339OrNow fallback = %v, want ~now", got)
	}
}
