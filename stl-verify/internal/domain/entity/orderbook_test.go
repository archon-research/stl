package entity

import (
	"testing"
	"time"
)

func TestSideString(t *testing.T) {
	tests := []struct {
		side Side
		want string
	}{
		{Bid, "bid"},
		{Ask, "ask"},
		{Side(99), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.side.String(); got != tt.want {
			t.Errorf("Side(%d).String() = %q, want %q", tt.side, got, tt.want)
		}
	}
}

// sizeAt returns the size string at price, or ok=false when absent.
func sizeAt(levels []PriceLevel, price string) (string, bool) {
	for _, l := range levels {
		if l.Price == price {
			return l.Size, true
		}
	}
	return "", false
}

func TestOrderbookApplyLevel(t *testing.T) {
	tests := []struct {
		name      string
		side      Side
		price     string
		size      string
		wantDepth int
		wantSize  string // size at price after apply ("" == absent)
	}{
		{name: "insert bid", side: Bid, price: "100", size: "2", wantDepth: 1, wantSize: "2"},
		{name: "insert ask", side: Ask, price: "101", size: "3", wantDepth: 1, wantSize: "3"},
		{name: "zero size removes", side: Bid, price: "100", size: "0", wantDepth: 0, wantSize: ""},
		{name: "negative size removes", side: Bid, price: "100", size: "-1", wantDepth: 0, wantSize: ""},
		{name: "non-finite size removes", side: Bid, price: "100", size: "NaN", wantDepth: 0, wantSize: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ob := NewOrderbook("test", "BTCUSDT")
			ob.ApplyLevel(tt.side, tt.price, tt.size)
			if got := ob.Depth(tt.side); got != tt.wantDepth {
				t.Errorf("Depth = %d, want %d", got, tt.wantDepth)
			}
			levels := ob.Bids()
			if tt.side == Ask {
				levels = ob.Asks()
			}
			got, _ := sizeAt(levels, tt.price)
			if got != tt.wantSize {
				t.Errorf("size at %s = %q, want %q", tt.price, got, tt.wantSize)
			}
		})
	}
}

func TestOrderbookApplyLevelIsAbsolute(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, "100", "5")
	ob.ApplyLevel(Bid, "100", "9") // absolute replace, not additive
	if sz, ok := sizeAt(ob.Bids(), "100"); !ok || sz != "9" {
		t.Fatalf("size at 100 = %q ok=%v, want 9", sz, ok)
	}
	if ob.Depth(Bid) != 1 {
		t.Errorf("Depth(Bid) = %d, want 1", ob.Depth(Bid))
	}
}

func TestOrderbookReplaceSide(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, "1", "1") // should be cleared by ReplaceSide
	ob.ReplaceSide(Bid, []PriceLevel{{Price: "100", Size: "2"}, {Price: "99", Size: "3"}})
	if ob.Depth(Bid) != 2 {
		t.Fatalf("Depth(Bid) = %d, want 2", ob.Depth(Bid))
	}
	if sz, ok := sizeAt(ob.Bids(), "100"); !ok || sz != "2" {
		t.Errorf("size at 100 = %q ok=%v, want 2", sz, ok)
	}
	if _, ok := sizeAt(ob.Bids(), "1"); ok {
		t.Error("price 1 should have been cleared by ReplaceSide")
	}
}

func TestOrderbookReset(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, "100", "1")
	ob.ApplyLevel(Ask, "101", "1")
	ob.LastUpdateID = 42
	ob.Reset()
	if ob.Depth(Bid) != 0 || ob.Depth(Ask) != 0 {
		t.Error("Reset should clear both sides")
	}
	if ob.LastUpdateID != 0 {
		t.Errorf("Reset should zero LastUpdateID, got %d", ob.LastUpdateID)
	}
}

func TestOrderbookPreservesExactStrings(t *testing.T) {
	// A value with more significant digits than float64 can hold must be stored
	// and returned byte-for-byte — this is the whole point of string keys/values.
	ob := NewOrderbook("test", "BTCUSDT")
	const price = "0.000000000000001234"
	const size = "12345.678901234567890"
	ob.ApplyLevel(Bid, price, size)
	if sz, ok := sizeAt(ob.Bids(), price); !ok || sz != size {
		t.Errorf("size at %s = %q ok=%v, want %q (exact string preserved)", price, sz, ok, size)
	}
}

func TestOrderbookCloneIsIndependent(t *testing.T) {
	ob := NewOrderbook("binance", "BTCUSDT")
	ob.ApplyLevel(Bid, "100", "1")
	ob.ApplyLevel(Ask, "101", "1")
	ob.LastUpdateID = 7

	clone := ob.Clone()
	// Mutate the original after cloning.
	ob.ApplyLevel(Bid, "100", "5")
	ob.ApplyLevel(Bid, "99", "2")
	ob.LastUpdateID = 8

	if sz, ok := sizeAt(clone.Bids(), "100"); !ok || sz != "1" {
		t.Errorf("clone size at 100 = %q ok=%v, want 1 (clone must not see later mutation)", sz, ok)
	}
	if clone.Depth(Bid) != 1 {
		t.Errorf("clone Depth(Bid) = %d, want 1", clone.Depth(Bid))
	}
	if clone.LastUpdateID != 7 {
		t.Errorf("clone LastUpdateID = %d, want 7", clone.LastUpdateID)
	}
	if clone.Exchange != "binance" || clone.Symbol != "BTCUSDT" {
		t.Errorf("clone metadata = %s/%s, want binance/BTCUSDT", clone.Exchange, clone.Symbol)
	}
}

func TestNewOrderbookUpdate(t *testing.T) {
	ob := NewOrderbook("binance", "BTCUSDT")
	ob.ApplyLevel(Bid, "100", "1")
	now := time.Unix(1700000000, 0)

	upd := NewOrderbookUpdate(ob, true, now)
	if upd.Exchange != "binance" || upd.Symbol != "BTCUSDT" {
		t.Errorf("update metadata = %s/%s", upd.Exchange, upd.Symbol)
	}
	if !upd.IsSnapshot || !upd.Time.Equal(now) {
		t.Errorf("update flags: IsSnapshot=%v Time=%v", upd.IsSnapshot, upd.Time)
	}
	if upd.Time.Location() != time.UTC {
		t.Errorf("update Time location = %v, want UTC", upd.Time.Location())
	}
	// The update's book must be a clone, independent of later mutation.
	ob.ApplyLevel(Bid, "100", "9")
	if sz, ok := sizeAt(upd.Book.Bids(), "100"); !ok || sz != "1" {
		t.Errorf("update book size at 100 = %q ok=%v, want 1 (must be a clone)", sz, ok)
	}
}
