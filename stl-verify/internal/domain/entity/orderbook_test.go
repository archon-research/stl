package entity

import (
	"testing"
	"time"
)

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
		{name: "padded zero size removes", side: Bid, price: "100", size: "0.00000000", wantDepth: 0, wantSize: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ob := NewOrderbook("test", "BTCUSDT")
			ob.ApplyLevel(tt.side, tt.price, tt.size)
			levels := ob.Bids()
			if tt.side == Ask {
				levels = ob.Asks()
			}
			if len(levels) != tt.wantDepth {
				t.Errorf("depth = %d, want %d", len(levels), tt.wantDepth)
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
	if len(ob.Bids()) != 1 {
		t.Errorf("bid depth = %d, want 1", len(ob.Bids()))
	}
}

func TestOrderbookReset(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, "100", "1")
	ob.ApplyLevel(Ask, "101", "1")
	ob.Reset()
	if len(ob.Bids()) != 0 || len(ob.Asks()) != 0 {
		t.Error("Reset should clear both sides")
	}
}

func TestOrderbookPreservesExactStrings(t *testing.T) {
	// A value with more significant digits than float64 can hold must be stored
	// and returned byte-for-byte, which is the whole point of string keys/values.
	ob := NewOrderbook("test", "BTCUSDT")
	const price = "0.000000000000001234"
	const size = "12345.678901234567890"
	ob.ApplyLevel(Bid, price, size)
	if sz, ok := sizeAt(ob.Bids(), price); !ok || sz != size {
		t.Errorf("size at %s = %q ok=%v, want %q (exact string preserved)", price, sz, ok, size)
	}
}

func TestOrderbookCloneIsIndependent(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, "100", "1")
	ob.ApplyLevel(Ask, "101", "1")

	clone := ob.Clone()
	// Mutate the original after cloning.
	ob.ApplyLevel(Bid, "100", "5")
	ob.ApplyLevel(Bid, "99", "2")

	if sz, ok := sizeAt(clone.Bids(), "100"); !ok || sz != "1" {
		t.Errorf("clone size at 100 = %q ok=%v, want 1 (clone must not see later mutation)", sz, ok)
	}
	if len(clone.Bids()) != 1 {
		t.Errorf("clone bid depth = %d, want 1", len(clone.Bids()))
	}
	if clone.Exchange != "test" || clone.Symbol != "BTCUSDT" {
		t.Errorf("clone metadata = %s/%s, want test/BTCUSDT", clone.Exchange, clone.Symbol)
	}
}

func TestNewOrderbookUpdate(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, "100", "1")
	eventTime := time.Unix(1700000000, 0)
	ingestedAt := time.Unix(1700000005, 0)

	upd := NewOrderbookUpdate(ob, true, eventTime, ingestedAt)
	if upd.Book.Exchange != "test" || upd.Book.Symbol != "BTCUSDT" {
		t.Errorf("update book metadata = %s/%s", upd.Book.Exchange, upd.Book.Symbol)
	}
	if !upd.IsSnapshot || upd.Time == nil || !upd.Time.Equal(eventTime) || !upd.IngestedAt.Equal(ingestedAt) {
		t.Errorf("update flags: IsSnapshot=%v Time=%v IngestedAt=%v", upd.IsSnapshot, upd.Time, upd.IngestedAt)
	}
	if upd.Time.Location() != time.UTC || upd.IngestedAt.Location() != time.UTC {
		t.Errorf("times not UTC: Time=%v IngestedAt=%v", upd.Time.Location(), upd.IngestedAt.Location())
	}
	// A zero event time must be left nil, not fabricated from the local clock.
	z := NewOrderbookUpdate(ob, false, time.Time{}, ingestedAt)
	if z.Time != nil {
		t.Errorf("zero eventTime should yield nil Time, got %v", *z.Time)
	}
	if z.IngestedAt.IsZero() {
		t.Error("IngestedAt must always be set")
	}
	// The update's book must be a clone, independent of later mutation.
	ob.ApplyLevel(Bid, "100", "9")
	if sz, ok := sizeAt(upd.Book.Bids(), "100"); !ok || sz != "1" {
		t.Errorf("update book size at 100 = %q ok=%v, want 1 (must be a clone)", sz, ok)
	}
}
