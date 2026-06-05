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

func TestOrderbookApplyLevel(t *testing.T) {
	tests := []struct {
		name      string
		side      Side
		price     float64
		size      float64
		wantDepth int
		wantSize  float64 // expected size at price after apply (0 == absent)
	}{
		{name: "insert bid", side: Bid, price: 100, size: 2, wantDepth: 1, wantSize: 2},
		{name: "insert ask", side: Ask, price: 101, size: 3, wantDepth: 1, wantSize: 3},
		{name: "zero size removes", side: Bid, price: 100, size: 0, wantDepth: 0, wantSize: 0},
		{name: "negative size removes", side: Bid, price: 100, size: -1, wantDepth: 0, wantSize: 0},
		{name: "non-positive price ignored", side: Bid, price: 0, size: 5, wantDepth: 0, wantSize: 0},
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
			var gotSize float64
			for _, l := range levels {
				if l.Price == tt.price {
					gotSize = l.Size
				}
			}
			if gotSize != tt.wantSize {
				t.Errorf("size at %v = %v, want %v", tt.price, gotSize, tt.wantSize)
			}
		})
	}
}

func TestOrderbookApplyLevelUpdatesAbsolute(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, 100, 5)
	ob.ApplyLevel(Bid, 100, 9) // absolute replace, not additive
	bb, ok := ob.BestBid()
	if !ok || bb.Size != 9 {
		t.Fatalf("BestBid = %+v ok=%v, want size 9", bb, ok)
	}
	if ob.Depth(Bid) != 1 {
		t.Errorf("Depth(Bid) = %d, want 1", ob.Depth(Bid))
	}
}

func TestOrderbookIgnoresNonFinite(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	inf := 1.0 / zero()
	nan := inf - inf
	ob.ApplyLevel(Bid, nan, 1)
	ob.ApplyLevel(Bid, inf, 1)
	ob.ApplyLevel(Bid, 100, nan) // non-finite size removes (no-op on empty)
	if ob.Depth(Bid) != 0 {
		t.Errorf("Depth(Bid) = %d, want 0 (non-finite inputs ignored)", ob.Depth(Bid))
	}
}

// zero returns 0.0 without a constant divide-by-zero the compiler would reject.
func zero() float64 { return float64(len("")) }

func TestOrderbookSorting(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	for _, p := range []float64{100, 102, 101} {
		ob.ApplyLevel(Bid, p, 1)
	}
	for _, p := range []float64{200, 198, 199} {
		ob.ApplyLevel(Ask, p, 1)
	}

	bids := ob.Bids()
	wantBids := []float64{102, 101, 100} // descending
	for i, w := range wantBids {
		if bids[i].Price != w {
			t.Errorf("bids[%d].Price = %v, want %v", i, bids[i].Price, w)
		}
	}
	asks := ob.Asks()
	wantAsks := []float64{198, 199, 200} // ascending
	for i, w := range wantAsks {
		if asks[i].Price != w {
			t.Errorf("asks[%d].Price = %v, want %v", i, asks[i].Price, w)
		}
	}
}

func TestOrderbookBestEmpty(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	if _, ok := ob.BestBid(); ok {
		t.Error("BestBid on empty book should be ok=false")
	}
	if _, ok := ob.BestAsk(); ok {
		t.Error("BestAsk on empty book should be ok=false")
	}
}

func TestOrderbookReplaceSide(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, 1, 1) // should be cleared by ReplaceSide
	ob.ReplaceSide(Bid, []PriceLevel{{Price: 100, Size: 2}, {Price: 99, Size: 3}})
	if ob.Depth(Bid) != 2 {
		t.Fatalf("Depth(Bid) = %d, want 2", ob.Depth(Bid))
	}
	bb, _ := ob.BestBid()
	if bb.Price != 100 || bb.Size != 2 {
		t.Errorf("BestBid = %+v, want {100 2}", bb)
	}
}

func TestOrderbookCrossed(t *testing.T) {
	tests := []struct {
		name string
		bid  float64
		ask  float64
		set  bool // set both sides
		want bool
	}{
		{name: "normal", bid: 100, ask: 101, set: true, want: false},
		{name: "crossed", bid: 102, ask: 101, set: true, want: true},
		{name: "locked is not crossed", bid: 101, ask: 101, set: true, want: false},
		{name: "empty", set: false, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ob := NewOrderbook("test", "BTCUSDT")
			if tt.set {
				ob.ApplyLevel(Bid, tt.bid, 1)
				ob.ApplyLevel(Ask, tt.ask, 1)
			}
			if got := ob.Crossed(); got != tt.want {
				t.Errorf("Crossed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOrderbookReset(t *testing.T) {
	ob := NewOrderbook("test", "BTCUSDT")
	ob.ApplyLevel(Bid, 100, 1)
	ob.ApplyLevel(Ask, 101, 1)
	ob.LastUpdateID = 42
	ob.Reset()
	if ob.Depth(Bid) != 0 || ob.Depth(Ask) != 0 {
		t.Error("Reset should clear both sides")
	}
	if ob.LastUpdateID != 0 {
		t.Errorf("Reset should zero LastUpdateID, got %d", ob.LastUpdateID)
	}
}

func TestOrderbookCloneIsIndependent(t *testing.T) {
	ob := NewOrderbook("binance", "BTCUSDT")
	ob.ApplyLevel(Bid, 100, 1)
	ob.ApplyLevel(Ask, 101, 1)
	ob.LastUpdateID = 7

	clone := ob.Clone()
	// Mutate the original after cloning.
	ob.ApplyLevel(Bid, 100, 5)
	ob.ApplyLevel(Bid, 99, 2)
	ob.LastUpdateID = 8

	cb, _ := clone.BestBid()
	if cb.Size != 1 {
		t.Errorf("clone bid size = %v, want 1 (clone must not see later mutation)", cb.Size)
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
	ob.ApplyLevel(Bid, 100, 1)
	now := time.Unix(1700000000, 0)

	upd := NewOrderbookUpdate(ob, true, now)
	if upd.Exchange != "binance" || upd.Symbol != "BTCUSDT" {
		t.Errorf("update metadata = %s/%s", upd.Exchange, upd.Symbol)
	}
	if !upd.IsSnapshot || !upd.Time.Equal(now) {
		t.Errorf("update flags: IsSnapshot=%v Time=%v", upd.IsSnapshot, upd.Time)
	}
	// The update's book must be a clone, independent of later mutation.
	ob.ApplyLevel(Bid, 100, 9)
	bb, _ := upd.Book.BestBid()
	if bb.Size != 1 {
		t.Errorf("update book bid size = %v, want 1 (must be a clone)", bb.Size)
	}
}
