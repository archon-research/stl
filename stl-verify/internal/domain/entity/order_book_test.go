package entity

import (
	"testing"
	"time"
)

func TestOrderBookSnapshot_Validate(t *testing.T) {
	validBids := []OrderBookLevel{
		{Price: "100.00", Qty: "1.0"},
		{Price: "99.50", Qty: "0.5"},
	}
	validAsks := []OrderBookLevel{
		{Price: "101.00", Qty: "0.8"},
		{Price: "101.50", Qty: "1.1"},
	}
	now := time.Now()

	tests := []struct {
		name    string
		snap    OrderBookSnapshot
		wantErr bool
	}{
		{
			name: "valid snapshot",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids:       validBids,
				Asks:       validAsks,
			},
		},
		{
			name: "empty bids and asks are valid",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
			},
		},
		{
			name:    "missing exchange",
			snap:    OrderBookSnapshot{Token: "BTCUSDT", CapturedAt: now},
			wantErr: true,
		},
		{
			name:    "missing token",
			snap:    OrderBookSnapshot{Exchange: "binance", CapturedAt: now},
			wantErr: true,
		},
		{
			name:    "zero captured_at",
			snap:    OrderBookSnapshot{Exchange: "binance", Token: "BTCUSDT"},
			wantErr: true,
		},
		{
			name: "unsorted bids ascending instead of descending",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "99.00", Qty: "1.0"},
					{Price: "100.00", Qty: "0.5"},
				},
				Asks: validAsks,
			},
			wantErr: true,
		},
		{
			name: "unsorted asks descending instead of ascending",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids:       validBids,
				Asks: []OrderBookLevel{
					{Price: "101.50", Qty: "1.1"},
					{Price: "101.00", Qty: "0.8"},
				},
			},
			wantErr: true,
		},
		{
			name: "single bid and ask are valid",
			snap: OrderBookSnapshot{
				Exchange:   "coinbase",
				Token:      "BTC-USD",
				CapturedAt: now,
				Bids:       []OrderBookLevel{{Price: "50000.00", Qty: "0.1"}},
				Asks:       []OrderBookLevel{{Price: "50001.00", Qty: "0.2"}},
			},
		},
		{
			name: "invalid price string in bids",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "not-a-number", Qty: "1.0"},
					{Price: "99.00", Qty: "0.5"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.snap.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
