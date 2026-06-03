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
		{
			name: "NaN price rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "NaN", Qty: "1.0"},
				},
			},
			wantErr: true,
		},
		{
			name: "Inf price rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Asks: []OrderBookLevel{
					{Price: "Inf", Qty: "0.5"},
				},
			},
			wantErr: true,
		},
		{
			name: "negative price rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "-100.00", Qty: "1.0"},
				},
			},
			wantErr: true,
		},
		{
			name: "zero price rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "0", Qty: "1.0"},
				},
			},
			wantErr: true,
		},
		{
			name: "negative qty rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "100.00", Qty: "-1.0"},
				},
			},
			wantErr: true,
		},
		{
			name: "invalid qty string rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "100.00", Qty: "not-a-number"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty qty string rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "100.00", Qty: ""},
				},
			},
			wantErr: true,
		},
		{
			name: "NaN qty rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Asks: []OrderBookLevel{
					{Price: "100.00", Qty: "NaN"},
				},
			},
			wantErr: true,
		},
		{
			name: "zero qty is valid (resting at zero)",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "100.00", Qty: "0"},
				},
			},
		},
		{
			name: "crossed book rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "101.50", Qty: "1.0"},
				},
				Asks: []OrderBookLevel{
					{Price: "100.00", Qty: "0.8"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty price string rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "", Qty: "1.0"},
				},
			},
			wantErr: true,
		},
		{
			// Pins the crossed-book check at >= : best bid == best ask is rejected.
			name: "equal best bid and ask rejected",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids:       []OrderBookLevel{{Price: "100.00", Qty: "1.0"}},
				Asks:       []OrderBookLevel{{Price: "100.00", Qty: "0.8"}},
			},
			wantErr: true,
		},
		{
			// Documents that equal adjacent prices on one side are allowed
			// (e.g. two aggregated levels reported at the same price).
			name: "equal adjacent bid prices allowed",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Bids: []OrderBookLevel{
					{Price: "100.00", Qty: "1.0"},
					{Price: "100.00", Qty: "0.5"},
				},
			},
		},
		{
			name: "equal adjacent ask prices allowed",
			snap: OrderBookSnapshot{
				Exchange:   "binance",
				Token:      "BTCUSDT",
				CapturedAt: now,
				Asks: []OrderBookLevel{
					{Price: "101.00", Qty: "1.0"},
					{Price: "101.00", Qty: "0.5"},
				},
			},
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
