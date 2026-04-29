package cex

import (
	"encoding/json"
	"testing"
)

func TestBinanceParser_SubscribeMessage(t *testing.T) {
	p := NewBinanceParser()
	msg, err := p.SubscribeMessage([]string{"btcusdt", "ethusdt"}, 20)
	if err != nil {
		t.Fatal(err)
	}

	var sub struct {
		Method string   `json:"method"`
		Params []string `json:"params"`
		ID     int      `json:"id"`
	}
	if err := json.Unmarshal(msg, &sub); err != nil {
		t.Fatal(err)
	}
	if sub.Method != "SUBSCRIBE" {
		t.Errorf("expected SUBSCRIBE, got %q", sub.Method)
	}
	if len(sub.Params) != 2 {
		t.Fatalf("expected 2 params, got %d", len(sub.Params))
	}
	if sub.Params[0] != "btcusdt@depth20@100ms" {
		t.Errorf("unexpected param: %q", sub.Params[0])
	}
}

func TestBinanceParser_ParsePartialDepth(t *testing.T) {
	p := NewBinanceParser()
	msg := `{
		"stream": "btcusdt@depth20@100ms",
		"data": {
			"lastUpdateId": 160,
			"bids": [["95000.10", "1.5"], ["94999.00", "2.0"]],
			"asks": [["95001.00", "0.8"], ["95002.50", "1.2"]]
		}
	}`
	snapshots, err := p.ParseMessage([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}

	snap := snapshots[0]
	if snap.Exchange != "binance" {
		t.Errorf("expected exchange binance, got %q", snap.Exchange)
	}
	if snap.Symbol != "BTC" {
		t.Errorf("expected symbol BTC, got %q", snap.Symbol)
	}
	if len(snap.Bids) != 2 {
		t.Fatalf("expected 2 bids, got %d", len(snap.Bids))
	}
	if snap.Bids[0].Price != 95000.10 {
		t.Errorf("expected bid price 95000.10, got %f", snap.Bids[0].Price)
	}
	expectedLiq := snap.Bids[0].Price * snap.Bids[0].Size
	if snap.Bids[0].Liquidity != expectedLiq {
		t.Errorf("expected bid liquidity %f, got %f", expectedLiq, snap.Bids[0].Liquidity)
	}
	if len(snap.Asks) != 2 {
		t.Fatalf("expected 2 asks, got %d", len(snap.Asks))
	}
}

func TestBinanceParser_IgnoresNonOrderbookMessages(t *testing.T) {
	p := NewBinanceParser()
	msg := `{"result": null, "id": 1}`
	snapshots, err := p.ParseMessage([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if snapshots != nil {
		t.Errorf("expected nil for non-orderbook message, got %d snapshots", len(snapshots))
	}
}
