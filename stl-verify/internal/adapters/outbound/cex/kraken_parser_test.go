package cex

import (
	"encoding/json"
	"testing"
)

func TestKrakenParser_SubscribeMessage(t *testing.T) {
	p := NewKrakenParser()
	msg, err := p.SubscribeMessage([]string{"BTC/USD"}, 100)
	if err != nil {
		t.Fatal(err)
	}

	var sub struct {
		Method string `json:"method"`
		Params struct {
			Channel string   `json:"channel"`
			Symbol  []string `json:"symbol"`
			Depth   int      `json:"depth"`
		} `json:"params"`
	}
	if err := json.Unmarshal(msg, &sub); err != nil {
		t.Fatal(err)
	}
	if sub.Method != "subscribe" {
		t.Errorf("expected method=subscribe, got %q", sub.Method)
	}
	if sub.Params.Channel != "book" {
		t.Errorf("expected channel=book, got %q", sub.Params.Channel)
	}
	if sub.Params.Depth != 100 {
		t.Errorf("expected depth=100, got %d", sub.Params.Depth)
	}
	if len(sub.Params.Symbol) != 1 || sub.Params.Symbol[0] != "BTC/USD" {
		t.Errorf("unexpected symbol: %v", sub.Params.Symbol)
	}
}

func TestKrakenParser_ParseSnapshot(t *testing.T) {
	p := NewKrakenParser()
	msg := `{
		"channel": "book",
		"type": "snapshot",
		"data": [{
			"symbol": "BTC/USD",
			"bids": [{"price": 95000.0, "qty": 1.5}, {"price": 94999.0, "qty": 2.0}],
			"asks": [{"price": 95001.0, "qty": 0.8}, {"price": 95002.5, "qty": 1.2}],
			"timestamp": "2024-01-01T00:00:00.000Z"
		}]
	}`
	snapshots, err := p.ParseMessage([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 1 {
		t.Fatalf("expected 1 snapshot, got %d", len(snapshots))
	}

	snap := snapshots[0]
	if snap.Exchange != "kraken" {
		t.Errorf("expected exchange kraken, got %q", snap.Exchange)
	}
	if snap.Symbol != "BTC" {
		t.Errorf("expected symbol BTC, got %q", snap.Symbol)
	}
	if len(snap.Bids) != 2 {
		t.Fatalf("expected 2 bids, got %d", len(snap.Bids))
	}
	if snap.Bids[0].Price != 95000.0 {
		t.Errorf("expected bid price 95000.0, got %f", snap.Bids[0].Price)
	}
	if snap.Bids[0].Size != 1.5 {
		t.Errorf("expected bid size 1.5, got %f", snap.Bids[0].Size)
	}
	expectedLiq := 95000.0 * 1.5
	if snap.Bids[0].Liquidity != expectedLiq {
		t.Errorf("expected bid liquidity %f, got %f", expectedLiq, snap.Bids[0].Liquidity)
	}
	if len(snap.Asks) != 2 {
		t.Fatalf("expected 2 asks, got %d", len(snap.Asks))
	}
}
