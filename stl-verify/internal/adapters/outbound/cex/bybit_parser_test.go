package cex

import (
	"encoding/json"
	"testing"
)

func TestBybitParser_SubscribeMessage(t *testing.T) {
	p := NewBybitParser()
	msg, err := p.SubscribeMessage([]string{"BTCUSDT"}, 200)
	if err != nil {
		t.Fatal(err)
	}

	var sub struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}
	if err := json.Unmarshal(msg, &sub); err != nil {
		t.Fatal(err)
	}
	if sub.Op != "subscribe" {
		t.Errorf("expected op=subscribe, got %q", sub.Op)
	}
	if len(sub.Args) != 1 {
		t.Fatalf("expected 1 arg, got %d", len(sub.Args))
	}
	if sub.Args[0] != "orderbook.200.BTCUSDT" {
		t.Errorf("unexpected arg: %q", sub.Args[0])
	}
}

func TestBybitParser_ParseSnapshot(t *testing.T) {
	p := NewBybitParser()
	msg := `{
		"topic": "orderbook.200.BTCUSDT",
		"type": "snapshot",
		"ts": 1672304484978,
		"data": {
			"s": "BTCUSDT",
			"b": [["95000.10", "1.5"], ["94999.00", "2.0"]],
			"a": [["95001.00", "0.8"], ["95002.50", "1.2"]]
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
	if snap.Exchange != "bybit" {
		t.Errorf("expected exchange bybit, got %q", snap.Exchange)
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
	if len(snap.Asks) != 2 {
		t.Fatalf("expected 2 asks, got %d", len(snap.Asks))
	}
	expectedLiq := snap.Bids[0].Price * snap.Bids[0].Size
	if snap.Bids[0].Liquidity != expectedLiq {
		t.Errorf("expected bid liquidity %f, got %f", expectedLiq, snap.Bids[0].Liquidity)
	}
}

func TestBybitParser_IgnoresNonOrderbookMessages(t *testing.T) {
	p := NewBybitParser()
	msg := `{"success": true, "op": "subscribe"}`
	snapshots, err := p.ParseMessage([]byte(msg))
	if err != nil {
		t.Fatal(err)
	}
	if snapshots != nil {
		t.Errorf("expected nil for non-orderbook message, got %d snapshots", len(snapshots))
	}
}
