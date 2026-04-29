package cex

import (
	"encoding/json"
	"testing"
)

func TestCoinbaseParser_SubscribeMessage(t *testing.T) {
	p := NewCoinbaseParser()
	msg, err := p.SubscribeMessage([]string{"BTC-USD"}, 0)
	if err != nil {
		t.Fatal(err)
	}

	var sub struct {
		Type       string   `json:"type"`
		ProductIDs []string `json:"product_ids"`
		Channel    string   `json:"channel"`
	}
	if err := json.Unmarshal(msg, &sub); err != nil {
		t.Fatal(err)
	}
	if sub.Type != "subscribe" {
		t.Errorf("expected type=subscribe, got %q", sub.Type)
	}
	if sub.Channel != "level2" {
		t.Errorf("expected channel=level2, got %q", sub.Channel)
	}
	if len(sub.ProductIDs) != 1 || sub.ProductIDs[0] != "BTC-USD" {
		t.Errorf("unexpected product_ids: %v", sub.ProductIDs)
	}
}

func TestCoinbaseParser_ParseSnapshot(t *testing.T) {
	p := NewCoinbaseParser()
	msg := `{
		"channel": "l2_data",
		"events": [{
			"type": "snapshot",
			"product_id": "BTC-USD",
			"updates": [
				{"side": "bid", "price_level": "95000.10", "new_quantity": "1.5"},
				{"side": "bid", "price_level": "94999.00", "new_quantity": "2.0"},
				{"side": "offer", "price_level": "95001.00", "new_quantity": "0.8"},
				{"side": "offer", "price_level": "95002.50", "new_quantity": "1.2"},
				{"side": "bid", "price_level": "94998.00", "new_quantity": "0"}
			]
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
	if snap.Exchange != "coinbase" {
		t.Errorf("expected exchange coinbase, got %q", snap.Exchange)
	}
	if snap.Symbol != "BTC" {
		t.Errorf("expected symbol BTC, got %q", snap.Symbol)
	}
	// Zero-quantity entry should be skipped, so only 2 bids
	if len(snap.Bids) != 2 {
		t.Fatalf("expected 2 bids (zero-qty skipped), got %d", len(snap.Bids))
	}
	if snap.Bids[0].Price != 95000.10 {
		t.Errorf("expected bid price 95000.10, got %f", snap.Bids[0].Price)
	}
	if len(snap.Asks) != 2 {
		t.Fatalf("expected 2 asks, got %d", len(snap.Asks))
	}
	if snap.Asks[0].Price != 95001.00 {
		t.Errorf("expected ask price 95001.00, got %f", snap.Asks[0].Price)
	}
	expectedLiq := snap.Asks[0].Price * snap.Asks[0].Size
	if snap.Asks[0].Liquidity != expectedLiq {
		t.Errorf("expected ask liquidity %f, got %f", expectedLiq, snap.Asks[0].Liquidity)
	}
}
