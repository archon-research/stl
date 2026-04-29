package cex

import (
	"encoding/json"
	"testing"
)

func TestOKXParser_SubscribeMessage(t *testing.T) {
	p := NewOKXParser()
	msg, err := p.SubscribeMessage([]string{"BTC-USDT"}, 0)
	if err != nil {
		t.Fatal(err)
	}

	var sub struct {
		Op   string `json:"op"`
		Args []struct {
			Channel string `json:"channel"`
			InstID  string `json:"instId"`
		} `json:"args"`
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
	if sub.Args[0].Channel != "books" {
		t.Errorf("expected channel=books, got %q", sub.Args[0].Channel)
	}
	if sub.Args[0].InstID != "BTC-USDT" {
		t.Errorf("unexpected instId: %q", sub.Args[0].InstID)
	}
}

func TestOKXParser_ParseSnapshot(t *testing.T) {
	p := NewOKXParser()
	msg := `{
		"arg": {"channel": "books", "instId": "BTC-USDT"},
		"action": "snapshot",
		"data": [{
			"bids": [["95000.10", "1.5", "0", "1"], ["94999.00", "2.0", "0", "1"]],
			"asks": [["95001.00", "0.8", "0", "1"], ["95002.50", "1.2", "0", "1"]],
			"ts": "1672304484978"
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
	if snap.Exchange != "okx" {
		t.Errorf("expected exchange okx, got %q", snap.Exchange)
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
}

func TestOKXParser_PongReturnsNil(t *testing.T) {
	p := NewOKXParser()
	snapshots, err := p.ParseMessage([]byte("pong"))
	if err != nil {
		t.Errorf("expected no error on pong, got %v", err)
	}
	if snapshots != nil {
		t.Errorf("expected nil for pong, got %d snapshots", len(snapshots))
	}
}
