package cex_test

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func TestNewExchangeOrderBookStreamer(t *testing.T) {
	tests := []struct {
		name     string
		exchange string
		wantName string
		wantErr  bool
	}{
		{name: "binance lowercase", exchange: "binance", wantName: "binance"},
		{name: "binance uppercase", exchange: "BINANCE", wantName: "binance"},
		{name: "binance mixed case", exchange: "Binance", wantName: "binance"},
		{name: "coinbase lowercase", exchange: "coinbase", wantName: "coinbase"},
		{name: "coinbase with spaces", exchange: " coinbase ", wantName: "coinbase"},
		{name: "unsupported exchange", exchange: "kraken", wantErr: true},
		{name: "empty exchange", exchange: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			streamer, err := cex.NewExchangeOrderBookStreamer(tt.exchange, cex.Config{})
			if (err != nil) != tt.wantErr {
				t.Errorf("NewExchangeOrderBookStreamer(%q) error = %v, wantErr %v", tt.exchange, err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if streamer == nil {
				t.Fatal("expected non-nil streamer")
			}
			if got := streamer.Name(); got != tt.wantName {
				t.Errorf("Name() = %q, want %q", got, tt.wantName)
			}
		})
	}
}

func TestNewExchangeOrderBookStreamer_UnsupportedError_MentionsSupportedExchanges(t *testing.T) {
	_, err := cex.NewExchangeOrderBookStreamer("kraken", cex.Config{})
	if err == nil {
		t.Fatal("want error for unsupported exchange, got nil")
	}
	msg := err.Error()
	for _, ex := range cex.SupportedExchanges() {
		if !strings.Contains(msg, ex) {
			t.Errorf("error %q should mention supported exchange %q", msg, ex)
		}
	}
}

func TestSupportedExchanges_Sorted(t *testing.T) {
	exchanges := cex.SupportedExchanges()
	if len(exchanges) == 0 {
		t.Fatal("SupportedExchanges() returned empty list")
	}
	for i := 1; i < len(exchanges); i++ {
		if exchanges[i-1] >= exchanges[i] {
			t.Errorf("SupportedExchanges() not sorted at index %d: %v", i, exchanges)
		}
	}
}

func TestNewExchangeOrderBookStreamer_SatisfiesInterface(t *testing.T) {
	streamer, err := cex.NewExchangeOrderBookStreamer("binance", cex.Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var _ outbound.ExchangeOrderBookStreamer = streamer
}
