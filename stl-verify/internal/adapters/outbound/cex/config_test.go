package cex

import "testing"

func TestAllExchangesHaveWSConfig(t *testing.T) {
	for name, cfg := range Exchanges {
		if cfg.WebSocketURL == "" {
			t.Errorf("exchange %q has empty WebSocketURL", name)
		}
		if len(cfg.Symbols) == 0 {
			t.Errorf("exchange %q has no symbols", name)
		}
	}
}

func TestSymbolMappingsResolve(t *testing.T) {
	for _, name := range []string{"binance", "bybit", "okx", "kraken", "coinbase"} {
		cfg, ok := Exchanges[name]
		if !ok {
			t.Fatalf("exchange %q not found in config", name)
		}
		pair, ok := cfg.Symbols["BTC"]
		if !ok {
			t.Errorf("exchange %q missing BTC symbol mapping", name)
		}
		if pair == "" {
			t.Errorf("exchange %q has empty BTC trading pair", name)
		}
	}
}

func TestSymbolMappingsNonUniversal(t *testing.T) {
	binance := Exchanges["binance"]
	if _, ok := binance.Symbols["HYPE"]; ok {
		t.Error("HYPE should not be mapped on Binance (not listed on spot)")
	}
	kraken := Exchanges["kraken"]
	if _, ok := kraken.Symbols["HYPE"]; ok {
		t.Error("HYPE should not be mapped on Kraken")
	}
}
