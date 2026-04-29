// Package cex provides adapters for centralized exchange orderbook data.
package cex

import "time"

// ExchangeConfig holds all configuration for connecting to a single exchange.
type ExchangeConfig struct {
	Name                string
	DisplayName         string
	WebSocketURL        string
	RESTBaseURL         string
	Symbols             map[string]string // normalized symbol -> exchange pair
	MaxDepth            int
	PingInterval        time.Duration
	PongTimeout         time.Duration
	ReconnectBackoff    time.Duration
	MaxReconnectBackoff time.Duration
}

// DefaultTimings returns common WebSocket timing defaults.
func DefaultTimings() (pingInterval, pongTimeout, reconnectBackoff, maxReconnectBackoff time.Duration) {
	return 20 * time.Second, 10 * time.Second, 1 * time.Second, 60 * time.Second
}

var Exchanges = map[string]*ExchangeConfig{
	"binance": {
		Name:         "binance",
		DisplayName:  "Binance",
		WebSocketURL: "wss://stream.binance.com:9443/ws",
		RESTBaseURL:  "https://api.binance.com",
		Symbols: map[string]string{
			"BTC":    "btcusdt",
			"ETH":    "ethusdt",
			"XRP":    "xrpusdt",
			"WBTC":   "wbtcusdt",
			"WSTETH": "wstethusdt",
		},
		MaxDepth:            20,
		PingInterval:        20 * time.Second,
		PongTimeout:         10 * time.Second,
		ReconnectBackoff:    1 * time.Second,
		MaxReconnectBackoff: 60 * time.Second,
	},
	"bybit": {
		Name:         "bybit",
		DisplayName:  "Bybit",
		WebSocketURL: "wss://stream.bybit.com/v5/public/spot",
		RESTBaseURL:  "https://api.bybit.com",
		Symbols: map[string]string{
			"BTC":    "BTCUSDT",
			"ETH":    "ETHUSDT",
			"XRP":    "XRPUSDT",
			"WBTC":   "WBTCUSDT",
			"HYPE":   "HYPEUSDT",
			"WSTETH": "WSTETHUSDT",
			"CBBTC":  "CBBTCUSDT",
			"LBTC":   "LBTCUSDT",
			"WEETH":  "WEETHUSDT",
		},
		MaxDepth:            200,
		PingInterval:        20 * time.Second,
		PongTimeout:         10 * time.Second,
		ReconnectBackoff:    1 * time.Second,
		MaxReconnectBackoff: 60 * time.Second,
	},
	"okx": {
		Name:         "okx",
		DisplayName:  "OKX",
		WebSocketURL: "wss://ws.okx.com:8443/ws/v5/public",
		RESTBaseURL:  "https://www.okx.com",
		Symbols: map[string]string{
			"BTC":   "BTC-USDT",
			"ETH":   "ETH-USDT",
			"XRP":   "XRP-USDT",
			"WBTC":  "WBTC-USDT",
			"HYPE":  "HYPE-USDT",
			"CBBTC": "CBBTC-USDT",
		},
		MaxDepth:            400,
		PingInterval:        20 * time.Second,
		PongTimeout:         10 * time.Second,
		ReconnectBackoff:    1 * time.Second,
		MaxReconnectBackoff: 60 * time.Second,
	},
	"kraken": {
		Name:         "kraken",
		DisplayName:  "Kraken",
		WebSocketURL: "wss://ws.kraken.com/v2",
		RESTBaseURL:  "https://api.kraken.com",
		Symbols: map[string]string{
			"BTC":  "BTC/USD",
			"ETH":  "ETH/USD",
			"XRP":  "XRP/USD",
			"WBTC": "WBTC/USD",
		},
		MaxDepth:            100,
		PingInterval:        30 * time.Second,
		PongTimeout:         10 * time.Second,
		ReconnectBackoff:    1 * time.Second,
		MaxReconnectBackoff: 60 * time.Second,
	},
	"coinbase": {
		Name:         "coinbase",
		DisplayName:  "Coinbase",
		WebSocketURL: "wss://advanced-trade-ws.coinbase.com",
		RESTBaseURL:  "https://api.exchange.coinbase.com",
		Symbols: map[string]string{
			"BTC":   "BTC-USD",
			"ETH":   "ETH-USD",
			"XRP":   "XRP-USD",
			"WBTC":  "WBTC-USD",
			"HYPE":  "HYPE-USD",
			"CBBTC": "CBBTC-USD",
		},
		MaxDepth:            0,
		PingInterval:        30 * time.Second,
		PongTimeout:         10 * time.Second,
		ReconnectBackoff:    1 * time.Second,
		MaxReconnectBackoff: 60 * time.Second,
	},
}

// AllSymbols returns the deduplicated set of all normalized symbols across all exchanges.
func AllSymbols() []string {
	seen := make(map[string]bool)
	var result []string
	for _, cfg := range Exchanges {
		for sym := range cfg.Symbols {
			if !seen[sym] {
				seen[sym] = true
				result = append(result, sym)
			}
		}
	}
	return result
}

// ExchangesForSymbol returns the names of exchanges that list a given symbol.
func ExchangesForSymbol(symbol string) []string {
	var result []string
	for name, cfg := range Exchanges {
		if _, ok := cfg.Symbols[symbol]; ok {
			result = append(result, name)
		}
	}
	return result
}
