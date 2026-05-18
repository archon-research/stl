package cex

import "github.com/archon-research/stl/stl-verify/internal/domain/entity"

// WSProtocol owns the connection-protocol concerns for a single exchange:
// what subscribe frame to send, and how to keep the connection alive. It does
// NOT parse incoming messages. This is the only CEX-specific interface the
// watcher binary needs.
type WSProtocol interface {
	// Exchange returns the canonical exchange identifier (e.g., "binance").
	Exchange() string
	// SubscribeMessage builds the WS frame that subscribes to the given pairs.
	SubscribeMessage(pairs []string, depth int) ([]byte, error)
	// PingMessage returns the keepalive frame to send on the ping interval,
	// or nil to use a standard WebSocket control ping.
	PingMessage() []byte
}

// Parser converts exchange-specific WebSocket payloads into normalized
// orderbook snapshots. Only the indexer worker uses this — the watcher
// forwards raw payloads without parsing.
type Parser interface {
	// Exchange returns the canonical exchange identifier (e.g., "binance").
	Exchange() string
	// ParseMessage decodes a single raw WS frame. Returns (nil, nil) for
	// frames that are not orderbook data (heartbeats, sub-acks, etc).
	ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error)
}

// ProtocolForExchange returns the WSProtocol implementation for the given
// exchange name, or nil if unknown. Used by the watcher binary which only
// knows the exchange name from configuration.
func ProtocolForExchange(name string) WSProtocol {
	return parserForExchange(name)
}

// ParserForExchange returns the Parser implementation for the given exchange
// name, or nil if unknown. Used by the indexer worker to dispatch incoming
// messages to the right decoder.
func ParserForExchange(name string) Parser {
	return parserForExchange(name)
}

// parserForExchange is the single source of truth for the per-exchange
// implementation registry. The same struct satisfies both WSProtocol and
// Parser, so this is shared.
func parserForExchange(name string) interface {
	WSProtocol
	Parser
} {
	switch name {
	case "binance":
		return NewBinanceParser()
	case "bybit":
		return NewBybitParser()
	case "okx":
		return NewOKXParser()
	case "kraken":
		return NewKrakenParser()
	case "coinbase":
		return NewCoinbaseParser()
	default:
		return nil
	}
}

// AllParsers returns a Parser for every configured exchange, keyed by
// exchange name. The indexer worker uses this to build its dispatch table.
func AllParsers() map[string]Parser {
	parsers := make(map[string]Parser, len(Exchanges))
	for name := range Exchanges {
		if p := ParserForExchange(name); p != nil {
			parsers[name] = p
		}
	}
	return parsers
}
