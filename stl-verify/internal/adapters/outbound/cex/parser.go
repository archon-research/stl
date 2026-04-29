package cex

import "github.com/archon-research/stl/stl-verify/internal/domain/entity"

// Parser converts exchange-specific WebSocket messages into normalized orderbook snapshots.
type Parser interface {
	Exchange() string
	SubscribeMessage(pairs []string, depth int) ([]byte, error)
	ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error)
	PingMessage() []byte
}
