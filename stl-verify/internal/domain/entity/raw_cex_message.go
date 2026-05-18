package entity

import "time"

// RawCEXMessage is the transport envelope for a raw WebSocket frame from a
// centralized exchange. The watcher publishes these; the indexer worker parses
// the payload using the source-appropriate parser.
//
// Payload is the unparsed WS frame as received. Go's json encoder marshals
// []byte as base64, which keeps the envelope binary-safe regardless of what
// the exchange sends.
type RawCEXMessage struct {
	Source     string    `json:"source"`
	CapturedAt time.Time `json:"captured_at"`
	Payload    []byte    `json:"payload"`
}
