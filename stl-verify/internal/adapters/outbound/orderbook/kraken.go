package orderbook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	exchangeKraken = "kraken"
	// krakenWSBase is the Kraken v1 public WebSocket.
	krakenWSBase = "wss://ws.kraken.com"
	// krakenDepth is the requested book depth.
	krakenDepth      = 500
	krakenMaxSymbols = 50
	krakenPingPeriod = 20 * time.Second
)

// Compile-time check that KrakenProvider implements outbound.OrderbookProvider.
var _ outbound.OrderbookProvider = (*KrakenProvider)(nil)

// KrakenProvider streams L2 books from Kraken (WebSocket v1). The book channel
// pushes a snapshot frame (as/bs) followed by update frames (a/b); a zero
// volume removes a level.
//
// Integrity note: Kraken v1 carries no per-message sequence numbers — its only
// in-band integrity signal is the CRC32 "c" checksum, which this version does
// not validate. Because frames arrive in order over a single TCP stream, the
// only way to miss updates is a disconnect, which is detected and triggers a
// fresh snapshot on reconnect. Checksum validation can be layered into the
// handler later without changing the provider interface.
type KrakenProvider struct {
	*wsSnapshotProvider
}

// NewKrakenProvider creates a Kraken orderbook provider.
func NewKrakenProvider(cfg Config) *KrakenProvider {
	ex := &krakenExchange{wsBase: krakenWSBase}
	return &KrakenProvider{
		wsSnapshotProvider: newWSSnapshotProvider(cfg, ex, krakenMaxSymbols),
	}
}

type krakenExchange struct {
	wsBase string
}

func (e *krakenExchange) name() string             { return exchangeKraken }
func (e *krakenExchange) endpoint([]string) string { return e.wsBase }
func (e *krakenExchange) newHandler() frameHandler {
	return &krakenHandler{books: newBookSet(exchangeKraken)}
}

// appPing satisfies appPinger with Kraken's JSON ping event.
func (e *krakenExchange) appPing() ([]byte, time.Duration) {
	return []byte(`{"event":"ping"}`), krakenPingPeriod
}

func (e *krakenExchange) subscribeMessages(group []string) ([]any, error) {
	return []any{map[string]any{
		"event":        "subscribe",
		"pair":         group,
		"subscription": map[string]any{"name": "book", "depth": krakenDepth},
	}}, nil
}

type krakenHandler struct {
	books *bookSet
}

// krakenBookData is one data object in a book frame. Snapshots use as/bs;
// updates use a/b. Level entries are [price, volume, timestamp, (republish)];
// only price and volume are read.
type krakenBookData struct {
	As [][]string `json:"as"`
	Bs [][]string `json:"bs"`
	A  [][]string `json:"a"`
	B  [][]string `json:"b"`
}

// handle dispatches between Kraken's two frame shapes: JSON objects are control
// events; JSON arrays are book data.
func (h *krakenHandler) handle(raw []byte) ([]emitSignal, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil, nil
	}
	switch trimmed[0] {
	case '{':
		return h.handleEvent(trimmed)
	case '[':
		return h.handleBook(trimmed)
	default:
		return nil, nil
	}
}

func (h *krakenHandler) handleEvent(raw []byte) ([]emitSignal, error) {
	var ev struct {
		Event        string `json:"event"`
		Status       string `json:"status"`
		ErrorMessage string `json:"errorMessage"`
		Pair         string `json:"pair"`
	}
	if err := json.Unmarshal(raw, &ev); err != nil {
		return nil, fmt.Errorf("decode kraken event: %w", err)
	}
	if ev.Event == "subscriptionStatus" && ev.Status == "error" {
		return nil, fmt.Errorf("kraken subscription error for %s: %s", ev.Pair, ev.ErrorMessage)
	}
	// systemStatus, heartbeat, pong, and successful subscriptionStatus carry no
	// book data.
	return nil, nil
}

// handleBook parses a book array frame of the form
//
//	[channelID, data, ("book-N"), pair]            // snapshot or single-side update
//	[channelID, dataA, dataB, ("book-N"), pair]    // combined two-side update
//
// The first element is the channel id, the last is the pair, the penultimate is
// the channel name, and the element(s) in between are book-data objects.
func (h *krakenHandler) handleBook(raw []byte) ([]emitSignal, error) {
	var parts []json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return nil, fmt.Errorf("decode kraken book frame: %w", err)
	}
	if len(parts) < 4 {
		return nil, fmt.Errorf("kraken book frame has %d parts, want >= 4", len(parts))
	}

	var pair string
	if err := json.Unmarshal(parts[len(parts)-1], &pair); err != nil {
		return nil, fmt.Errorf("decode kraken pair: %w", err)
	}
	book := h.books.get(pair)

	isSnapshot := false
	applied := false
	// Data objects sit between the channel id (index 0) and the trailing channel
	// name + pair (last two elements). Kraken v1 has no sequence numbers, so a
	// data object we fail to decode would be silently lost with nothing to detect
	// it later — treat a decode failure here as fatal so the connection reconnects
	// and re-snapshots rather than serving a quietly stale book.
	for i := 1; i <= len(parts)-3; i++ {
		var d krakenBookData
		if err := json.Unmarshal(parts[i], &d); err != nil {
			return nil, fmt.Errorf("decode kraken data object at index %d: %w", i, err)
		}
		didApply, snap, err := applyKrakenData(book, d)
		if err != nil {
			return nil, err
		}
		applied = applied || didApply
		isSnapshot = isSnapshot || snap
	}

	if !applied {
		return nil, nil
	}
	return []emitSignal{{book: book, isSnapshot: isSnapshot, t: time.Now()}}, nil
}

// applyKrakenData applies one data object, reporting whether it changed the book
// and whether it was a snapshot.
func applyKrakenData(book *entity.Orderbook, d krakenBookData) (applied, snapshot bool, err error) {
	if len(d.As) > 0 || len(d.Bs) > 0 {
		book.Reset()
		// After Reset the book is empty, so applying snapshot levels as deltas is
		// equivalent to setting them.
		if err := applyDeltaLevels(book, entity.Ask, d.As); err != nil {
			return false, false, fmt.Errorf("kraken snapshot asks: %w", err)
		}
		if err := applyDeltaLevels(book, entity.Bid, d.Bs); err != nil {
			return false, false, fmt.Errorf("kraken snapshot bids: %w", err)
		}
		return true, true, nil
	}
	if len(d.A) > 0 || len(d.B) > 0 {
		if err := applyDeltaLevels(book, entity.Ask, d.A); err != nil {
			return false, false, fmt.Errorf("kraken asks: %w", err)
		}
		if err := applyDeltaLevels(book, entity.Bid, d.B); err != nil {
			return false, false, fmt.Errorf("kraken bids: %w", err)
		}
		return true, false, nil
	}
	return false, false, nil // e.g. checksum-only object
}
