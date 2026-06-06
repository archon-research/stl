package orderbook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
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
// Integrity: Kraken v1 carries no per-message sequence numbers, so the CRC32 "c"
// checksum over the top 10 levels is the only in-band way to detect a divergent
// book. Each update is validated against it (see verifyKrakenChecksum); a
// mismatch forces a reconnect and fresh snapshot. The feed is also fixed-depth
// top-N: an insert that brings a better price into range pushes the outermost
// level out of scope, and Kraken sends no explicit delete for that pushed-out
// level, so the book is trimmed to krakenDepth after every frame.
//
// Docs: https://docs.kraken.com/api/docs/guides/spot-ws-book-v1/ and
// https://support.kraken.com/articles/360027821131
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
	C  string     `json:"c"` // CRC32 checksum, present on update frames
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
	checksum := ""
	// Data objects sit between the channel id (index 0) and the trailing channel
	// name + pair (last two elements). Kraken v1 has no sequence numbers, so a
	// data object we fail to decode would be silently lost with nothing to detect
	// it later: treat a decode failure here as fatal so the connection reconnects
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
		if d.C != "" {
			checksum = d.C
		}
	}

	if !applied {
		return nil, nil
	}

	// Kraken's book is fixed-depth top-N: an insert that brings a better price into
	// range pushes the outermost level out of scope with no explicit delete for it,
	// so trim the tail to keep the book at the subscribed depth.
	trimKrakenSide(book, entity.Bid, krakenDepth)
	trimKrakenSide(book, entity.Ask, krakenDepth)

	// The CRC32 over the top 10 levels is the only divergence signal Kraken v1
	// gives; a mismatch means the local book has drifted, so force a re-sync.
	if checksum != "" {
		if err := verifyKrakenChecksum(book, checksum); err != nil {
			return nil, err
		}
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

// trimKrakenSide drops levels beyond depth, keeping the best-priced ones (highest
// bids, lowest asks). Kraken pushes the outermost level out of scope when an
// insert brings a better price into range, with no explicit delete for the
// pushed-out level, so without trimming the tail would grow unbounded.
func trimKrakenSide(book *entity.Orderbook, side entity.Side, depth int) {
	levels := book.Bids()
	ascending := false // bids: keep the highest prices
	if side == entity.Ask {
		levels = book.Asks()
		ascending = true // asks: keep the lowest prices
	}
	if len(levels) <= depth {
		return
	}
	sorted := krakenSortByPrice(levels, ascending)
	for _, lvl := range sorted[depth:] {
		book.ApplyLevel(side, lvl.Price, "0") // zero size removes the level
	}
}

// verifyKrakenChecksum recomputes the CRC32 over the top 10 levels and compares
// it to Kraken's checksum field (an unsigned 32-bit integer in decimal). A
// mismatch is reported as errSequenceGap so the engine reconnects and re-snapshots.
func verifyKrakenChecksum(book *entity.Orderbook, want string) error {
	got := strconv.FormatUint(uint64(krakenChecksum(book)), 10)
	if got != want {
		return fmt.Errorf("%w: kraken %s checksum mismatch (got %s, want %s)",
			errSequenceGap, book.Symbol, got, want)
	}
	return nil
}

// krakenChecksum implements Kraken's WebSocket v1 book checksum: concatenate the
// top 10 asks (lowest price first) then the top 10 bids (highest price first),
// each as price+volume with the decimal point and leading zeros removed, and take
// the CRC32 (IEEE) of the resulting ASCII string.
// See https://docs.kraken.com/api/docs/guides/spot-ws-book-v1/
func krakenChecksum(book *entity.Orderbook) uint32 {
	var sb strings.Builder
	writeTop := func(levels []entity.PriceLevel) {
		for _, lvl := range levels[:min(len(levels), 10)] {
			sb.WriteString(krakenChecksumToken(lvl.Price))
			sb.WriteString(krakenChecksumToken(lvl.Size))
		}
	}
	writeTop(krakenSortByPrice(book.Asks(), true))
	writeTop(krakenSortByPrice(book.Bids(), false))
	return crc32.ChecksumIEEE([]byte(sb.String()))
}

// krakenChecksumToken formats one price or volume for the checksum: remove the
// decimal point and any leading zeros (e.g. "0.50000" -> "50000", "101.5" -> "1015").
func krakenChecksumToken(s string) string {
	s = strings.Replace(s, ".", "", 1)
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	return s
}

// krakenSortByPrice returns levels sorted by numeric price. Prices are parsed
// only to order them (never stored as floats); an unparseable price is skipped.
func krakenSortByPrice(levels []entity.PriceLevel, ascending bool) []entity.PriceLevel {
	type priced struct {
		lvl entity.PriceLevel
		p   float64
	}
	parsed := make([]priced, 0, len(levels))
	for _, l := range levels {
		p, err := strconv.ParseFloat(l.Price, 64)
		if err != nil {
			continue
		}
		parsed = append(parsed, priced{l, p})
	}
	sort.Slice(parsed, func(i, j int) bool {
		if ascending {
			return parsed[i].p < parsed[j].p
		}
		return parsed[i].p > parsed[j].p
	})
	out := make([]entity.PriceLevel, len(parsed))
	for i, pp := range parsed {
		out[i] = pp.lvl
	}
	return out
}
