package orderbook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"slices"
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

// NewKrakenProvider creates a provider that streams L2 books from Kraken
// (WebSocket v1). The book channel pushes a snapshot frame (as/bs) followed by
// update frames (a/b); a zero volume removes a level.
//
// Integrity: Kraken v1 carries no per-message sequence numbers, so the CRC32 "c"
// checksum over the top 10 levels is the only in-band way to detect a divergent
// book. Each update is validated against it (see verifyKrakenChecksum); a
// mismatch forces a reconnect and fresh snapshot. The feed is also fixed-depth
// top-N: an insert that brings a better price into range pushes the outermost
// level out of scope, and Kraken sends no explicit delete for that pushed-out
// level, so the book is trimmed to krakenDepth after every frame.
//
// Symbols are slash-separated pairs using Kraken's asset names (e.g. "XBT/USD",
// not "BTC/USD"); Watch upper-cases and validates the structure but does not
// rewrite asset names.
//
// Docs: https://docs.kraken.com/api/docs/guides/spot-ws-book-v1/ and
// https://support.kraken.com/articles/360027821131
func NewKrakenProvider(cfg Config) outbound.OrderbookProvider {
	return newFeedProvider(cfg, &krakenExchange{}, krakenMaxSymbols)
}

type krakenExchange struct{}

// Compile-time check that krakenExchange supplies an application-level keepalive.
var _ appPinger = (*krakenExchange)(nil)

func (e *krakenExchange) name() string     { return exchangeKraken }
func (e *krakenExchange) endpoint() string { return krakenWSBase }

func (e *krakenExchange) normalizeSymbol(s string) (string, error) {
	return normalizeSeparatedPair(s, "/")
}
func (e *krakenExchange) newHandler(group []string, logger *slog.Logger) frameHandler {
	return &krakenHandler{books: newBookSet(exchangeKraken), allowed: symbolSet(group), logger: logger}
}

// appPing satisfies appPinger with Kraken's JSON ping event.
func (e *krakenExchange) appPing() ([]byte, time.Duration) {
	return []byte(`{"event":"ping"}`), krakenPingPeriod
}

func (e *krakenExchange) subscribeMessages(group []string) []any {
	return []any{map[string]any{
		"event":        "subscribe",
		"pair":         group,
		"subscription": map[string]any{"name": "book", "depth": krakenDepth},
	}}
}

type krakenHandler struct {
	books   *bookSet
	allowed map[string]bool // subscribed pairs; frames for any other pair are rejected
	logger  *slog.Logger
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
func (h *krakenHandler) handle(raw []byte) ([]bookChange, error) {
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

func (h *krakenHandler) handleEvent(raw []byte) ([]bookChange, error) {
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
		// A subscription error is scoped to one pair; Kraken keeps the connection
		// and the other subscriptions alive. Treating it as connection-fatal would
		// reconnect with the same subscribe payload and permanently cycle every
		// healthy pair in the group, so log loudly and carry on instead. A rejected
		// pair simply never syncs: it keeps the reconnect backoff unreset and emits
		// nothing, both visible alongside this log line.
		h.logger.Error("kraken subscription error", "pair", ev.Pair, "error", ev.ErrorMessage)
		return nil, nil
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
func (h *krakenHandler) handleBook(raw []byte) ([]bookChange, error) {
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
	if !symbolAllowed(h.allowed, pair) {
		return nil, fmt.Errorf("%w: kraken %s", errUnexpectedSymbol, pair)
	}
	book := h.books.getOrCreate(pair)

	isSnapshot := false
	applied := false
	checksum := ""
	var eventTime time.Time
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
		if t := krakenMaxLevelTime(d); t.After(eventTime) {
			eventTime = t
		}
		// Kraken orders a combined frame [channelID, a-data, b-data, name, pair] so
		// the checksum on the last data object reflects the book after every object
		// in the frame is applied; keep the last non-empty c.
		if d.C != "" {
			checksum = d.C
		}
	}

	if !applied {
		return nil, nil
	}

	if err := finalizeKrakenBook(book, checksum); err != nil {
		return nil, err
	}

	return []bookChange{{book: book, isSnapshot: isSnapshot, t: eventTime}}, nil
}

// finalizeKrakenBook brings the book to its post-frame state in the one order
// that is correct: trim to the subscribed depth FIRST (Kraken pushes the
// outermost level out of scope with no explicit delete when a better price
// arrives), THEN verify the CRC32 over the resulting top 10. Verifying before
// trimming would checksum levels Kraken has already dropped. Keeping both steps
// here means a later edit cannot reorder or separate them.
func finalizeKrakenBook(book *entity.Orderbook, checksum string) error {
	trimKrakenSide(book, entity.Bid, krakenDepth)
	trimKrakenSide(book, entity.Ask, krakenDepth)
	if checksum == "" {
		return nil
	}
	// A mismatch means the local book has drifted from Kraken's; force a re-sync.
	return verifyKrakenChecksum(book, checksum)
}

// krakenMaxLevelTime returns the latest per-level update time in a data object.
// Kraken v1 frames carry no frame-level event time, but each level is
// [price, volume, timestamp, (republish)] with the timestamp as Unix seconds
// (e.g. "1700000000.123456"); the max across applied levels is the closest thing
// to an event time. Zero when no level carries a parseable timestamp.
func krakenMaxLevelTime(d krakenBookData) time.Time {
	var maxT time.Time
	for _, group := range [][][]string{d.As, d.Bs, d.A, d.B} {
		for _, lvl := range group {
			if len(lvl) < 3 {
				continue
			}
			if t := parseKrakenLevelTime(lvl[2]); t.After(maxT) {
				maxT = t
			}
		}
	}
	return maxT
}

// parseKrakenLevelTime parses a Kraken level timestamp ("seconds.fraction" Unix
// time) into UTC, returning the zero Time when it is empty or unparseable. It
// splits on the dot so no precision is lost to float64.
func parseKrakenLevelTime(s string) time.Time {
	secStr, fracStr, _ := strings.Cut(s, ".")
	sec, err := strconv.ParseInt(secStr, 10, 64)
	if err != nil {
		return time.Time{}
	}
	var nsec int64
	if fracStr != "" {
		// Pad/truncate the fraction to nanosecond precision (9 digits).
		n, err := strconv.ParseInt((fracStr + "000000000")[:9], 10, 64)
		if err != nil {
			return time.Time{}
		}
		nsec = n
	}
	return time.Unix(sec, nsec).UTC()
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

// krakenSortByPrice returns levels sorted by price using exact decimal
// comparison (never float64, which could mis-order two levels that differ only
// past its precision and so drop the wrong one at a deep book's edge). ascending
// keeps the lowest prices first (asks); otherwise highest first (bids). Prices
// are canonical decimals (parseLevel gated them on the way into the book).
func krakenSortByPrice(levels []entity.PriceLevel, ascending bool) []entity.PriceLevel {
	out := make([]entity.PriceLevel, len(levels))
	copy(out, levels)
	slices.SortFunc(out, func(a, b entity.PriceLevel) int {
		c := entity.CompareDecimal(a.Price, b.Price)
		if ascending {
			return c
		}
		return -c
	})
	return out
}
