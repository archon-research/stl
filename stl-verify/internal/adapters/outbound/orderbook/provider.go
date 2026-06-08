// Package orderbook implements outbound.OrderbookProvider for several centralised
// exchanges (Coinbase, OKX, Kraken).
//
// Each adapter maintains a full local book per symbol: it seeds an L2 book from a
// snapshot, then keeps it current by applying the exchange's WebSocket delta
// stream, emitting a fully aggregated book on every change. Symbols are
// multiplexed over the minimum number of WebSocket connections the exchange
// supports, and connections reconnect automatically with exponential backoff.
//
// The package deliberately contains no persistence: it produces a stream of
// entity.OrderbookUpdate values and leaves storage (e.g. the 1s JSONB dump to
// TimescaleDB) to the service layer.
package orderbook

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/wsclient"
)

// Config holds the tuning shared by every exchange adapter. Use DefaultConfig
// and override as needed; zero-valued fields fall back to defaults.
type Config struct {
	// Logger is the structured logger. Defaults to slog.Default.
	Logger *slog.Logger

	// InitialBackoff is the delay before the first reconnect attempt.
	InitialBackoff time.Duration
	// MaxBackoff caps the exponential reconnect backoff.
	MaxBackoff time.Duration
	// BackoffFactor multiplies the backoff after each failed attempt.
	BackoffFactor float64

	// HandshakeTimeout bounds the WebSocket upgrade handshake.
	HandshakeTimeout time.Duration
	// ReadTimeout is the WebSocket read deadline (extended on every frame).
	ReadTimeout time.Duration
	// WriteTimeout bounds a single WebSocket write.
	WriteTimeout time.Duration
	// PingInterval is the keepalive ping period (negative disables pings).
	PingInterval time.Duration

	// OutputBuffer is the per-Watch channel buffer. Because each update carries
	// the full book, updates are dropped (not corrupted) when the consumer falls
	// behind, so this need not be large.
	OutputBuffer int
}

// DefaultConfig returns production-sensible defaults.
func DefaultConfig() Config {
	return Config{
		Logger:           slog.Default(),
		InitialBackoff:   1 * time.Second,
		MaxBackoff:       60 * time.Second,
		BackoffFactor:    2.0,
		HandshakeTimeout: 10 * time.Second,
		ReadTimeout:      60 * time.Second,
		WriteTimeout:     10 * time.Second,
		PingInterval:     20 * time.Second,
		OutputBuffer:     256,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	if c.Logger == nil {
		c.Logger = d.Logger
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = d.InitialBackoff
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = d.MaxBackoff
	}
	if c.BackoffFactor <= 1 {
		c.BackoffFactor = d.BackoffFactor
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = d.HandshakeTimeout
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = d.ReadTimeout
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = d.WriteTimeout
	}
	if c.PingInterval == 0 {
		c.PingInterval = d.PingInterval
	}
	if c.OutputBuffer <= 0 {
		c.OutputBuffer = d.OutputBuffer
	}
	return c
}

// ws derives the managed-connection config from the shared Config.
func (c Config) ws() wsclient.Config {
	return wsclient.Config{
		HandshakeTimeout: c.HandshakeTimeout,
		ReadTimeout:      c.ReadTimeout,
		WriteTimeout:     c.WriteTimeout,
		PingInterval:     c.PingInterval,
		InboundBuffer:    c.OutputBuffer,
		Logger:           c.Logger,
	}
}

// validateSymbols rejects an empty list or empty entries.
func validateSymbols(symbols []string) error {
	if len(symbols) == 0 {
		return errors.New("at least one symbol is required")
	}
	for i, s := range symbols {
		if s == "" {
			return fmt.Errorf("symbol at index %d is empty", i)
		}
	}
	return nil
}

// normalizeSymbols applies normalize to each symbol, returning the normalized
// slice or the first error.
func normalizeSymbols(symbols []string, normalize func(string) (string, error)) ([]string, error) {
	out := make([]string, len(symbols))
	for i, s := range symbols {
		n, err := normalize(s)
		if err != nil {
			return nil, fmt.Errorf("symbol %q: %w", s, err)
		}
		out[i] = n
	}
	return out, nil
}

// isAlphanumeric reports whether s is non-empty and contains only ASCII letters
// or digits (after upper-casing, only A-Z and 0-9). Whitespace and punctuation
// are rejected so a malformed symbol cannot slip through as a subscribe payload
// or book key the venue never echoes back.
func isAlphanumeric(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		isDigit := r >= '0' && r <= '9'
		isLetter := r >= 'A' && r <= 'Z'
		if !isDigit && !isLetter {
			return false
		}
	}
	return true
}

// normalizeSeparatedPair upper-cases symbol and requires exactly two
// alphanumeric parts split on sep (e.g. "btc-usd" -> "BTC-USD"). Parts with
// whitespace or punctuation are rejected rather than silently trimmed, since a
// padded symbol would produce a subscribe payload the venue never echoes back.
func normalizeSeparatedPair(symbol, sep string) (string, error) {
	up := strings.ToUpper(symbol)
	parts := strings.Split(up, sep)
	if len(parts) != 2 || !isAlphanumeric(parts[0]) || !isAlphanumeric(parts[1]) {
		return "", fmt.Errorf("expected two alphanumeric parts separated by %q, got %q", sep, symbol)
	}
	return up, nil
}

// chunkSymbols splits symbols into groups of at most size entries. A size of
// zero (or one group that fits) yields a single group.
func chunkSymbols(symbols []string, size int) [][]string {
	if size <= 0 || len(symbols) <= size {
		return [][]string{symbols}
	}
	groups := make([][]string, 0, (len(symbols)+size-1)/size)
	for i := 0; i < len(symbols); i += size {
		groups = append(groups, symbols[i:min(i+size, len(symbols))])
	}
	return groups
}

// runConnections starts one goroutine per symbol group running connect, and
// returns the shared output channel. The channel is closed once ctx is
// cancelled and every connection goroutine has returned.
func runConnections(
	ctx context.Context,
	groups [][]string,
	buffer int,
	connect func(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate),
) <-chan entity.OrderbookUpdate {
	if buffer <= 0 {
		buffer = 1
	}
	out := make(chan entity.OrderbookUpdate, buffer)

	var wg sync.WaitGroup
	for _, g := range groups {
		wg.Add(1)
		go func(group []string) {
			defer wg.Done()
			connect(ctx, group, out)
		}(g)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// reconnectLoop runs connect repeatedly until ctx is cancelled, backing off
// exponentially between attempts. connect is passed a ready callback it should
// invoke once the connection is fully established and synchronised; calling it
// resets the backoff so a long-lived connection that later drops reconnects
// promptly.
func reconnectLoop(
	ctx context.Context,
	cfg Config,
	logger *slog.Logger,
	connect func(ctx context.Context, ready func()) error,
) {
	backoff := cfg.InitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}

		err := connect(ctx, func() { backoff = cfg.InitialBackoff })

		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Warn("orderbook connection lost, reconnecting", "error", err, "backoff", backoff)
		} else {
			logger.Info("orderbook connection closed, reconnecting", "backoff", backoff)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(time.Duration(float64(backoff)*cfg.BackoffFactor), cfg.MaxBackoff)
	}
}

// emitter delivers full-book updates to one connection's output channel. It never
// blocks: a blocking send would park the read loop, stop the socket draining, and
// wedge the connection with no reconnect. Updates are dropped when the consumer is
// behind, which is safe because every update carries the whole book. A dropped
// snapshot's discontinuity is not lost, though: its IsSnapshot flag is carried
// onto the next delivered update for that symbol.
type emitter struct {
	out      chan<- entity.OrderbookUpdate
	deferred map[string]bool // symbol -> a snapshot was dropped; re-flag next delivery
}

func newEmitter(out chan<- entity.OrderbookUpdate) *emitter {
	return &emitter{out: out, deferred: make(map[string]bool)}
}

// emit sends book without blocking and reports whether it was delivered. The
// update is flagged a snapshot when isSnapshot is set or a snapshot was dropped
// earlier for this symbol.
func (e *emitter) emit(book *entity.Orderbook, isSnapshot bool, t time.Time) bool {
	snapshot := isSnapshot || e.deferred[book.Symbol]
	// Skip building the clone when the buffer is already full.
	if c := cap(e.out); c > 0 && len(e.out) >= c {
		if snapshot {
			e.deferred[book.Symbol] = true
		}
		return false
	}
	select {
	case e.out <- entity.NewOrderbookUpdate(book, snapshot, t, time.Now()):
		delete(e.deferred, book.Symbol)
		return true
	default:
		if snapshot {
			e.deferred[book.Symbol] = true
		}
		return false
	}
}

// parseLevel validates an exchange [price, size] pair and returns the original
// decimal strings unchanged. Both must be canonical fixed-point decimals
// (entity.IsCanonicalDecimal): signs, exponents and NaN/Inf are rejected (so map
// keys stay stable and Kraken's checksum sees plain digits), and a violation fails
// the frame to force a resync. Price must be positive; size zero means "level
// removed".
func parseLevel(price, size string) (entity.PriceLevel, error) {
	if !entity.IsCanonicalDecimal(price) {
		return entity.PriceLevel{}, fmt.Errorf("invalid price %q: not a canonical decimal", price)
	}
	if entity.IsZeroDecimal(price) {
		return entity.PriceLevel{}, fmt.Errorf("invalid price %q: must be positive", price)
	}
	if !entity.IsCanonicalDecimal(size) {
		return entity.PriceLevel{}, fmt.Errorf("invalid size %q: not a canonical decimal", size)
	}
	return entity.PriceLevel{Price: price, Size: size}, nil
}

// forEachLevel parses each [price, size] pair (validating shape and contents via
// parseLevel) and invokes fn with the parsed level, stopping at the first error.
func forEachLevel(raw [][]string, fn func(entity.PriceLevel) error) error {
	for i, pair := range raw {
		if len(pair) < 2 {
			return fmt.Errorf("level %d: expected [price, size], got %d fields", i, len(pair))
		}
		lvl, err := parseLevel(pair[0], pair[1])
		if err != nil {
			return fmt.Errorf("level %d: %w", i, err)
		}
		if err := fn(lvl); err != nil {
			return err
		}
	}
	return nil
}

// applyDeltaLevels applies a delta's [price, size] pairs to one side of book in
// place (a zero size removes a level). This is the hot path for WS deltas.
func applyDeltaLevels(book *entity.Orderbook, side entity.Side, raw [][]string) error {
	return forEachLevel(raw, func(lvl entity.PriceLevel) error {
		book.ApplyLevel(side, lvl.Price, lvl.Size)
		return nil
	})
}

// errSequenceGap signals that an adapter detected a gap in an exchange's update
// sequence (or, for Kraken, a CRC32 mismatch) and must re-synchronise from a
// fresh snapshot. The feed engine treats it, like any handler error, as
// connection-fatal and reconnects.
var errSequenceGap = errors.New("orderbook update sequence gap")

// errUnexpectedSymbol is returned by a handler when the venue pushes book data
// for a symbol we never subscribed to. The engine treats it like any handler
// error: drop the connection and reconnect (which re-sends only our
// subscriptions), rather than emit a book we cannot account for.
var errUnexpectedSymbol = errors.New("orderbook update for unsubscribed symbol")

// symbolSet builds a lookup of normalised symbols for the unsubscribed-symbol
// guard.
func symbolSet(symbols []string) map[string]bool {
	m := make(map[string]bool, len(symbols))
	for _, s := range symbols {
		m[strings.ToUpper(s)] = true
	}
	return m
}

// symbolAllowed reports whether sym is one of the subscribed symbols. A nil set
// (handlers constructed directly in tests) allows everything, so the guard is
// active only once the engine populates it from the subscription group.
func symbolAllowed(allowed map[string]bool, sym string) bool {
	return allowed == nil || allowed[strings.ToUpper(sym)]
}

// parseRFC3339OrZero parses an RFC3339 venue timestamp, returning the zero Time
// when the string is empty or malformed. The zero Time signals "no usable venue
// event time"; callers must not substitute the local clock for it (the local
// clock is recorded separately as OrderbookUpdate.IngestedAt).
func parseRFC3339OrZero(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}

// parseUnixMillisOrZero parses a Unix-millisecond venue timestamp string,
// returning the zero Time when it is empty or unparseable (see parseRFC3339OrZero).
func parseUnixMillisOrZero(s string) time.Time {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
