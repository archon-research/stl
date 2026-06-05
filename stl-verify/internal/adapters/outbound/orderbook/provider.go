// Package orderbook implements outbound.OrderbookProvider for several centralised
// exchanges (Binance, Coinbase, OKX, Kraken).
//
// Each adapter maintains a Full Localbook per symbol: it seeds an L2 book from a
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
	"math"
	"strconv"
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

// emit sends a full-book update on out.
//
// Incremental (delta) updates are best-effort: if the consumer is behind (buffer
// full) the update is dropped, which is safe because every update carries the
// complete book — the consumer simply receives the next one. Skipping the clone
// in that case also keeps the high-frequency apply path from paying for work
// that would be discarded.
//
// Snapshot updates (isSnapshot) are NEVER dropped: IsSnapshot is a discontinuity
// signal marking the first book after a (re)sync, and a consumer may rely on it
// to reset derived state. Snapshots are delivered with a context-aware blocking
// send instead.
func emit(
	ctx context.Context,
	out chan<- entity.OrderbookUpdate,
	book *entity.Orderbook,
	isSnapshot bool,
	t time.Time,
) {
	if !isSnapshot {
		if c := cap(out); c > 0 && len(out) >= c {
			return // best-effort drop for deltas only
		}
	}
	upd := entity.NewOrderbookUpdate(book, isSnapshot, t)
	if isSnapshot {
		select {
		case out <- upd:
		case <-ctx.Done():
		}
		return
	}
	select {
	case out <- upd:
	case <-ctx.Done():
	default:
	}
}

// parseLevel validates an exchange [price, size] string pair and returns the
// original decimal strings (never a float, so no precision is lost on the way to
// storage).
//
// strconv.ParseFloat accepts "NaN"/"Inf"/"-Inf" and negative numbers, none of
// which are valid book values, so they are rejected here and the frame is failed
// (forcing a resync) rather than stored. The parsed float is used only to
// validate and is then discarded. A size of exactly zero is valid: it is the
// exchange convention for "level removed".
func parseLevel(price, size string) (entity.PriceLevel, error) {
	p, err := strconv.ParseFloat(price, 64)
	if err != nil {
		return entity.PriceLevel{}, fmt.Errorf("parse price %q: %w", price, err)
	}
	if math.IsNaN(p) || math.IsInf(p, 0) || p <= 0 {
		return entity.PriceLevel{}, fmt.Errorf("invalid price %q: must be finite and positive", price)
	}
	s, err := strconv.ParseFloat(size, 64)
	if err != nil {
		return entity.PriceLevel{}, fmt.Errorf("parse size %q: %w", size, err)
	}
	if math.IsNaN(s) || math.IsInf(s, 0) || s < 0 {
		return entity.PriceLevel{}, fmt.Errorf("invalid size %q: must be finite and non-negative", size)
	}
	return entity.PriceLevel{Price: price, Size: size}, nil
}

// parseLevels parses a list of [price, size] string pairs. Each entry must have
// at least two elements; extra elements (e.g. Kraken timestamps) are ignored.
func parseLevels(raw [][]string) ([]entity.PriceLevel, error) {
	levels := make([]entity.PriceLevel, 0, len(raw))
	for i, pair := range raw {
		if len(pair) < 2 {
			return nil, fmt.Errorf("level %d: expected [price, size], got %d fields", i, len(pair))
		}
		lvl, err := parseLevel(pair[0], pair[1])
		if err != nil {
			return nil, fmt.Errorf("level %d: %w", i, err)
		}
		levels = append(levels, lvl)
	}
	return levels, nil
}

// applyDeltaLevels applies a delta's [price, size] pairs to one side of book.
// Unlike parseLevels it mutates the book in place (a zero size removes a level)
// rather than returning a fresh slice, which is the hot path for WS deltas.
func applyDeltaLevels(book *entity.Orderbook, side entity.Side, raw [][]string) error {
	for i, pair := range raw {
		if len(pair) < 2 {
			return fmt.Errorf("level %d: expected [price, size], got %d fields", i, len(pair))
		}
		lvl, err := parseLevel(pair[0], pair[1])
		if err != nil {
			return fmt.Errorf("level %d: %w", i, err)
		}
		book.ApplyLevel(side, lvl.Price, lvl.Size)
	}
	return nil
}

// errSequenceGap signals that an adapter detected a gap in an exchange's update
// sequence and must re-synchronise from a fresh snapshot. Handling differs by
// engine: the WS-snapshot engine treats it (like any handler error) as
// connection-fatal and reconnects, while the Binance adapter intercepts it to
// re-sync a single symbol without dropping the shared connection.
var errSequenceGap = errors.New("orderbook update sequence gap")

// parseRFC3339OrNow parses an RFC3339 timestamp, falling back to the current
// time when the string is empty or malformed (a timestamp is informational, not
// worth dropping an otherwise valid update over).
func parseRFC3339OrNow(s string) time.Time {
	if s == "" {
		return time.Now().UTC()
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Now().UTC()
	}
	return t.UTC()
}

// parseUnixMillisOrNow parses a Unix-millisecond timestamp string, falling back
// to the current time on error. The result is always UTC.
func parseUnixMillisOrNow(s string) time.Time {
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Now().UTC()
	}
	return time.UnixMilli(ms).UTC()
}
