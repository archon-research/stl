package orderbook

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/wsclient"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Shared engine for exchanges that deliver the initial snapshot in-band over the
// WebSocket (Coinbase, OKX, Kraken). Connection lifecycle, subscribe, keepalive,
// emission and reconnection are identical; only the wire format differs, supplied
// per exchange via exchangeFeed.

// bookChange marks a book that changed while handling one inbound frame.
type bookChange struct {
	book       *entity.Orderbook
	isSnapshot bool
	t          time.Time
}

// bookSet holds the per-symbol books for a single connection.
type bookSet struct {
	exchange string
	books    map[string]*entity.Orderbook
}

func newBookSet(exchange string) *bookSet {
	return &bookSet{exchange: exchange, books: make(map[string]*entity.Orderbook)}
}

// getOrCreate returns the book for symbol, creating an empty one on first use.
func (bs *bookSet) getOrCreate(symbol string) *entity.Orderbook {
	b, ok := bs.books[symbol]
	if !ok {
		b = entity.NewOrderbook(bs.exchange, symbol)
		bs.books[symbol] = b
	}
	return b
}

// exchangeFeed describes a snapshot-over-WebSocket exchange: static
// metadata plus a factory for per-connection frame handlers.
type exchangeFeed interface {
	// name is the exchange identifier (e.g. "coinbase").
	name() string
	// normalizeSymbol canonicalises and validates one symbol, erroring on a bad format.
	normalizeSymbol(symbol string) (string, error)
	// endpoint returns the WebSocket URL for a symbol group.
	endpoint(group []string) string
	// subscribeMessages returns the frames to send after connecting.
	subscribeMessages(group []string) ([]any, error)
	// newHandler creates a fresh frame handler for one connection, scoped to the
	// symbols in group (so it can reject book data for anything unsubscribed).
	// Because each connection (and reconnection) gets its own handler, handlers
	// may hold mutable per-connection sync state without locking.
	newHandler(group []string) frameHandler
}

// frameHandler maintains one connection's books and synchronisation state. Its
// handle method is only ever called from a single goroutine.
type frameHandler interface {
	// handle parses one inbound frame, applies it to the handler's books, and
	// returns the books that should be emitted. A single frame may legitimately
	// fan out to multiple bookChanges (e.g. one frame carrying events for several
	// symbols), so the result is a slice. Returning errSequenceGap (or any error)
	// causes the engine to drop the connection and reconnect.
	handle(raw []byte) ([]bookChange, error)
}

// appPinger is an optional interface for exchanges that require an
// application-level keepalive (e.g. OKX's "ping" text frame, Kraken's
// {"event":"ping"}) in addition to protocol-level pings.
type appPinger interface {
	// appPing returns the raw text frame to send and the interval between sends.
	appPing() (frame []byte, interval time.Duration)
}

// feedProvider implements outbound.OrderbookProvider for any
// exchangeFeed.
type feedProvider struct {
	cfg        Config
	logger     *slog.Logger
	exchange   exchangeFeed
	maxSymbols int
}

var _ outbound.OrderbookProvider = (*feedProvider)(nil)

func newFeedProvider(cfg Config, exchange exchangeFeed, maxSymbols int) *feedProvider {
	cfg = cfg.withDefaults()
	return &feedProvider{
		cfg:        cfg,
		logger:     cfg.Logger.With("component", exchange.name()+"-orderbook"),
		exchange:   exchange,
		maxSymbols: maxSymbols,
	}
}

// Name returns the exchange identifier.
func (p *feedProvider) Name() string { return p.exchange.name() }

// Watch subscribes to symbols and streams aggregated books, splitting symbols
// across the fewest connections the exchange allows.
func (p *feedProvider) Watch(ctx context.Context, symbols []string) (<-chan entity.OrderbookUpdate, error) {
	if err := validateSymbols(symbols); err != nil {
		return nil, err
	}
	symbols, err := normalizeSymbols(symbols, p.exchange.normalizeSymbol)
	if err != nil {
		return nil, err
	}
	groups := chunkSymbols(symbols, p.maxSymbols)
	out := runConnections(ctx, groups, p.cfg.OutputBuffer, func(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate) {
		reconnectLoop(ctx, p.cfg, p.logger, func(ctx context.Context, ready func()) error {
			return p.runConnection(ctx, group, out, ready)
		})
	})
	return out, nil
}

// runConnection owns one WebSocket connection: it dials, subscribes, optionally
// starts an application-level keepalive, then applies frames until the
// connection drops or ctx is cancelled.
func (p *feedProvider) runConnection(ctx context.Context, group []string, out chan<- entity.OrderbookUpdate, ready func()) error {
	ws, err := wsclient.Dial(ctx, p.exchange.endpoint(group), p.cfg.ws())
	if err != nil {
		return err
	}
	defer ws.Close()

	// Scope helper goroutines (the app-level keepalive) to this connection rather
	// than to the long-lived Watch ctx, so they stop the moment this connection
	// ends instead of leaking across every reconnect.
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	subs, err := p.exchange.subscribeMessages(group)
	if err != nil {
		return fmt.Errorf("build subscribe message: %w", err)
	}
	for _, msg := range subs {
		if err := ws.WriteJSON(msg); err != nil {
			return fmt.Errorf("send subscribe: %w", err)
		}
	}

	if pinger, ok := p.exchange.(appPinger); ok {
		p.startAppPing(connCtx, ws, pinger)
	}

	handler := p.exchange.newHandler(group)
	em := newEmitter(out)
	// Reset the reconnect backoff only once every symbol in the group has produced
	// its initial snapshot. Resetting on the first symbol would let one healthy
	// symbol mask another that never syncs (e.g. a bad symbol that errors after a
	// good one snapshots), turning a partial-sync failure into a reconnect storm.
	pending := symbolSet(group)
	readyCalled := false
	for {
		frame, err := ws.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("websocket: %w", err)
		}
		changes, err := handler.handle(frame.Data)
		if err != nil {
			return err
		}
		for _, s := range changes {
			em.emit(s.book, s.isSnapshot, s.t)
			if s.isSnapshot && !readyCalled {
				delete(pending, strings.ToUpper(s.book.Symbol))
				if len(pending) == 0 {
					readyCalled = true
					ready()
				}
			}
		}
	}
}

// startAppPing launches a goroutine that sends the exchange's keepalive frame on
// its interval until the connection closes or ctx is cancelled.
func (p *feedProvider) startAppPing(ctx context.Context, ws *wsclient.Conn, pinger appPinger) {
	frame, interval := pinger.appPing()
	if interval <= 0 || len(frame) == 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := ws.WriteText(frame); err != nil {
					// The app ping is the primary keepalive for OKX/Kraken; if it can't
					// be sent, close so we reconnect promptly instead of idling to the
					// read timeout. Warn, since it explains the reconnect an operator sees.
					p.logger.Warn("app ping failed, closing connection", "error", err)
					ws.Close()
					return
				}
			}
		}
	}()
}
