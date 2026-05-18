package cex

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/gorilla/websocket"
)

type WSConnectionConfig struct {
	URL              string
	Protocol         WSProtocol
	Pairs            []string
	Depth            int
	PingInterval     time.Duration
	PongTimeout      time.Duration
	ReconnectBackoff time.Duration
	MaxBackoff       time.Duration
	ChannelBuffer    int
}

// WSConnection maintains a single resilient WebSocket connection to one
// exchange. It forwards every incoming frame as a RawCEXMessage without
// parsing — the indexer worker is responsible for interpreting payloads.
type WSConnection struct {
	config WSConnectionConfig
	logger *slog.Logger
	conn   *websocket.Conn
	mu     sync.RWMutex
	done   chan struct{}
	closed atomic.Bool
	out    chan entity.RawCEXMessage
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWSConnection(config WSConnectionConfig, logger *slog.Logger) *WSConnection {
	if logger == nil {
		logger = slog.Default()
	}
	if config.ChannelBuffer <= 0 {
		config.ChannelBuffer = 100
	}
	return &WSConnection{
		config: config,
		logger: logger.With("component", "ws-connection", "exchange", config.Protocol.Exchange()),
		done:   make(chan struct{}),
		out:    make(chan entity.RawCEXMessage, config.ChannelBuffer),
	}
}

// Subscribe starts the connection loop and returns a channel of raw frames.
// The channel is closed when Close is called.
func (w *WSConnection) Subscribe(ctx context.Context) (<-chan entity.RawCEXMessage, error) {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.connectionLoop()
	return w.out, nil
}

func (w *WSConnection) Close() error {
	if w.closed.Swap(true) {
		return nil
	}
	close(w.done)
	if w.cancel != nil {
		w.cancel()
	}
	w.mu.Lock()
	var connErr error
	if w.conn != nil {
		connErr = w.conn.Close()
		w.conn = nil
	}
	w.mu.Unlock()
	w.wg.Wait()
	close(w.out)
	return connErr
}

func (w *WSConnection) HealthCheck() error {
	if w.closed.Load() {
		return fmt.Errorf("%s: connection closed", w.config.Protocol.Exchange())
	}
	w.mu.RLock()
	conn := w.conn
	w.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("%s: not connected", w.config.Protocol.Exchange())
	}
	return nil
}

func (w *WSConnection) connectionLoop() {
	defer w.wg.Done()
	backoff := w.config.ReconnectBackoff

	for {
		select {
		case <-w.done:
			return
		case <-w.ctx.Done():
			return
		default:
		}

		if err := w.connectAndSubscribe(); err != nil {
			w.logger.Warn("connect failed", "error", err, "backoff", backoff)
			select {
			case <-w.done:
				return
			case <-w.ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(time.Duration(float64(backoff)*2), w.config.MaxBackoff)
			continue
		}

		backoff = w.config.ReconnectBackoff
		w.logger.Info("connected")

		if err := w.readLoop(); err != nil {
			w.logger.Warn("connection lost, reconnecting", "error", err)
		}
	}
}

func (w *WSConnection) connectAndSubscribe() error {
	dialer := websocket.Dialer{HandshakeTimeout: 10 * time.Second}
	conn, _, err := dialer.DialContext(w.ctx, w.config.URL, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(w.config.PongTimeout + w.config.PingInterval))
	})

	subMsg, err := w.config.Protocol.SubscribeMessage(w.config.Pairs, w.config.Depth)
	if err != nil {
		conn.Close()
		return fmt.Errorf("build subscribe: %w", err)
	}
	if err := conn.WriteMessage(websocket.TextMessage, subMsg); err != nil {
		conn.Close()
		return fmt.Errorf("send subscribe: %w", err)
	}

	w.mu.Lock()
	w.conn = conn
	w.mu.Unlock()
	return nil
}

func (w *WSConnection) readLoop() error {
	pingTicker := time.NewTicker(w.config.PingInterval)
	defer pingTicker.Stop()

	readErr := make(chan error, 1)
	msgChan := make(chan []byte, 10)
	readerDone := make(chan struct{})
	defer close(readerDone)

	go func() {
		for {
			w.mu.RLock()
			conn := w.conn
			w.mu.RUnlock()
			if conn == nil {
				select {
				case readErr <- fmt.Errorf("connection is nil"):
				case <-readerDone:
				}
				return
			}
			_, msg, err := conn.ReadMessage()
			if err != nil {
				select {
				case readErr <- err:
				case <-readerDone:
				}
				return
			}
			select {
			case msgChan <- msg:
			case <-readerDone:
				return
			}
		}
	}()

	source := w.config.Protocol.Exchange()
	for {
		select {
		case <-w.done:
			w.closeConn()
			return nil
		case <-w.ctx.Done():
			w.closeConn()
			return nil
		case err := <-readErr:
			w.closeConn()
			return fmt.Errorf("read: %w", err)
		case msg := <-msgChan:
			envelope := entity.RawCEXMessage{
				Source:     source,
				CapturedAt: time.Now().UTC(),
				Payload:    msg,
			}
			select {
			case w.out <- envelope:
			default:
				w.logger.Warn("channel full, dropping message", "source", source)
			}
		case <-pingTicker.C:
			if err := w.sendPing(); err != nil {
				w.closeConn()
				return fmt.Errorf("ping: %w", err)
			}
		}
	}
}

func (w *WSConnection) sendPing() error {
	w.mu.RLock()
	conn := w.conn
	w.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("no connection")
	}
	if pingMsg := w.config.Protocol.PingMessage(); pingMsg != nil {
		return conn.WriteMessage(websocket.TextMessage, pingMsg)
	}
	return conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(w.config.PongTimeout))
}

func (w *WSConnection) closeConn() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.conn != nil {
		w.conn.Close()
		w.conn = nil
	}
}

// BuildWSConnectionConfig assembles a WSConnectionConfig for the given
// exchange from the global Exchanges registry. Returns an error if the
// exchange is unknown.
func BuildWSConnectionConfig(exchangeName string) (WSConnectionConfig, error) {
	exchange, ok := Exchanges[exchangeName]
	if !ok {
		return WSConnectionConfig{}, fmt.Errorf("unknown exchange %q", exchangeName)
	}
	protocol := ProtocolForExchange(exchangeName)
	if protocol == nil {
		return WSConnectionConfig{}, fmt.Errorf("no protocol implementation for exchange %q", exchangeName)
	}
	pairs := make([]string, 0, len(exchange.Symbols))
	for _, pair := range exchange.Symbols {
		pairs = append(pairs, pair)
	}
	return WSConnectionConfig{
		URL:              exchange.WebSocketURL,
		Protocol:         protocol,
		Pairs:            pairs,
		Depth:            exchange.MaxDepth,
		PingInterval:     exchange.PingInterval,
		PongTimeout:      exchange.PongTimeout,
		ReconnectBackoff: exchange.ReconnectBackoff,
		MaxBackoff:       exchange.MaxReconnectBackoff,
		ChannelBuffer:    200,
	}, nil
}
