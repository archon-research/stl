// Package wsclient owns the transport-level mechanics for Gorilla WebSocket
// clients: dialing, deadlines, pongs, serialized writes, keepalive pings, and
// clean shutdown.
package wsclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/archon-research/stl/stl-verify/internal/pkg/proxytls"
)

// ErrClosed is returned by Next once the connection has been closed.
var ErrClosed = errors.New("websocket connection closed")

// Config tunes a managed WebSocket connection.
type Config struct {
	HandshakeTimeout time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	PingInterval     time.Duration
	InboundBuffer    int
	Logger           *slog.Logger
}

func (c Config) withDefaults() Config {
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 10 * time.Second
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 60 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.PingInterval == 0 {
		c.PingInterval = 20 * time.Second
	}
	if c.InboundBuffer <= 0 {
		c.InboundBuffer = 256
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Conn is a managed WebSocket connection. Writes are safe to call concurrently;
// Next must be called from a single goroutine.
type Conn struct {
	conn   *websocket.Conn
	cfg    Config
	logger *slog.Logger

	msgs chan []byte
	errc chan error
	done chan struct{}

	writeMu   sync.Mutex
	closeOnce sync.Once
}

// Dial opens and starts a managed WebSocket connection to url.
func Dial(ctx context.Context, url string, cfg Config) (*Conn, error) {
	cfg = cfg.withDefaults()

	dialer := websocket.Dialer{
		HandshakeTimeout: cfg.HandshakeTimeout,
		Proxy:            http.ProxyFromEnvironment,
		TLSClientConfig:  proxytls.Config(),
	}

	conn, resp, err := dialer.DialContext(ctx, url, nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("dial %s: %w (http %d)", url, err, resp.StatusCode)
		}
		return nil, fmt.Errorf("dial %s: %w", url, err)
	}

	c := &Conn{
		conn:   conn,
		cfg:    cfg,
		logger: cfg.Logger,
		msgs:   make(chan []byte, cfg.InboundBuffer),
		errc:   make(chan error, 1),
		done:   make(chan struct{}),
	}

	if err := conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout)); err != nil {
		closeErr := conn.Close()
		return nil, errors.Join(fmt.Errorf("set read deadline: %w", err), closeErr)
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
	})

	go c.readPump()
	if cfg.PingInterval > 0 {
		go c.pingLoop()
	}
	return c, nil
}

func (c *Conn) readPump() {
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			c.reportError(err)
			return
		}
		if err := c.conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout)); err != nil {
			c.reportError(fmt.Errorf("set read deadline: %w", err))
			return
		}
		select {
		case c.msgs <- data:
		case <-c.done:
			return
		}
	}
}

func (c *Conn) pingLoop() {
	ticker := time.NewTicker(c.cfg.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			if err := c.writeControl(websocket.PingMessage, nil); err != nil {
				c.reportError(fmt.Errorf("ping: %w", err))
				return
			}
		}
	}
}

func (c *Conn) reportError(err error) {
	select {
	case c.errc <- err:
	default:
	}
}

// WriteJSON marshals and sends v as a single text frame.
func (c *Conn) WriteJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	return c.conn.WriteJSON(v)
}

// WriteText sends a raw text frame.
func (c *Conn) WriteText(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

func (c *Conn) writeControl(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(messageType, data, time.Now().Add(c.cfg.WriteTimeout))
}

// Next returns the next inbound frame, or an error if the connection failed,
// ctx was cancelled, or the connection was closed.
func (c *Conn) Next(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-c.errc:
		return nil, err
	case msg := <-c.msgs:
		return msg, nil
	case <-c.done:
		return nil, ErrClosed
	}
}

// Close tears down the connection and stops internal goroutines.
func (c *Conn) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if err := c.conn.Close(); err != nil {
			c.logger.Debug("error closing websocket", "error", err)
		}
	})
}
