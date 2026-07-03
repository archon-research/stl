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
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/archon-research/stl/stl-verify/internal/pkg/proxytls"
)

// ErrClosed is returned by Next when the connection was shut down by a caller's
// Close. A connection that died for any other reason (read error, deadline,
// ping failure) surfaces that underlying error from Next instead, so callers
// can branch on errors.Is(err, ErrClosed) to tell an intentional shutdown apart
// from a drop that warrants reconnecting.
var ErrClosed = errors.New("websocket connection closed")

// Frame is a single inbound WebSocket message. Type is the gorilla message type
// (websocket.TextMessage or websocket.BinaryMessage) so callers can demux text
// and binary feeds on the same connection.
type Frame struct {
	Type int
	Data []byte
}

// Config tunes a managed WebSocket connection.
type Config struct {
	HandshakeTimeout time.Duration
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	// PingInterval controls keepalive pings: 0 uses the default (20s); a negative
	// value disables keepalive pings entirely.
	PingInterval  time.Duration
	InboundBuffer int
	Logger        *slog.Logger
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

// wsConn is the subset of *websocket.Conn that Conn depends on. It exists so the
// underlying socket can be faked in tests (e.g. to force write/ping failures).
type wsConn interface {
	ReadMessage() (messageType int, p []byte, err error)
	SetReadDeadline(t time.Time) error
	SetPongHandler(h func(appData string) error)
	SetWriteDeadline(t time.Time) error
	WriteMessage(messageType int, data []byte) error
	WriteControl(messageType int, data []byte, deadline time.Time) error
	WriteJSON(v any) error
	Close() error
}

// Conn is a managed WebSocket connection. Writes are safe to call concurrently;
// Next must be called from a single goroutine.
type Conn struct {
	conn   wsConn
	cfg    Config
	logger *slog.Logger

	msgs chan Frame
	done chan struct{}

	// termErr holds the first non-Close error that tore the connection down. It
	// is the single source of truth for why the connection ended: Next reads it
	// on the done signal and returns it in place of ErrClosed.
	termErr atomic.Pointer[error]

	// closing is set by Close before teardown so the read/ping error that results
	// from closing the socket ourselves is not mistaken for a terminal failure.
	closing atomic.Bool

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

	return newConn(conn, cfg)
}

// newConn wires up the read/ping pumps and deadlines around an established
// connection. Dial uses it after dialing; tests use it to inject a fake socket.
// cfg is assumed to already carry defaults (Dial applies them); it is re-applied
// here so direct callers get sane values too.
func newConn(raw wsConn, cfg Config) (*Conn, error) {
	cfg = cfg.withDefaults()

	c := &Conn{
		conn:   raw,
		cfg:    cfg,
		logger: cfg.Logger,
		msgs:   make(chan Frame, cfg.InboundBuffer),
		done:   make(chan struct{}),
	}

	if err := raw.SetReadDeadline(time.Now().Add(cfg.ReadTimeout)); err != nil {
		closeErr := raw.Close()
		return nil, errors.Join(fmt.Errorf("set read deadline: %w", err), closeErr)
	}
	raw.SetPongHandler(func(string) error {
		return raw.SetReadDeadline(time.Now().Add(cfg.ReadTimeout))
	})

	go c.readPump()
	if cfg.PingInterval > 0 {
		go c.pingLoop()
	}
	return c, nil
}

func (c *Conn) readPump() {
	for {
		mt, data, err := c.conn.ReadMessage()
		if err != nil {
			c.reportError(err)
			return
		}
		if err := c.conn.SetReadDeadline(time.Now().Add(c.cfg.ReadTimeout)); err != nil {
			c.reportError(fmt.Errorf("set read deadline: %w", err))
			return
		}
		select {
		case c.msgs <- Frame{Type: mt, Data: data}:
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

func (c *Conn) writeControl(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteControl(messageType, data, time.Now().Add(c.cfg.WriteTimeout))
}

func (c *Conn) reportError(err error) {
	// A read/ping error observed after the caller invoked Close is just the
	// consequence of us closing the socket; leave termErr nil so Next reports the
	// intentional shutdown as ErrClosed.
	if c.closing.Load() {
		return
	}
	// Record the first error as the terminal cause, then tear down. Subsequent
	// errors are follow-on symptoms of the same teardown; keep the root cause and
	// log the rest at debug so they are not silently masked in diagnostics.
	if !c.termErr.CompareAndSwap(nil, &err) {
		c.logger.Debug("dropping subsequent websocket error", "error", err)
	}
	c.shutdown()
}

// shutdown closes done and the underlying connection exactly once. It is the
// single teardown path shared by terminal read/ping errors and Close.
func (c *Conn) shutdown() {
	c.closeOnce.Do(func() {
		close(c.done)
		if err := c.conn.Close(); err != nil {
			c.logger.Debug("error closing websocket", "error", err)
		}
	})
}

// Next returns the next inbound frame, or an error if ctx was cancelled or the
// connection ended. When the connection ended, it returns the terminal error
// that tore it down, or ErrClosed if it was shut down by a call to Close. Next
// must be called from a single goroutine.
func (c *Conn) Next(ctx context.Context) (Frame, error) {
	select {
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	case msg := <-c.msgs:
		return msg, nil
	case <-c.done:
		return c.terminalResult()
	}
}

// terminalResult produces Next's return value once c.done is observed closed.
// After an unexpected drop (termErr set) it first drains any buffered frame so
// callers still receive every message that arrived before the drop; repeated
// calls drain the rest before the error is finally returned. A caller-initiated
// Close (termErr nil) returns ErrClosed immediately.
func (c *Conn) terminalResult() (Frame, error) {
	if errp := c.termErr.Load(); errp != nil {
		select {
		case msg := <-c.msgs:
			return msg, nil
		default:
		}
		return Frame{}, *errp
	}
	return Frame{}, ErrClosed
}

// teardownErr reports a non-nil error once teardown has begun (done closed),
// returning the terminal cause when known and ErrClosed otherwise. Writes check
// this so they fail deterministically as soon as Next can observe the drop —
// shutdown closes done before it closes the socket, and a write racing into that
// window would otherwise buffer into the dropped TCP connection and appear to
// succeed.
func (c *Conn) teardownErr() error {
	select {
	case <-c.done:
		if errp := c.termErr.Load(); errp != nil {
			return fmt.Errorf("write on closed connection: %w", *errp)
		}
		return ErrClosed
	default:
		return nil
	}
}

// WriteJSON marshals and sends v as a single text frame.
func (c *Conn) WriteJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.teardownErr(); err != nil {
		return err
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	if err := c.conn.WriteJSON(v); err != nil {
		return fmt.Errorf("write json frame: %w", err)
	}
	return nil
}

// WriteText sends a raw text frame.
func (c *Conn) WriteText(data []byte) error {
	return c.writeMessage(websocket.TextMessage, data)
}

// WriteBinary sends a raw binary frame.
func (c *Conn) WriteBinary(data []byte) error {
	return c.writeMessage(websocket.BinaryMessage, data)
}

func (c *Conn) writeMessage(messageType int, data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.teardownErr(); err != nil {
		return err
	}
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout)); err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}
	if err := c.conn.WriteMessage(messageType, data); err != nil {
		return fmt.Errorf("write websocket frame: %w", err)
	}
	return nil
}

// Close tears down the connection and stops internal goroutines. After Close,
// Next returns ErrClosed.
func (c *Conn) Close() {
	c.closing.Store(true)
	c.shutdown()
}
