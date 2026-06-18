package orderbook

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/gorilla/websocket"
)

// testLogger returns a logger that discards output, keeping test runs quiet.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// syncBuffer is a concurrency-safe buffer for capturing log output written from a
// background goroutine (e.g. the app-ping loop) while the test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogger returns a logger whose output is recorded in the returned buffer.
func captureLogger() (*slog.Logger, *syncBuffer) {
	sb := &syncBuffer{}
	return slog.New(slog.NewTextHandler(sb, nil)), sb
}

// testConfig returns a Config tuned for fast, quiet tests.
func testConfig() Config {
	cfg := DefaultConfig()
	cfg.Logger = testLogger()
	cfg.InitialBackoff = 5 * time.Millisecond
	cfg.MaxBackoff = 20 * time.Millisecond
	return cfg
}

// wsTestServer is a minimal WebSocket server for tests. Each accepted connection
// is handed to onConn, which owns the connection for its lifetime (the socket is
// closed when onConn returns).
type wsTestServer struct {
	url string // ws:// URL
}

func newWSTestServer(t *testing.T, onConn func(conn *websocket.Conn)) *wsTestServer {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("websocket upgrade failed: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		onConn(conn)
	}))
	t.Cleanup(srv.Close)
	return &wsTestServer{url: "ws" + strings.TrimPrefix(srv.URL, "http")}
}

// keepOpen blocks reading from conn until the client disconnects, so the server
// side stays alive while the client reads pushed frames.
func keepOpen(conn *websocket.Conn) {
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

// sendText writes a text frame, failing the test on error.
func sendText(t *testing.T, conn *websocket.Conn, payload string) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
		t.Errorf("server write: %v", err)
	}
}

// sizeAt returns the size at price on one side, or ok=false when absent. Shared
// by the per-exchange handler tests.
func sizeAt(levels []entity.PriceLevel, price string) (string, bool) {
	for _, l := range levels {
		if l.Price == price {
			return l.Size, true
		}
	}
	return "", false
}
