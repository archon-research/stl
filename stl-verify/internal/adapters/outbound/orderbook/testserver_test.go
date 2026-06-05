package orderbook

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// testLogger returns a logger that discards output, keeping test runs quiet.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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
	server *httptest.Server
	url    string // ws:// URL
}

func newWSTestServer(t *testing.T, onConn func(conn *websocket.Conn)) *wsTestServer {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		onConn(conn)
	}))
	t.Cleanup(srv.Close)
	return &wsTestServer{server: srv, url: "ws" + strings.TrimPrefix(srv.URL, "http")}
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
