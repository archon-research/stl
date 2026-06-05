package wsclient

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testConfig() Config {
	return Config{
		HandshakeTimeout: 2 * time.Second,
		ReadTimeout:      2 * time.Second,
		WriteTimeout:     2 * time.Second,
		PingInterval:     -1,
		InboundBuffer:    8,
		Logger:           slog.Default(),
	}
}

func newTestServer(t *testing.T, onConn func(*websocket.Conn)) string {
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
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func keepOpen(conn *websocket.Conn) {
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func sendText(t *testing.T, conn *websocket.Conn, payload string) {
	t.Helper()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
		t.Errorf("server write: %v", err)
	}
}

func TestConnReceivesFramesInOrder(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `{"n":1}`)
		sendText(t, conn, `{"n":2}`)
		sendText(t, conn, `{"n":3}`)
		keepOpen(conn)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	for _, want := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		got, err := conn.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if string(got) != want {
			t.Errorf("frame = %s, want %s", got, want)
		}
	}
}

func TestConnWriteJSONReachesServer(t *testing.T) {
	received := make(chan string, 1)
	url := newTestServer(t, func(conn *websocket.Conn) {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- string(data)
		keepOpen(conn)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteJSON(map[string]any{"op": "subscribe"}); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	select {
	case got := <-received:
		if strings.TrimSpace(got) != `{"op":"subscribe"}` {
			t.Errorf("server received %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive the subscribe frame")
	}
}

func TestConnNextReturnsErrorOnServerClose(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `hello`)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Next(ctx); err != nil {
		t.Fatalf("first Next should succeed, got %v", err)
	}
	if _, err := conn.Next(ctx); err == nil {
		t.Fatal("Next should return an error after server closes the connection")
	}
}

func TestConnNextRespectsContext(t *testing.T) {
	url := newTestServer(t, keepOpen)

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := Dial(dialCtx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := conn.Next(ctx); err == nil {
		t.Fatal("Next should return ctx error when context is cancelled")
	}
}

func TestConnSendsKeepalivePing(t *testing.T) {
	pinged := make(chan struct{}, 1)
	url := newTestServer(t, func(conn *websocket.Conn) {
		conn.SetPingHandler(func(string) error {
			select {
			case pinged <- struct{}{}:
			default:
			}
			return nil
		})
		keepOpen(conn)
	})

	cfg := testConfig()
	cfg.PingInterval = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	select {
	case <-pinged:
	case <-time.After(2 * time.Second):
		t.Fatal("server never received a keepalive ping")
	}
}

func TestConnCloseIsIdempotent(t *testing.T) {
	url := newTestServer(t, keepOpen)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	conn.Close()
	conn.Close()
	if _, err := conn.Next(ctx); err == nil {
		t.Error("Next after close should return an error")
	}
}
