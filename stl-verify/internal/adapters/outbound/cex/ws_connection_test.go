package cex

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type mockProtocol struct{}

func (m *mockProtocol) Exchange() string { return "mock" }
func (m *mockProtocol) SubscribeMessage(_ []string, _ int) ([]byte, error) {
	return []byte(`{"op":"subscribe"}`), nil
}
func (m *mockProtocol) PingMessage() []byte { return nil }

func newTestWSServer(t *testing.T, handler func(conn *websocket.Conn)) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Logf("upgrade error: %v", err)
			return
		}
		defer conn.Close()
		handler(conn)
	}))
}

func TestWSConnection_ForwardsRawFrames(t *testing.T) {
	const framePayload = `{"type":"orderbook","data":"hello"}`

	server := newTestWSServer(t, func(conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage() // consume subscribe
		conn.WriteMessage(websocket.TextMessage, []byte(framePayload))
		time.Sleep(200 * time.Millisecond)
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := WSConnectionConfig{
		URL: wsURL, Protocol: &mockProtocol{}, Pairs: []string{"BTCUSDT"}, Depth: 20,
		PingInterval: 5 * time.Second, PongTimeout: 3 * time.Second,
		ReconnectBackoff: 100 * time.Millisecond, MaxBackoff: 1 * time.Second, ChannelBuffer: 10,
	}
	conn := NewWSConnection(cfg, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, err := conn.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	select {
	case msg := <-ch:
		if msg.Source != "mock" {
			t.Errorf("expected source mock, got %q", msg.Source)
		}
		if string(msg.Payload) != framePayload {
			t.Errorf("payload mismatch: got %q want %q", msg.Payload, framePayload)
		}
		if msg.CapturedAt.IsZero() {
			t.Errorf("expected CapturedAt to be set")
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for frame")
	}
}

func TestWSConnection_ReconnectsOnDisconnect(t *testing.T) {
	var connectCount int
	var mu sync.Mutex

	server := newTestWSServer(t, func(conn *websocket.Conn) {
		mu.Lock()
		connectCount++
		count := connectCount
		mu.Unlock()

		_, _, _ = conn.ReadMessage()
		if count == 1 {
			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"orderbook"}`))
			time.Sleep(50 * time.Millisecond)
			conn.Close()
			return
		}
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"orderbook"}`))
		time.Sleep(500 * time.Millisecond)
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := WSConnectionConfig{
		URL: wsURL, Protocol: &mockProtocol{}, Pairs: []string{"BTCUSDT"}, Depth: 20,
		PingInterval: 5 * time.Second, PongTimeout: 3 * time.Second,
		ReconnectBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond, ChannelBuffer: 10,
	}
	wsConn := NewWSConnection(cfg, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := wsConn.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer wsConn.Close()

	received := 0
	for received < 2 {
		select {
		case <-ch:
			received++
		case <-ctx.Done():
			t.Fatalf("timed out, received %d frames (expected 2)", received)
		}
	}

	mu.Lock()
	if connectCount < 2 {
		t.Errorf("expected at least 2 connections, got %d", connectCount)
	}
	mu.Unlock()
}

func TestBuildWSConnectionConfig_KnownExchange(t *testing.T) {
	cfg, err := BuildWSConnectionConfig("binance")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Protocol == nil {
		t.Fatal("expected Protocol to be set")
	}
	if cfg.Protocol.Exchange() != "binance" {
		t.Errorf("expected exchange binance, got %q", cfg.Protocol.Exchange())
	}
	if len(cfg.Pairs) == 0 {
		t.Error("expected at least one pair")
	}
}

func TestBuildWSConnectionConfig_UnknownExchange(t *testing.T) {
	_, err := BuildWSConnectionConfig("nonexistent")
	if err == nil {
		t.Error("expected error for unknown exchange")
	}
}
