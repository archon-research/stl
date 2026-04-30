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
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type mockParser struct{}

func (m *mockParser) Exchange() string { return "mock" }
func (m *mockParser) SubscribeMessage(_ []string, _ int) ([]byte, error) {
	return []byte(`{"op":"subscribe"}`), nil
}
func (m *mockParser) ParseMessage(msg []byte) ([]entity.OrderbookSnapshot, error) {
	if strings.Contains(string(msg), "orderbook") {
		return []entity.OrderbookSnapshot{{
			Exchange:   "mock",
			Symbol:     "BTC",
			Bids:       []entity.OrderbookLevel{{Price: 100, Size: 1, Liquidity: 100}},
			Asks:       []entity.OrderbookLevel{{Price: 101, Size: 1, Liquidity: 101}},
			CapturedAt: time.Now(),
		}}, nil
	}
	return nil, nil
}
func (m *mockParser) PingMessage() []byte { return nil }

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

func TestWSConnection_ReceivesSnapshots(t *testing.T) {
	server := newTestWSServer(t, func(conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage() // read subscription
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"orderbook"}`))
		time.Sleep(200 * time.Millisecond)
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	cfg := WSConnectionConfig{
		URL: wsURL, Parser: &mockParser{}, Pairs: []string{"BTCUSDT"}, Depth: 20,
		PingInterval: 5 * time.Second, PongTimeout: 3 * time.Second,
		ReconnectBackoff: 100 * time.Millisecond, MaxBackoff: 1 * time.Second, ChannelBuffer: 10,
	}
	conn := NewWSConnection(cfg, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch, err := conn.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	select {
	case snap := <-ch:
		if snap.Exchange != "mock" {
			t.Errorf("expected mock, got %q", snap.Exchange)
		}
		if snap.Symbol != "BTC" {
			t.Errorf("expected BTC, got %q", snap.Symbol)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for snapshot")
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
		URL: wsURL, Parser: &mockParser{}, Pairs: []string{"BTCUSDT"}, Depth: 20,
		PingInterval: 5 * time.Second, PongTimeout: 3 * time.Second,
		ReconnectBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond, ChannelBuffer: 10,
	}
	wsConn := NewWSConnection(cfg, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := wsConn.Connect(ctx)
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
			t.Fatalf("timed out, received %d snapshots (expected 2)", received)
		}
	}

	mu.Lock()
	if connectCount < 2 {
		t.Errorf("expected at least 2 connections, got %d", connectCount)
	}
	mu.Unlock()
}
