package cex

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.OrderbookSubscriber = (*WSCollector)(nil)

func TestWSCollector_MultiplexesFromMultipleExchanges(t *testing.T) {
	// Create two test WS servers, each sending one orderbook message.
	server1 := newTestWSServer(t, func(conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage() // read subscription
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"orderbook","id":1}`))
		time.Sleep(300 * time.Millisecond)
	})
	defer server1.Close()

	server2 := newTestWSServer(t, func(conn *websocket.Conn) {
		_, _, _ = conn.ReadMessage() // read subscription
		conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"orderbook","id":2}`))
		time.Sleep(300 * time.Millisecond)
	})
	defer server2.Close()

	wsURL1 := "ws" + strings.TrimPrefix(server1.URL, "http")
	wsURL2 := "ws" + strings.TrimPrefix(server2.URL, "http")

	configs := []WSConnectionConfig{
		{
			URL:              wsURL1,
			Parser:           &mockParser{},
			Pairs:            []string{"BTCUSDT"},
			Depth:            20,
			PingInterval:     5 * time.Second,
			PongTimeout:      3 * time.Second,
			ReconnectBackoff: 100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			ChannelBuffer:    10,
		},
		{
			URL:              wsURL2,
			Parser:           &mockParser{},
			Pairs:            []string{"ETHUSDT"},
			Depth:            20,
			PingInterval:     5 * time.Second,
			PongTimeout:      3 * time.Second,
			ReconnectBackoff: 100 * time.Millisecond,
			MaxBackoff:       1 * time.Second,
			ChannelBuffer:    10,
		},
	}

	collector := NewWSCollector(configs, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ch, err := collector.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}

	received := 0
	for received < 2 {
		select {
		case snap := <-ch:
			if snap.Exchange == "" {
				t.Error("expected non-empty exchange")
			}
			received++
		case <-ctx.Done():
			t.Fatalf("timed out waiting for snapshots, received %d (expected 2)", received)
		}
	}

	if err := collector.Unsubscribe(); err != nil {
		t.Logf("unsubscribe error (may be expected on close): %v", err)
	}
}
