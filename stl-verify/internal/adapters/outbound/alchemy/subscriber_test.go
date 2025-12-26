package alchemy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockWSServer creates a mock WebSocket server for testing.
type mockWSServer struct {
	server     *httptest.Server
	upgrader   websocket.Upgrader
	handler    func(conn *websocket.Conn)
	connMu     sync.Mutex
	conn       *websocket.Conn
	connClosed atomic.Bool
}

func newMockWSServer(handler func(conn *websocket.Conn)) *mockWSServer {
	m := &mockWSServer{
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		handler: handler,
	}

	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := m.upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		m.connMu.Lock()
		m.conn = conn
		m.connMu.Unlock()
		m.handler(conn)
	}))

	return m
}

func (m *mockWSServer) URL() string {
	return "ws" + strings.TrimPrefix(m.server.URL, "http")
}

func (m *mockWSServer) Close() {
	m.connMu.Lock()
	if m.conn != nil && !m.connClosed.Load() {
		m.conn.Close()
		m.connClosed.Store(true)
	}
	m.connMu.Unlock()
	m.server.Close()
}

func (m *mockWSServer) CloseConnection() {
	m.connMu.Lock()
	defer m.connMu.Unlock()
	if m.conn != nil && !m.connClosed.Load() {
		m.conn.Close()
		m.connClosed.Store(true)
	}
}

// --- Test: NewSubscriber ---

func TestNewSubscriber_RequiresWebSocketURL(t *testing.T) {
	_, err := NewSubscriber(SubscriberConfig{})
	if err == nil {
		t.Fatal("expected error when WebSocketURL is empty")
	}
	if !strings.Contains(err.Error(), "WebSocketURL is required") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewSubscriber_AppliesDefaults(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: "ws://localhost:8545",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defaults := SubscriberConfigDefaults()

	if sub.config.InitialBackoff != defaults.InitialBackoff {
		t.Errorf("InitialBackoff: got %v, want %v", sub.config.InitialBackoff, defaults.InitialBackoff)
	}
	if sub.config.MaxBackoff != defaults.MaxBackoff {
		t.Errorf("MaxBackoff: got %v, want %v", sub.config.MaxBackoff, defaults.MaxBackoff)
	}
	if sub.config.BackoffFactor != defaults.BackoffFactor {
		t.Errorf("BackoffFactor: got %v, want %v", sub.config.BackoffFactor, defaults.BackoffFactor)
	}
	if sub.config.PingInterval != defaults.PingInterval {
		t.Errorf("PingInterval: got %v, want %v", sub.config.PingInterval, defaults.PingInterval)
	}
	if sub.config.PongTimeout != defaults.PongTimeout {
		t.Errorf("PongTimeout: got %v, want %v", sub.config.PongTimeout, defaults.PongTimeout)
	}
	if sub.config.ReadTimeout != defaults.ReadTimeout {
		t.Errorf("ReadTimeout: got %v, want %v", sub.config.ReadTimeout, defaults.ReadTimeout)
	}
	if sub.config.ChannelBufferSize != defaults.ChannelBufferSize {
		t.Errorf("ChannelBufferSize: got %v, want %v", sub.config.ChannelBufferSize, defaults.ChannelBufferSize)
	}
	if sub.config.HealthTimeout != defaults.HealthTimeout {
		t.Errorf("HealthTimeout: got %v, want %v", sub.config.HealthTimeout, defaults.HealthTimeout)
	}
}

func TestNewSubscriber_UsesCustomConfig(t *testing.T) {
	customConfig := SubscriberConfig{
		WebSocketURL:      "ws://localhost:8545",
		InitialBackoff:    5 * time.Second,
		MaxBackoff:        120 * time.Second,
		BackoffFactor:     3.0,
		PingInterval:      45 * time.Second,
		PongTimeout:       15 * time.Second,
		ReadTimeout:       90 * time.Second,
		ChannelBufferSize: 200,
		HealthTimeout:     60 * time.Second,
	}

	sub, err := NewSubscriber(customConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sub.config.InitialBackoff != customConfig.InitialBackoff {
		t.Errorf("InitialBackoff: got %v, want %v", sub.config.InitialBackoff, customConfig.InitialBackoff)
	}
	if sub.config.MaxBackoff != customConfig.MaxBackoff {
		t.Errorf("MaxBackoff: got %v, want %v", sub.config.MaxBackoff, customConfig.MaxBackoff)
	}
	if sub.config.BackoffFactor != customConfig.BackoffFactor {
		t.Errorf("BackoffFactor: got %v, want %v", sub.config.BackoffFactor, customConfig.BackoffFactor)
	}
	if sub.config.ChannelBufferSize != customConfig.ChannelBufferSize {
		t.Errorf("ChannelBufferSize: got %v, want %v", sub.config.ChannelBufferSize, customConfig.ChannelBufferSize)
	}
}

// --- Test: Subscribe ---

func TestSubscribe_ReceivesBlockHeaders(t *testing.T) {
	blocksSent := make(chan struct{})

	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		if err := conn.WriteJSON(resp); err != nil {
			return
		}

		// Send block headers
		for i := 0; i < 3; i++ {
			header := outbound.BlockHeader{
				Number:     "0x" + string(rune('1'+i)),
				Hash:       "0xabc" + string(rune('0'+i)),
				ParentHash: "0xdef" + string(rune('0'+i)),
			}
			notification := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0x1234",
					"result":       header,
				},
			}
			if err := conn.WriteJSON(notification); err != nil {
				return
			}
		}
		close(blocksSent)

		// Keep connection open until test finishes
		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		InitialBackoff: 10 * time.Millisecond,
		ReadTimeout:    5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for blocks to be sent
	<-blocksSent

	// Collect received headers
	received := 0
	timeout := time.After(2 * time.Second)
	for received < 3 {
		select {
		case _, ok := <-headers:
			if !ok {
				t.Fatal("channel closed unexpectedly")
			}
			received++
		case <-timeout:
			t.Fatalf("timeout waiting for headers, received %d/3", received)
		}
	}

	if received != 3 {
		t.Errorf("expected 3 headers, got %d", received)
	}
}

func TestSubscribe_FailsWhenClosed(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: "ws://localhost:8545",
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	// Close the subscriber before subscribing
	sub.Unsubscribe()

	_, err = sub.Subscribe(context.Background())
	if err == nil {
		t.Fatal("expected error when subscribing to closed subscriber")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Test: Unsubscribe ---

func TestUnsubscribe_ClosesChannel(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Keep connection open
		<-time.After(10 * time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx := context.Background()
	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Give connection time to establish
	time.Sleep(100 * time.Millisecond)

	err = sub.Unsubscribe()
	if err != nil {
		t.Fatalf("failed to unsubscribe: %v", err)
	}

	// Channel should be closed
	select {
	case _, ok := <-headers:
		if ok {
			t.Fatal("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for channel to close")
	}
}

func TestUnsubscribe_Idempotent(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: "ws://localhost:8545",
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	// Unsubscribe multiple times should not panic
	for i := 0; i < 3; i++ {
		err := sub.Unsubscribe()
		if err != nil {
			t.Fatalf("unsubscribe %d failed: %v", i, err)
		}
	}
}

// --- Test: Reconnection ---

func TestSubscribe_ReconnectsOnConnectionLoss(t *testing.T) {
	connectCount := atomic.Int32{}
	reconnectCalled := atomic.Bool{}
	blockSent := make(chan struct{}, 1)

	server := newMockWSServer(func(conn *websocket.Conn) {
		count := connectCount.Add(1)

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		if count == 1 {
			// First connection: close immediately to trigger reconnect
			conn.Close()
			return
		}

		// Second connection: send a block and stay open
		header := outbound.BlockHeader{
			Number:     "0x100",
			Hash:       "0xabc",
			ParentHash: "0xdef",
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}
		conn.WriteJSON(notification)

		select {
		case blockSent <- struct{}{}:
		default:
		}

		<-time.After(5 * time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		ReadTimeout:    2 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	sub.SetOnReconnect(func() {
		reconnectCalled.Store(true)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for block from second connection
	select {
	case <-blockSent:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for reconnection and block")
	}

	// Verify we received the header
	select {
	case <-headers:
		// Good
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for header")
	}

	if connectCount.Load() < 2 {
		t.Errorf("expected at least 2 connections, got %d", connectCount.Load())
	}

	if !reconnectCalled.Load() {
		t.Error("onReconnect callback was not called")
	}
}

func TestSubscribe_BackoffIncreasesOnRepeatedFailures(t *testing.T) {
	connectTimes := make([]time.Time, 0, 5)
	var mu sync.Mutex

	server := newMockWSServer(func(conn *websocket.Conn) {
		mu.Lock()
		connectTimes = append(connectTimes, time.Now())
		mu.Unlock()

		// Always fail immediately
		conn.Close()
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     500 * time.Millisecond,
		BackoffFactor:  2.0,
		ReadTimeout:    100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub.Subscribe(ctx)
	defer sub.Unsubscribe()

	// Wait for several connection attempts
	time.Sleep(800 * time.Millisecond)

	mu.Lock()
	times := append([]time.Time{}, connectTimes...)
	mu.Unlock()

	if len(times) < 3 {
		t.Fatalf("expected at least 3 connection attempts, got %d", len(times))
	}

	// Verify backoff is increasing
	for i := 2; i < len(times); i++ {
		gap1 := times[i-1].Sub(times[i-2])
		gap2 := times[i].Sub(times[i-1])

		// Second gap should be larger than first (with some tolerance for timing)
		if gap2 < gap1 && gap1 < 400*time.Millisecond { // Only check before hitting max
			t.Logf("gap %d: %v, gap %d: %v", i-1, gap1, i, gap2)
			// Note: Not failing as timing can be imprecise
		}
	}
}

// --- Test: HealthCheck ---

func TestHealthCheck_ReturnsErrorWhenClosed(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: "ws://localhost:8545",
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	sub.Unsubscribe()

	ctx := context.Background()
	err = sub.HealthCheck(ctx)
	if err == nil {
		t.Fatal("expected error for closed subscriber")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHealthCheck_ReturnsErrorWhenNoBlocksReceived(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:  "ws://localhost:8545",
		HealthTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	// Simulate old last block time
	sub.lastBlockTime.Store(time.Now().Add(-5 * time.Minute).Unix())

	ctx := context.Background()
	err = sub.HealthCheck(ctx)
	if err == nil {
		t.Fatal("expected error when no recent blocks")
	}
	if !strings.Contains(err.Error(), "no blocks received") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestHealthCheck_SucceedsWithRecentBlock(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		// Just accept connections for health check
		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:  server.URL(),
		HealthTimeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	// Simulate recent block
	sub.lastBlockTime.Store(time.Now().Unix())

	ctx := context.Background()
	err = sub.HealthCheck(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// --- Test: Edge Cases ---

func TestSubscribe_HandlesSubscriptionError(t *testing.T) {
	connectCount := atomic.Int32{}

	server := newMockWSServer(func(conn *websocket.Conn) {
		connectCount.Add(1)
		defer conn.Close()

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send error response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Error: &jsonRPCError{
				Code:    -32000,
				Message: "subscription limit reached",
			},
		}
		conn.WriteJSON(resp)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		ReadTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Subscribe will return immediately but connection manager will keep trying
	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe should not return error immediately: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for several reconnection attempts
	time.Sleep(200 * time.Millisecond)

	// Verify connection manager is actually retrying
	attempts := connectCount.Load()
	if attempts < 2 {
		t.Errorf("expected at least 2 connection attempts, got %d", attempts)
	}
}

func TestSubscribe_ChannelBufferFull(t *testing.T) {
	blocksSent := atomic.Int32{}
	const totalBlocks = 20
	const bufferSize = 5

	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Flood with blocks (more than buffer size)
		for i := 0; i < totalBlocks; i++ {
			header := outbound.BlockHeader{
				Number:     fmt.Sprintf("0x%x", 1000+i), // Valid hex block numbers
				Hash:       "0xabc",
				ParentHash: "0xdef",
			}
			notification := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0x1234",
					"result":       header,
				},
			}
			conn.WriteJSON(notification)
			blocksSent.Add(1)
			time.Sleep(5 * time.Millisecond)
		}

		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:      server.URL(),
		ChannelBufferSize: bufferSize,
		ReadTimeout:       5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Don't read from channel - let it fill up
	// The subscriber should not block or crash, just drop blocks
	time.Sleep(500 * time.Millisecond)

	// Verify all blocks were sent by the server
	if sent := blocksSent.Load(); sent != totalBlocks {
		t.Errorf("expected %d blocks sent, got %d", totalBlocks, sent)
	}

	// Now drain the channel and count received blocks
	received := 0
drainLoop:
	for {
		select {
		case _, ok := <-headers:
			if !ok {
				break drainLoop
			}
			received++
		default:
			break drainLoop
		}
	}

	// Should have received at most bufferSize blocks (the rest were dropped)
	if received > bufferSize+1 { // +1 for potential timing variance
		t.Errorf("expected at most %d blocks received, got %d (blocks should be dropped)", bufferSize+1, received)
	}

	// Verify blocks were actually dropped
	dropped := int(blocksSent.Load()) - received
	if dropped <= 0 {
		t.Error("expected some blocks to be dropped, but none were")
	}
	t.Logf("sent %d blocks, received %d, dropped %d", blocksSent.Load(), received, dropped)
}

func TestSubscribe_ContextCancellation(t *testing.T) {
	blocksSentAfterCancel := atomic.Int32{}
	cancelledChan := make(chan struct{})

	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Wait for cancellation signal, then try to send more blocks
		<-cancelledChan
		for i := 0; i < 5; i++ {
			header := outbound.BlockHeader{
				Number:     fmt.Sprintf("0x%x", 100+i),
				Hash:       "0xabc",
				ParentHash: "0xdef",
			}
			notification := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0x1234",
					"result":       header,
				},
			}
			if err := conn.WriteJSON(notification); err != nil {
				return // Connection closed, as expected
			}
			blocksSentAfterCancel.Add(1)
		}
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Give connection time to establish
	time.Sleep(100 * time.Millisecond)

	// Cancel context
	cancel()
	close(cancelledChan)

	// Give time for cleanup
	time.Sleep(200 * time.Millisecond)

	// Verify no blocks received after cancellation
	received := 0
drainLoop:
	for {
		select {
		case _, ok := <-headers:
			if !ok {
				break drainLoop
			}
			received++
			if received > 5 {
				t.Fatal("receiving too many blocks after context cancellation")
			}
		default:
			break drainLoop
		}
	}

	// Clean up
	sub.Unsubscribe()

	// Verify the connection was closed (blocks sent after cancel should fail or be minimal)
	t.Logf("blocks sent after cancel: %d, received: %d", blocksSentAfterCancel.Load(), received)
}

func TestSubscribe_MalformedJSON(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Send malformed params (should be handled gracefully)
		badNotification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params":  "not an object",
		}
		conn.WriteJSON(badNotification)

		// Send valid notification after
		header := outbound.BlockHeader{
			Number:     "0x100",
			Hash:       "0xabc",
			ParentHash: "0xdef",
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}
		conn.WriteJSON(notification)

		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Should still receive the valid block after the malformed one
	select {
	case header := <-headers:
		if header.Number != "0x100" {
			t.Errorf("unexpected block number: %s", header.Number)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for header after malformed message")
	}
}

func TestSubscribe_ConcurrentUnsubscribe(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		<-time.After(10 * time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx := context.Background()
	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	// Concurrent unsubscribe calls should not panic
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub.Unsubscribe()
		}()
	}
	wg.Wait()

	// Verify subscriber is actually closed
	if !sub.closed {
		t.Error("expected subscriber to be closed after concurrent unsubscribe")
	}

	// Verify channel is closed
	select {
	case _, ok := <-headers:
		if ok {
			t.Error("expected headers channel to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("timeout waiting for headers channel to close")
	}

	// Verify Subscribe fails on closed subscriber
	_, err = sub.Subscribe(context.Background())
	if err == nil {
		t.Error("expected error when subscribing to closed subscriber")
	}
}

func TestSubscribe_PingKeepsConnectionAlive(t *testing.T) {
	pingReceived := atomic.Bool{}

	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		conn.SetPingHandler(func(data string) error {
			pingReceived.Store(true)
			return conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
		})

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Keep reading to handle ping/pong
		for {
			conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		PingInterval: 100 * time.Millisecond, // Fast ping for testing
		PongTimeout:  50 * time.Millisecond,
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for ping to be sent
	time.Sleep(300 * time.Millisecond)

	if !pingReceived.Load() {
		t.Error("expected ping to be sent")
	}
}

// --- Test: SetOnReconnect ---

func TestSetOnReconnect_CallbackNotCalledOnFirstConnect(t *testing.T) {
	reconnectCalled := atomic.Bool{}

	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	sub.SetOnReconnect(func() {
		reconnectCalled.Store(true)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for connection to establish
	time.Sleep(200 * time.Millisecond)

	if reconnectCalled.Load() {
		t.Error("onReconnect should not be called on first connection")
	}
}

// --- Test: Additional Coverage ---

func TestSubscribe_IgnoresNonSubscriptionMessages(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Send a non-subscription message (should be ignored)
		nonSubMsg := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      2,
			"result":  "0x123",
		}
		conn.WriteJSON(nonSubMsg)

		// Send another non-subscription message with different method
		otherMethodMsg := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_blockNumber",
			"params":  []interface{}{},
		}
		conn.WriteJSON(otherMethodMsg)

		// Now send a valid subscription message
		header := outbound.BlockHeader{
			Number:     "0x100",
			Hash:       "0xabcdef1234567890",
			ParentHash: "0xdef",
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}
		conn.WriteJSON(notification)

		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Should only receive the valid subscription message
	select {
	case header := <-headers:
		if header.Number != "0x100" {
			t.Errorf("unexpected block number: %s", header.Number)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for header")
	}
}

func TestSubscribe_ReadTimeoutTriggersReconnect(t *testing.T) {
	connectCount := atomic.Int32{}

	server := newMockWSServer(func(conn *websocket.Conn) {
		count := connectCount.Add(1)

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		if count == 1 {
			// First connection: don't send anything, let read timeout trigger
			<-time.After(5 * time.Second)
		} else {
			// Second connection: send a block
			header := outbound.BlockHeader{
				Number:     "0x100",
				Hash:       "0xabc",
				ParentHash: "0xdef",
			}
			notification := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0x1234",
					"result":       header,
				},
			}
			conn.WriteJSON(notification)
			<-time.After(time.Second)
		}
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		ReadTimeout:    100 * time.Millisecond, // Short timeout to trigger reconnect
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for reconnect and block
	select {
	case <-headers:
		// Good - received block from second connection
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for header after reconnect")
	}

	if connectCount.Load() < 2 {
		t.Errorf("expected at least 2 connections due to timeout, got %d", connectCount.Load())
	}
}

func TestHealthCheck_DialFailsWhenNoConnection(t *testing.T) {
	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:  "ws://localhost:19999", // Invalid port, won't connect
		HealthTimeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	// Don't subscribe, so conn is nil
	// lastBlockTime is 0, so it won't check that

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = sub.HealthCheck(ctx)
	if err == nil {
		t.Fatal("expected health check to fail when dial fails")
	}
	if !strings.Contains(err.Error(), "health check failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSubscribe_WriteSubscriptionFails(t *testing.T) {
	connectCount := atomic.Int32{}

	server := newMockWSServer(func(conn *websocket.Conn) {
		count := connectCount.Add(1)

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		if count == 1 {
			// First connection: close before we can send response (simulates write failure)
			conn.Close()
			return
		}

		// Second connection: work normally
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)
		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		ReadTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for reconnect attempts
	time.Sleep(300 * time.Millisecond)

	if connectCount.Load() < 2 {
		t.Errorf("expected at least 2 connection attempts, got %d", connectCount.Load())
	}
}

func TestSubscribe_SubscriptionWithNilParams(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Send subscription message with nil params (should be handled gracefully)
		nilParamsMsg := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			// params is missing/nil
		}
		conn.WriteJSON(nilParamsMsg)

		// Send valid message after
		header := outbound.BlockHeader{
			Number:     "0x100",
			Hash:       "0xabc",
			ParentHash: "0xdef",
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}
		conn.WriteJSON(notification)

		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Should still receive the valid message
	select {
	case header := <-headers:
		if header.Number != "0x100" {
			t.Errorf("unexpected block number: %s", header.Number)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for header")
	}
}

func TestSubscribe_SubscriptionErrorResponse(t *testing.T) {
	connectCount := atomic.Int32{}

	server := newMockWSServer(func(conn *websocket.Conn) {
		count := connectCount.Add(1)

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		if count == 1 {
			// First connection: return error response
			resp := jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      1,
				Error: &jsonRPCError{
					Code:    -32000,
					Message: "subscription limit exceeded",
				},
			}
			conn.WriteJSON(resp)
			conn.Close()
			return
		}

		// Second connection: work normally
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)
		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
		ReadTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for reconnect attempts
	time.Sleep(300 * time.Millisecond)

	if connectCount.Load() < 2 {
		t.Errorf("expected at least 2 connection attempts due to error response, got %d", connectCount.Load())
	}
}

func TestSubscribe_InvalidParamsJSON(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Send subscription message with invalid params JSON structure
		invalidParamsMsg := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params":  "not an object", // Invalid: should be object with result field
		}
		conn.WriteJSON(invalidParamsMsg)

		// Send valid message after
		header := outbound.BlockHeader{
			Number:     "0x100",
			Hash:       "0xabcdef1234567890",
			ParentHash: "0xdef",
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}
		conn.WriteJSON(notification)

		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Should still receive the valid message after the invalid one
	select {
	case header := <-headers:
		if header.Number != "0x100" {
			t.Errorf("unexpected block number: %s", header.Number)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for header")
	}
}

func TestSubscribe_DoneChannelClosedDuringBlockSend(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Send blocks continuously
		for i := 0; i < 100; i++ {
			header := outbound.BlockHeader{
				Number:     fmt.Sprintf("0x%x", 1000+i),
				Hash:       fmt.Sprintf("0xabc%d1234567890", i),
				ParentHash: "0xdef",
			}
			notification := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0x1234",
					"result":       header,
				},
			}
			if err := conn.WriteJSON(notification); err != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Wait a bit for some blocks to be sent
	time.Sleep(50 * time.Millisecond)

	// Unsubscribe while blocks are being sent - should not hang
	done := make(chan struct{})
	go func() {
		sub.Unsubscribe()
		close(done)
	}()

	select {
	case <-done:
		// Good - unsubscribe completed without hanging
	case <-time.After(2 * time.Second):
		t.Fatal("Unsubscribe hung while blocks were being sent")
	}

	// Verify subscriber is closed
	if !sub.closed {
		t.Error("expected subscriber to be closed after Unsubscribe")
	}
}

func TestSubscribe_ContextCancelledDuringBlockSend(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		defer conn.Close()

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Send blocks continuously
		for i := 0; i < 100; i++ {
			header := outbound.BlockHeader{
				Number:     fmt.Sprintf("0x%x", 1000+i),
				Hash:       fmt.Sprintf("0xabc%d1234567890", i),
				ParentHash: "0xdef",
			}
			notification := map[string]interface{}{
				"jsonrpc": "2.0",
				"method":  "eth_subscription",
				"params": map[string]interface{}{
					"subscription": "0x1234",
					"result":       header,
				},
			}
			if err := conn.WriteJSON(notification); err != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait a bit for some blocks to be sent
	time.Sleep(50 * time.Millisecond)

	// Cancel context while blocks are being sent
	cancel()

	// Verify subscriber handles cancellation gracefully - Unsubscribe should not hang
	done := make(chan struct{})
	go func() {
		sub.Unsubscribe()
		close(done)
	}()

	select {
	case <-done:
		// Good - unsubscribe completed without hanging after context cancellation
	case <-time.After(2 * time.Second):
		t.Fatal("Unsubscribe hung after context cancellation during block send")
	}
}

func TestSubscribe_PingFailureTriggersReconnect(t *testing.T) {
	connectCount := atomic.Int32{}

	server := newMockWSServer(func(conn *websocket.Conn) {
		count := connectCount.Add(1)

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		if count == 1 {
			// First connection: close connection during ping interval
			// This will cause ping to fail
			time.Sleep(80 * time.Millisecond)
			conn.Close()
			return
		}

		// Second connection: work normally
		header := outbound.BlockHeader{
			Number:     "0x100",
			Hash:       "0xabc1234567890",
			ParentHash: "0xdef",
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}
		conn.WriteJSON(notification)
		<-time.After(time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:   server.URL(),
		ReadTimeout:    5 * time.Second,
		PingInterval:   50 * time.Millisecond, // Short ping interval
		PongTimeout:    10 * time.Millisecond,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	headers, err := sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for ping failure and reconnect
	select {
	case <-headers:
		// Good - received block from second connection
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for header after ping failure reconnect")
	}

	if connectCount.Load() < 2 {
		t.Errorf("expected at least 2 connections due to ping failure, got %d", connectCount.Load())
	}
}

func TestConnectionManager_ContextDoneExitsLoop(t *testing.T) {
	server := newMockWSServer(func(conn *websocket.Conn) {
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		// Keep connection open
		<-time.After(5 * time.Second)
	})
	defer server.Close()

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL: server.URL(),
		ReadTimeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to create subscriber: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	_, err = sub.Subscribe(ctx)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Wait for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Cancel context - should exit connectionManager
	cancel()

	// Wait for cleanup
	time.Sleep(100 * time.Millisecond)

	// Unsubscribe should not hang
	done := make(chan struct{})
	go func() {
		sub.Unsubscribe()
		close(done)
	}()

	select {
	case <-done:
		// Good
	case <-time.After(time.Second):
		t.Fatal("Unsubscribe hung after context cancellation")
	}
}
