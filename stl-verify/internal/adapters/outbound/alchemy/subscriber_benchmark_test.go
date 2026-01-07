package alchemy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// BenchmarkSubscriber_MessageLatency measures the time from when a message
// is sent from the WebSocket server until it is received in the headers channel.
func BenchmarkSubscriber_MessageLatency(b *testing.B) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	// Channel to signal when subscriber is ready
	ready := make(chan struct{})
	// Channel to receive the server connection
	connChan := make(chan *websocket.Conn, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		// Read subscription request
		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			conn.Close()
			return
		}

		// Send subscription response
		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		if err := conn.WriteJSON(resp); err != nil {
			b.Errorf("failed to write JSON: %v", err)
			return
		}

		// Signal ready and send connection for benchmark to use
		connChan <- conn
		close(ready)

		// Keep connection open until test ends
		<-time.After(time.Minute)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:      wsURL,
		ReadTimeout:       30 * time.Second,
		ChannelBufferSize: b.N + 100, // Ensure buffer is large enough
	})
	if err != nil {
		b.Fatalf("failed to create subscriber: %v", err)
	}

	ctx := context.Background()
	headers, err := sub.Subscribe(ctx)
	if err != nil {
		b.Fatalf("failed to subscribe: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			b.Logf("unsubscribe returned error: %v", err)
		}
	}()

	// Wait for connection to be ready
	<-ready
	conn := <-connChan

	// Collect latencies for statistics
	latencies := make([]time.Duration, 0, b.N)

	// Reset timer before benchmark loop
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Create block header message
		header := outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", 1000+i),
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}

		// Send message and measure time until received
		startTime := time.Now()
		if err := conn.WriteJSON(notification); err != nil {
			b.Fatalf("failed to write message: %v", err)
		}

		// Wait for header to be received
		select {
		case <-headers:
			latencies = append(latencies, time.Since(startTime))
		case <-time.After(5 * time.Second):
			b.Fatal("timeout waiting for header")
		}
	}

	b.StopTimer()

	// Calculate and report statistics
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		var total time.Duration
		for _, l := range latencies {
			total += l
		}
		avg := total / time.Duration(len(latencies))
		min := latencies[0]
		max := latencies[len(latencies)-1]
		p50 := latencies[len(latencies)*50/100]
		p99 := latencies[len(latencies)*99/100]

		b.ReportMetric(float64(avg.Microseconds()), "avg_µs")
		b.ReportMetric(float64(min.Microseconds()), "min_µs")
		b.ReportMetric(float64(max.Microseconds()), "max_µs")
		b.ReportMetric(float64(p50.Microseconds()), "p50_µs")
		b.ReportMetric(float64(p99.Microseconds()), "p99_µs")
	}
}

// BenchmarkSubscriber_Throughput measures how many messages per second
// can be processed through the subscriber.
func BenchmarkSubscriber_Throughput(b *testing.B) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	ready := make(chan struct{})
	connChan := make(chan *websocket.Conn, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			conn.Close()
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		connChan <- conn
		close(ready)

		<-time.After(time.Minute)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:      wsURL,
		ReadTimeout:       30 * time.Second,
		ChannelBufferSize: b.N + 100,
	})
	if err != nil {
		b.Fatalf("failed to create subscriber: %v", err)
	}

	ctx := context.Background()
	headers, err := sub.Subscribe(ctx)
	if err != nil {
		b.Fatalf("failed to subscribe: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			b.Logf("unsubscribe returned error: %v", err)
		}
	}()

	<-ready
	conn := <-connChan

	// Pre-build all messages
	messages := make([]map[string]interface{}, b.N)
	for i := 0; i < b.N; i++ {
		header := outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", 1000+i),
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
		}
		messages[i] = map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}
	}

	b.ResetTimer()
	startTime := time.Now()

	// Send all messages as fast as possible
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			if err := conn.WriteJSON(messages[i]); err != nil {
				return
			}
		}
	}()

	// Receive all messages
	received := 0
	timeout := time.After(30 * time.Second)
	for received < b.N {
		select {
		case <-headers:
			received++
		case <-timeout:
			b.Fatalf("timeout: received %d/%d messages", received, b.N)
		}
	}

	wg.Wait()
	b.StopTimer()

	elapsed := time.Since(startTime)
	msgsPerSec := float64(b.N) / elapsed.Seconds()

	b.ReportMetric(msgsPerSec, "msgs/sec")
	b.ReportMetric(float64(elapsed.Milliseconds()), "total_ms")
	b.ReportMetric(float64(received), "received")
}

// BenchmarkSubscriber_LatencyPercentiles measures latency distribution
// by collecting individual message latencies and reporting detailed percentiles.
// Run with -benchtime=1000x or higher for meaningful percentile data.
func BenchmarkSubscriber_LatencyPercentiles(b *testing.B) {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	ready := make(chan struct{})
	connChan := make(chan *websocket.Conn, 1)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}

		var req jsonRPCRequest
		if err := conn.ReadJSON(&req); err != nil {
			conn.Close()
			return
		}

		resp := jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  json.RawMessage(`"0x1234"`),
		}
		conn.WriteJSON(resp)

		connChan <- conn
		close(ready)

		<-time.After(time.Minute)
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	sub, err := NewSubscriber(SubscriberConfig{
		WebSocketURL:      wsURL,
		ReadTimeout:       30 * time.Second,
		ChannelBufferSize: b.N + 100,
	})
	if err != nil {
		b.Fatalf("failed to create subscriber: %v", err)
	}

	ctx := context.Background()
	headers, err := sub.Subscribe(ctx)
	if err != nil {
		b.Fatalf("failed to subscribe: %v", err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			b.Logf("unsubscribe returned error: %v", err)
		}
	}()

	<-ready
	conn := <-connChan

	latencies := make([]time.Duration, 0, b.N)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		header := outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", 1000+i),
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
		}
		notification := map[string]interface{}{
			"jsonrpc": "2.0",
			"method":  "eth_subscription",
			"params": map[string]interface{}{
				"subscription": "0x1234",
				"result":       header,
			},
		}

		start := time.Now()
		if err := conn.WriteJSON(notification); err != nil {
			b.Fatalf("failed to write message: %v", err)
		}

		select {
		case <-headers:
			latencies = append(latencies, time.Since(start))
		case <-time.After(5 * time.Second):
			b.Fatal("timeout waiting for header")
		}
	}

	b.StopTimer()

	// Calculate and report percentiles
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

		var total time.Duration
		for _, l := range latencies {
			total += l
		}

		n := len(latencies)
		avg := total / time.Duration(n)
		min := latencies[0]
		max := latencies[n-1]
		p50 := latencies[n*50/100]
		p75 := latencies[n*75/100]
		p90 := latencies[n*90/100]
		p95 := latencies[n*95/100]
		p99 := latencies[n*99/100]

		b.ReportMetric(float64(avg.Microseconds()), "avg_µs")
		b.ReportMetric(float64(min.Microseconds()), "min_µs")
		b.ReportMetric(float64(max.Microseconds()), "max_µs")
		b.ReportMetric(float64(p50.Microseconds()), "p50_µs")
		b.ReportMetric(float64(p75.Microseconds()), "p75_µs")
		b.ReportMetric(float64(p90.Microseconds()), "p90_µs")
		b.ReportMetric(float64(p95.Microseconds()), "p95_µs")
		b.ReportMetric(float64(p99.Microseconds()), "p99_µs")
	}
}
