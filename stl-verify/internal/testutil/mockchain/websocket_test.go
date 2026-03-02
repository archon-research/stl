package mockchain

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
	"github.com/gorilla/websocket"
)

// newTestWSServer creates a test HTTP server backed by a wsHandler.
func newTestWSServer(t *testing.T) (*httptest.Server, *wsHandler) {
	t.Helper()
	h := newWSHandler()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, h
}

// dialWS connects a WebSocket client to the test server URL.
func dialWS(t *testing.T, rawURL string) *websocket.Conn {
	t.Helper()
	wsURL := strings.Replace(rawURL, "http://", "ws://", 1)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// doSubscribe performs the eth_subscribe handshake and returns the subscription ID.
// It also asserts that the response carries jsonrpc "2.0".
func doSubscribe(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	req := rpcutil.Request{ID: json.RawMessage("1"), Method: "eth_subscribe", Params: json.RawMessage(`["newHeads"]`)}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var resp jsonRPCResponse
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("read subscribe response: %v", err)
	}
	if resp.JsonRPC != "2.0" {
		t.Errorf("expected jsonrpc 2.0, got %q", resp.JsonRPC)
	}
	if string(resp.ID) != string(req.ID) {
		t.Errorf("expected response id %s, got %s", req.ID, resp.ID)
	}
	return resp.Result
}

// TestNewWSHandler verifies the constructor initialises the handler correctly.
func TestNewWSHandler(t *testing.T) {
	h := newWSHandler()
	if h.subID != "0x1" {
		t.Errorf("expected subID 0x1, got %q", h.subID)
	}
	if h.conn != nil {
		t.Error("expected conn to be nil at construction")
	}
}

// TestWSHandler_Subscribe verifies the eth_subscribe handshake returns the correct subscription ID.
func TestWSHandler_Subscribe(t *testing.T) {
	srv, _ := newTestWSServer(t)
	conn := dialWS(t, srv.URL)

	subID := doSubscribe(t, conn)
	if subID != "0x1" {
		t.Errorf("expected subID 0x1, got %q", subID)
	}
}

// TestWSHandler_Broadcast verifies that Broadcast pushes a correctly shaped notification to the client.
func TestWSHandler_Broadcast(t *testing.T) {
	srv, h := newTestWSServer(t)
	conn := dialWS(t, srv.URL)
	doSubscribe(t, conn) // ensures h.conn is set before Broadcast

	want := outbound.BlockHeader{Number: "0x1", Hash: "0xabc"}
	h.Broadcast(want)

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var n jsonRPCNotification
	if err := conn.ReadJSON(&n); err != nil {
		t.Fatalf("read notification: %v", err)
	}

	if n.JsonRPC != "2.0" {
		t.Errorf("expected jsonrpc 2.0, got %q", n.JsonRPC)
	}
	if n.Method != "eth_subscription" {
		t.Errorf("expected method eth_subscription, got %q", n.Method)
	}
	if n.Params.Subscription != "0x1" {
		t.Errorf("expected subscription 0x1, got %q", n.Params.Subscription)
	}
	if n.Params.Result != want {
		t.Errorf("expected header %+v, got %+v", want, n.Params.Result)
	}
}

// TestWSHandler_BroadcastNoClient verifies that Broadcast does not panic when no client is connected.
func TestWSHandler_BroadcastNoClient(t *testing.T) {
	h := newWSHandler()
	h.Broadcast(outbound.BlockHeader{Number: "0x1"}) // must not panic
}

// TestWSHandler_InvalidParams verifies that an eth_subscribe with an unsupported type returns a JSON-RPC error.
func TestWSHandler_InvalidParams(t *testing.T) {
	srv, _ := newTestWSServer(t)
	conn := dialWS(t, srv.URL)

	req := rpcutil.Request{ID: json.RawMessage("2"), Method: "eth_subscribe", Params: json.RawMessage(`["logs"]`)}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var errResp jsonRPCErrorResponse
	if err := conn.ReadJSON(&errResp); err != nil {
		t.Fatalf("read error response: %v", err)
	}

	if errResp.JsonRPC != "2.0" {
		t.Errorf("expected jsonrpc 2.0, got %q", errResp.JsonRPC)
	}
	if string(errResp.ID) != "2" {
		t.Errorf("expected id 2, got %s", errResp.ID)
	}
	if errResp.Error.Code != -32602 {
		t.Errorf("expected error code -32602, got %d", errResp.Error.Code)
	}
}

// TestWSHandler_UnknownMethod verifies that an unsupported method returns a JSON-RPC method-not-found error.
func TestWSHandler_UnknownMethod(t *testing.T) {
	srv, _ := newTestWSServer(t)
	conn := dialWS(t, srv.URL)

	req := rpcutil.Request{ID: json.RawMessage("3"), Method: "eth_chainId", Params: nil}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var errResp jsonRPCErrorResponse
	if err := conn.ReadJSON(&errResp); err != nil {
		t.Fatalf("read error response: %v", err)
	}

	if errResp.JsonRPC != "2.0" {
		t.Errorf("expected jsonrpc 2.0, got %q", errResp.JsonRPC)
	}
	if errResp.Error.Code != -32601 {
		t.Errorf("expected error code -32601, got %d", errResp.Error.Code)
	}
}

// TestWSHandler_DisconnectClearsConn verifies that ServeHTTP sets conn to nil when the client disconnects.
func TestWSHandler_DisconnectClearsConn(t *testing.T) {
	srv, h := newTestWSServer(t)
	conn := dialWS(t, srv.URL)
	doSubscribe(t, conn) // ensures h.conn is set

	conn.Close()

	// ServeHTTP detects the disconnect asynchronously; poll with a timeout.
	testutil.WaitForCondition(t, 2*time.Second, func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return h.conn == nil
	}, "h.conn to be nil after client disconnect")
}

// TestWSHandler_ReplaceConn verifies that a second connection closes the first and takes over.
func TestWSHandler_ReplaceConn(t *testing.T) {
	srv, h := newTestWSServer(t)

	// First client connects and subscribes.
	conn1 := dialWS(t, srv.URL)
	doSubscribe(t, conn1)

	// Second client connects; server should close conn1's server-side connection.
	conn2 := dialWS(t, srv.URL)
	// doSubscribe on conn2 is a synchronisation point: by the time the server
	// writes its response, it has already closed conn1 and set h.conn = conn2.
	doSubscribe(t, conn2)

	// conn1's underlying connection was closed by the server; a read must fail.
	if err := conn1.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var ignored any
	if err := conn1.ReadJSON(&ignored); err == nil {
		t.Error("expected conn1 read to fail after server replaced it with conn2")
	}

	// Broadcast must reach conn2, not conn1.
	want := outbound.BlockHeader{Number: "0x2", Hash: "0xdef"}
	h.Broadcast(want)

	if err := conn2.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var n jsonRPCNotification
	if err := conn2.ReadJSON(&n); err != nil {
		t.Fatalf("read notification on conn2: %v", err)
	}
	if n.Params.Result != want {
		t.Errorf("expected %+v, got %+v", want, n.Params.Result)
	}
}
