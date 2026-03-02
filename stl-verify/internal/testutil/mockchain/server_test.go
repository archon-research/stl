package mockchain

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/gorilla/websocket"
)

// startTestServer starts a Server on a random port and registers cleanup.
func startTestServer(t *testing.T, store *DataStore) *Server {
	t.Helper()
	s := NewServer(store)
	if err := s.Start(":0"); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })
	return s
}

// serverPost sends a JSON-RPC POST to the server and returns the raw response body.
func serverPost(t *testing.T, s *Server, body string) []byte {
	t.Helper()
	url := fmt.Sprintf("http://%s", s.Addr().String())
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

// TestNewServer verifies the constructor initialises all fields and Addr is nil before Start.
func TestNewServer(t *testing.T) {
	store := NewFixtureDataStore()
	s := NewServer(store)

	if s.store == nil {
		t.Error("expected non-nil store")
	}
	if s.ws == nil {
		t.Error("expected non-nil ws handler")
	}
	if s.rpc == nil {
		t.Error("expected non-nil rpc handler")
	}
	if s.replayer == nil {
		t.Error("expected non-nil replayer")
	}
	if s.Addr() != nil {
		t.Error("expected nil Addr before Start")
	}
}

// TestServer_StartStop verifies Start binds successfully and Stop does not panic.
func TestServer_StartStop(t *testing.T) {
	s := startTestServer(t, NewFixtureDataStore())
	if s.Addr() == nil {
		t.Fatal("expected non-nil Addr after Start")
	}
}

// TestServer_Start_BadAddr verifies that Start returns an error for an invalid address.
func TestServer_Start_BadAddr(t *testing.T) {
	s := NewServer(NewFixtureDataStore())
	if err := s.Start(":notaport"); err == nil {
		t.Fatal("expected error for invalid address")
	}
}

// TestServer_WSSubscribe verifies that a WebSocket client can complete the eth_subscribe handshake.
func TestServer_WSSubscribe(t *testing.T) {
	s := startTestServer(t, NewFixtureDataStore())
	wsURL := fmt.Sprintf("ws://%s", s.Addr().String())
	conn := dialWS(t, wsURL)

	subID := doSubscribe(t, conn)
	if subID != "0x1" {
		t.Errorf("expected subID 0x1, got %q", subID)
	}
}

// TestServer_WSBroadcast verifies that Broadcast pushes a notification to the subscribed client.
func TestServer_WSBroadcast(t *testing.T) {
	s := startTestServer(t, NewFixtureDataStore())
	wsURL := fmt.Sprintf("ws://%s", s.Addr().String())
	conn := dialWS(t, wsURL)
	doSubscribe(t, conn)

	want := outbound.BlockHeader{Number: "0x5", Hash: "0xbeef"}
	s.ws.Broadcast(want)

	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var n jsonRPCNotification
	if err := conn.ReadJSON(&n); err != nil {
		t.Fatalf("read notification: %v", err)
	}
	if n.Method != "eth_subscription" {
		t.Errorf("expected method eth_subscription, got %q", n.Method)
	}
	if n.Params.Result != want {
		t.Errorf("expected %+v, got %+v", want, n.Params.Result)
	}
}

// TestServer_HTTP_BlockNumber verifies that eth_blockNumber returns a hex block number.
func TestServer_HTTP_BlockNumber(t *testing.T) {
	s := startTestServer(t, NewFixtureDataStore())
	raw := serverPost(t, s, `{"id":1,"method":"eth_blockNumber","params":[]}`)

	var resp httpRPCResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.ID != 1 {
		t.Errorf("expected id 1, got %d", resp.ID)
	}
	if len(resp.Result) == 0 {
		t.Error("expected non-empty result")
	}
}

// TestServer_HTTP_GetBlockByHash verifies that eth_getBlockByHash returns block data for a known derived hash.
func TestServer_HTTP_GetBlockByHash(t *testing.T) {
	store := NewFixtureDataStore()
	s := startTestServer(t, store)

	// Emit one block to populate the derived-hash map, then retrieve its hash.
	s.replayer.emit()
	header, ok := s.replayer.HeaderForNumber(s.replayer.CurrentBlockNumber())
	if !ok {
		t.Fatal("expected emitted block to be retrievable")
	}

	body := fmt.Sprintf(`{"id":2,"method":"eth_getBlockByHash","params":[%q,false]}`, header.Hash)
	raw := serverPost(t, s, body)

	var resp httpRPCResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.ID != 2 {
		t.Errorf("expected id 2, got %d", resp.ID)
	}
	if string(resp.Result) == "null" {
		t.Error("expected block data, got null")
	}
}

// TestServer_HTTP_UnknownMethod verifies that an unsupported method returns a -32601 JSON-RPC error.
func TestServer_HTTP_UnknownMethod(t *testing.T) {
	s := startTestServer(t, NewFixtureDataStore())
	raw := serverPost(t, s, `{"id":3,"method":"eth_chainId","params":[]}`)

	var errResp jsonRPCErrorResponse
	if err := json.Unmarshal(raw, &errResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if errResp.Error.Code != -32601 {
		t.Errorf("expected error code -32601, got %d", errResp.Error.Code)
	}
}

// TestServer_HTTP_Batch verifies that a batch request returns a matching array of responses.
func TestServer_HTTP_Batch(t *testing.T) {
	store := NewFixtureDataStore()
	s := startTestServer(t, store)

	// Emit one block to populate the derived-hash map.
	s.replayer.emit()
	header, ok := s.replayer.HeaderForNumber(s.replayer.CurrentBlockNumber())
	if !ok {
		t.Fatal("expected emitted block to be retrievable")
	}
	hash := header.Hash

	body := `[` +
		`{"jsonrpc":"2.0","id":10,"method":"eth_blockNumber","params":[]}` +
		`,{"jsonrpc":"2.0","id":11,"method":"eth_getBlockByHash","params":["` + hash + `",false]}` +
		`]`
	raw := serverPost(t, s, body)

	var responses []struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(raw, &responses); err != nil {
		t.Fatalf("unmarshal batch response: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("expected 2 responses, got %d", len(responses))
	}
	if responses[0].ID != 10 {
		t.Errorf("expected id 10, got %d", responses[0].ID)
	}
	if responses[1].ID != 11 {
		t.Errorf("expected id 11, got %d", responses[1].ID)
	}
	if len(responses[0].Result) == 0 {
		t.Error("expected non-empty blockNumber result")
	}
	if string(responses[1].Result) == "null" || len(responses[1].Result) == 0 {
		t.Error("expected non-null block result for known hash")
	}
}

// TestServer_Disconnect verifies that Server.Disconnect closes the active WebSocket
// connection so the client detects it.
func TestServer_Disconnect(t *testing.T) {
	s := startTestServer(t, NewFixtureDataStore())
	wsURL := fmt.Sprintf("ws://%s", s.Addr().String())
	conn := dialWS(t, wsURL)
	doSubscribe(t, conn)

	s.Disconnect()

	if err := conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var ignored any
	if err := conn.ReadJSON(&ignored); err == nil {
		t.Error("expected read to fail after Server.Disconnect()")
	}
}

// TestServer_SetInterval verifies that SetInterval changes the block emission rate.
// SetInterval must be called before Start.
func TestServer_SetInterval(t *testing.T) {
	s := NewServer(NewFixtureDataStore())
	s.SetInterval(50 * time.Millisecond)
	if err := s.Start(":0"); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })

	wsURL := fmt.Sprintf("ws://%s", s.Addr().String())
	conn := dialWS(t, wsURL)
	doSubscribe(t, conn)

	// With a 50ms interval, 3 blocks must arrive well within 2 seconds.
	for i := range 3 {
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		var n jsonRPCNotification
		if err := conn.ReadJSON(&n); err != nil {
			t.Fatalf("read notification %d: %v", i+1, err)
		}
	}
}

// TestServer_WSSubscriberLike replicates the exact connection pattern of the real
// alchemy subscriber: sets a read deadline before every ReadJSON and a pong handler.
// This catches any incompatibility between the mock server and the real watcher.
func TestServer_WSSubscriberLike(t *testing.T) {
	// Start server with a fast replayer so we don't wait 12s in CI.
	store := NewFixtureDataStore()
	s := NewServer(store)
	s.replayer.SetInterval(100 * time.Millisecond)
	if err := s.Start(":0"); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = s.Stop() })

	wsURL := fmt.Sprintf("ws://%s", s.Addr().String())
	dialer := websocket.Dialer{}
	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Mirror what alchemy.Subscriber.connectAndSubscribe does.
	readTimeout := 5 * time.Second
	if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readTimeout))
	})

	// Send eth_subscribe.
	subscribeReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_subscribe",
		"params":  []any{"newHeads"},
	}
	if err := conn.WriteJSON(subscribeReq); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}

	// Read the subscription acknowledgement.
	var subResp jsonRPCResponse
	if err := conn.ReadJSON(&subResp); err != nil {
		t.Fatalf("read subscribe response: %v", err)
	}
	if subResp.Result != "0x1" {
		t.Errorf("expected subscription id 0x1, got %q", subResp.Result)
	}

	// Mirror what alchemy.Subscriber.readLoop does: reset deadline before each read
	// and wait for at least 3 block notifications.
	received := 0
	for received < 3 {
		if err := conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		var msg jsonRPCNotification
		if err := conn.ReadJSON(&msg); err != nil {
			t.Fatalf("read notification %d: %v", received+1, err)
		}
		if msg.Method != "eth_subscription" {
			t.Errorf("expected method eth_subscription, got %q", msg.Method)
		}
		received++
	}
	if received < 3 {
		t.Errorf("expected 3 notifications, got %d", received)
	}
}
