package wsclient

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// fakeConn is a wsConn whose calls can be made to fail on demand, so the
// error paths that a real server cannot deterministically trigger (ping
// failure, dropped subsequent errors) can be exercised. ReadMessage blocks
// until Close, so the read pump does not race the path under test.
type fakeConn struct {
	readGate        chan struct{}
	closeOnce       sync.Once
	writeControlErr error
}

func newFakeConn() *fakeConn { return &fakeConn{readGate: make(chan struct{})} }

func (f *fakeConn) ReadMessage() (int, []byte, error) {
	<-f.readGate
	return 0, nil, errors.New("read after close")
}
func (f *fakeConn) SetReadDeadline(time.Time) error   { return nil }
func (f *fakeConn) SetPongHandler(func(string) error) {}
func (f *fakeConn) SetWriteDeadline(time.Time) error  { return nil }
func (f *fakeConn) WriteMessage(int, []byte) error    { return nil }
func (f *fakeConn) WriteJSON(any) error               { return nil }
func (f *fakeConn) WriteControl(int, []byte, time.Time) error {
	return f.writeControlErr
}
func (f *fakeConn) Close() error {
	f.closeOnce.Do(func() { close(f.readGate) })
	return nil
}

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
		if got.Type != websocket.TextMessage {
			t.Errorf("frame type = %d, want TextMessage", got.Type)
		}
		if string(got.Data) != want {
			t.Errorf("frame = %s, want %s", got.Data, want)
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

func TestConnWriteTextReachesServer(t *testing.T) {
	received := make(chan struct {
		mt   int
		data string
	}, 1)
	url := newTestServer(t, func(conn *websocket.Conn) {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		received <- struct {
			mt   int
			data string
		}{mt, string(data)}
		keepOpen(conn)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteText([]byte("ping-payload")); err != nil {
		t.Fatalf("WriteText: %v", err)
	}
	select {
	case got := <-received:
		if got.mt != websocket.TextMessage {
			t.Errorf("server received message type %d, want TextMessage", got.mt)
		}
		if got.data != "ping-payload" {
			t.Errorf("server received %q", got.data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive the text frame")
	}
}

// TestConnWriteJSONMarshalError exercises the error path where the value cannot
// be marshalled to JSON: WriteJSON must surface the error rather than send a
// malformed frame.
func TestConnWriteJSONMarshalError(t *testing.T) {
	url := newTestServer(t, keepOpen)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Channels are not JSON-marshalable.
	if err := conn.WriteJSON(make(chan int)); err == nil {
		t.Fatal("WriteJSON should return an error for an unmarshalable value")
	}
}

// TestConnWriteFailsAfterConnectionDrops covers the error path where the server
// drops the connection: once Next has surfaced the terminal error (so teardown
// has run), subsequent writes must fail rather than appear to succeed.
func TestConnWriteFailsAfterConnectionDrops(t *testing.T) {
	url := newTestServer(t, func(*websocket.Conn) {
		// Return immediately to drop the connection.
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Observing the terminal error guarantees the connection has been torn down.
	if _, err := conn.Next(ctx); err == nil {
		t.Fatal("Next should return an error after the server drops the connection")
	}

	if err := conn.WriteJSON(map[string]any{"op": "subscribe"}); err == nil {
		t.Error("WriteJSON should fail after the connection drops")
	}
	if err := conn.WriteText([]byte("hello")); err == nil {
		t.Error("WriteText should fail after the connection drops")
	}
	if err := conn.WriteBinary([]byte{0x01}); err == nil {
		t.Error("WriteBinary should fail after the connection drops")
	}
}

func TestConnBinaryFrameRoundTrip(t *testing.T) {
	received := make(chan []byte, 1)
	url := newTestServer(t, func(conn *websocket.Conn) {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if mt != websocket.BinaryMessage {
			t.Errorf("server received message type %d, want BinaryMessage", mt)
		}
		received <- data
		// Echo a binary frame back so the read side can be exercised too.
		if err := conn.WriteMessage(websocket.BinaryMessage, []byte{0xDE, 0xAD}); err != nil {
			t.Errorf("server write: %v", err)
		}
		keepOpen(conn)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if err := conn.WriteBinary([]byte{0x01, 0x02, 0x03}); err != nil {
		t.Fatalf("WriteBinary: %v", err)
	}
	select {
	case got := <-received:
		if string(got) != string([]byte{0x01, 0x02, 0x03}) {
			t.Errorf("server received %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not receive the binary frame")
	}

	frame, err := conn.Next(ctx)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if frame.Type != websocket.BinaryMessage {
		t.Errorf("frame type = %d, want BinaryMessage", frame.Type)
	}
	if string(frame.Data) != string([]byte{0xDE, 0xAD}) {
		t.Errorf("frame data = %v, want [0xDE 0xAD]", frame.Data)
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

func TestConnNextReturnsPromptlyAfterError(t *testing.T) {
	url := newTestServer(t, func(conn *websocket.Conn) {
		sendText(t, conn, `hello`)
	})

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := Dial(dialCtx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Next(dialCtx); err != nil {
		t.Fatalf("first Next should succeed, got %v", err)
	}
	// Drain the terminal error surfaced after the server closes the connection.
	if _, err := conn.Next(dialCtx); err == nil {
		t.Fatal("second Next should return the terminal error")
	}

	// A subsequent Next must return promptly (the connection is dead), not block
	// until the context deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := conn.Next(ctx); err == nil {
		t.Fatal("third Next should return an error")
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("third Next blocked until ctx deadline (%v); want prompt return", elapsed)
	}
	if ctx.Err() != nil {
		t.Fatal("third Next should not consume the context deadline")
	}
}

func TestConnWriteFailsAfterClose(t *testing.T) {
	url := newTestServer(t, keepOpen)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	conn.Close()

	tests := []struct {
		name string
		fn   func() error
	}{
		{"WriteJSON", func() error { return conn.WriteJSON(map[string]any{"op": "subscribe"}) }},
		{"WriteText", func() error { return conn.WriteText([]byte("hello")) }},
		{"WriteBinary", func() error { return conn.WriteBinary([]byte{0x01}) }},
	}
	for _, tt := range tests {
		if err := tt.fn(); err == nil {
			t.Errorf("%s should fail after the connection is closed", tt.name)
		}
	}
}

// TestConnNextSurfacesRealErrorNotErrClosed asserts that when the connection
// dies for a reason other than a caller-initiated Close, Next surfaces the real
// underlying error and NOT the ErrClosed sentinel. Consumers branch on
// errors.Is(err, ErrClosed) to decide "clean shutdown, stop" vs "connection
// dropped, reconnect"; masking a real drop as ErrClosed would make a block
// watcher exit instead of reconnecting. The server closes abnormally with no
// frames, so msgs stays empty and only the terminal-error path can fire. Run
// many iterations to defeat any select randomness between competing signals.
func TestConnNextSurfacesRealErrorNotErrClosed(t *testing.T) {
	for i := range 100 {
		url := newTestServer(t, func(*websocket.Conn) {
			// Return immediately: the deferred Close drops the TCP connection
			// abnormally (no close handshake) without sending any frame.
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := Dial(ctx, url, testConfig())
		if err != nil {
			cancel()
			t.Fatalf("iter %d: Dial: %v", i, err)
		}

		// Let the read error propagate fully: the read pump reports the error and
		// tears the connection down, so by the time Next runs both the terminal
		// error and the closed-done signal are ready. This is exactly the window
		// where a naive select can mask the real error behind ErrClosed.
		time.Sleep(20 * time.Millisecond)

		_, nextErr := conn.Next(ctx)
		conn.Close()
		cancel()

		if nextErr == nil {
			t.Fatalf("iter %d: Next should return the terminal read error", i)
		}
		if errors.Is(nextErr, ErrClosed) {
			t.Fatalf("iter %d: Next returned ErrClosed for an abnormal server drop; the real error was masked", i)
		}
	}
}

// TestConnNextReturnsErrClosedAfterCallerClose is the complement of the test
// above: when the CALLER closes the connection, Next must return the ErrClosed
// sentinel so consumers can recognise an intentional shutdown.
func TestConnNextReturnsErrClosedAfterCallerClose(t *testing.T) {
	url := newTestServer(t, keepOpen)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := Dial(ctx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	conn.Close()
	if _, err := conn.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next after caller Close = %v, want ErrClosed", err)
	}
}

// TestConnNextReturnsErrorOnReadDeadline exercises the read-deadline path: the
// server never sends anything, so the read deadline expires and Next surfaces
// the resulting error rather than blocking forever.
func TestConnNextReturnsErrorOnReadDeadline(t *testing.T) {
	url := newTestServer(t, keepOpen)

	cfg := testConfig()
	cfg.ReadTimeout = 50 * time.Millisecond

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := Dial(dialCtx, url, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := conn.Next(ctx); err == nil {
		t.Fatal("Next should return an error once the read deadline expires")
	} else if errors.Is(err, ErrClosed) {
		t.Fatalf("read-deadline failure should surface the real error, got ErrClosed")
	}
	if ctx.Err() != nil {
		t.Fatal("Next should return the deadline error itself, not block until ctx expires")
	}
}

// TestConnDeliversBufferedFramesBeforeDropError ensures that frames received
// just before the connection drops are all delivered before Next surfaces the
// terminal error, rather than being discarded when done is observed.
func TestConnDeliversBufferedFramesBeforeDropError(t *testing.T) {
	want := []string{`{"n":1}`, `{"n":2}`, `{"n":3}`}
	url := newTestServer(t, func(conn *websocket.Conn) {
		for _, frame := range want {
			sendText(t, conn, frame)
		}
		// Return to drop the connection right after sending.
	})

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dialCancel()
	conn, err := Dial(dialCtx, url, testConfig())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// Let every frame buffer and the drop register before reading.
	time.Sleep(50 * time.Millisecond)

	for _, w := range want {
		got, err := conn.Next(dialCtx)
		if err != nil {
			t.Fatalf("Next: got error %v before draining buffered frames", err)
		}
		if string(got.Data) != w {
			t.Errorf("frame = %s, want %s", got.Data, w)
		}
	}
	if _, err := conn.Next(dialCtx); err == nil {
		t.Fatal("Next should return the terminal error after draining frames")
	} else if errors.Is(err, ErrClosed) {
		t.Errorf("drop should surface the real error, got ErrClosed")
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

// TestConnPingFailureSurfacesError covers the pingLoop error path: when a
// keepalive ping write fails, the connection is torn down and Next surfaces the
// wrapped ping error.
func TestConnPingFailureSurfacesError(t *testing.T) {
	fake := newFakeConn()
	fake.writeControlErr = errors.New("boom")

	cfg := testConfig()
	cfg.PingInterval = 5 * time.Millisecond

	conn, err := newConn(fake, cfg)
	if err != nil {
		t.Fatalf("newConn: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = conn.Next(ctx)
	if err == nil {
		t.Fatal("Next should surface the ping failure")
	}
	if !strings.Contains(err.Error(), "ping:") || !strings.Contains(err.Error(), "boom") {
		t.Errorf("error = %v, want it to wrap the ping failure", err)
	}
	if errors.Is(err, ErrClosed) {
		t.Error("ping failure should surface the real error, not ErrClosed")
	}
}

// TestReportErrorKeepsFirstAndLogsRest covers reportError's CompareAndSwap-fails
// branch: the first error is retained as the terminal cause and subsequent
// errors are logged at debug rather than masking it.
func TestReportErrorKeepsFirstAndLogsRest(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))

	c := &Conn{
		conn:   newFakeConn(),
		cfg:    testConfig(),
		logger: logger,
		msgs:   make(chan Frame, 1),
		done:   make(chan struct{}),
	}

	first := errors.New("first error")
	second := errors.New("second error")
	c.reportError(first)
	c.reportError(second)

	if errp := c.termErr.Load(); errp == nil || *errp != first {
		t.Fatalf("termErr = %v, want %v", errp, first)
	}

	logs := buf.String()
	if !strings.Contains(logs, "dropping subsequent websocket error") {
		t.Errorf("expected debug log for the dropped error, got: %s", logs)
	}
	if !strings.Contains(logs, "second error") {
		t.Errorf("expected the dropped error to be logged, got: %s", logs)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := c.Next(ctx); err != first {
		t.Errorf("Next = %v, want the first error", err)
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
