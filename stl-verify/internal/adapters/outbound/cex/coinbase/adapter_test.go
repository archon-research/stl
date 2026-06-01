package coinbase_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex/coinbase"
)

const bookResponse = `{
	"bids": [["50000.00","1.2",1],["49999.50","0.4",2]],
	"asks": [["50001.00","0.8",1],["50002.00","1.1",3]],
	"sequence": 99999
}`

func newTestAdapter(t *testing.T, server *httptest.Server) *coinbase.Adapter {
	t.Helper()
	a, err := coinbase.NewAdapter(coinbase.Config{
		BaseURL:           server.URL,
		PollInterval:      10 * time.Millisecond,
		ChannelBufferSize: 10,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	return a
}

func TestAdapter_Name(t *testing.T) {
	a, err := coinbase.NewAdapter(coinbase.Config{})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	if got := a.Name(); got != "coinbase" {
		t.Errorf("Name() = %q, want %q", got, "coinbase")
	}
}

func TestNewAdapter_RejectsNegativeConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  coinbase.Config
	}{
		{name: "negative PollInterval", cfg: coinbase.Config{PollInterval: -1}},
		{name: "negative ChannelBufferSize", cfg: coinbase.Config{ChannelBufferSize: -1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := coinbase.NewAdapter(tt.cfg)
			if err == nil {
				t.Error("expected error for invalid config, got nil")
			}
		})
	}
}

func TestAdapter_Connect_RejectsEmptySymbols(t *testing.T) {
	a, _ := coinbase.NewAdapter(coinbase.Config{})
	if err := a.Connect(context.Background(), []string{}); err == nil {
		t.Error("Connect(empty) should return error")
	}
}

func TestAdapter_Connect_RejectsNilSymbols(t *testing.T) {
	a, _ := coinbase.NewAdapter(coinbase.Config{})
	if err := a.Connect(context.Background(), nil); err == nil {
		t.Error("Connect(nil) should return error")
	}
}

func TestAdapter_Connect_RejectsBlankSymbol(t *testing.T) {
	a, _ := coinbase.NewAdapter(coinbase.Config{})
	if err := a.Connect(context.Background(), []string{""}); err == nil {
		t.Error("Connect with blank symbol should return error")
	}
}

func TestAdapter_Stream_BeforeConnect_ReturnsError(t *testing.T) {
	a, _ := coinbase.NewAdapter(coinbase.Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, errCh := a.Stream(ctx)
	select {
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed without error")
		}
		if err == nil {
			t.Error("expected non-nil error for Stream before Connect")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for error")
	}
}

func TestAdapter_Stream_CalledTwice_ReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"bids":[],"asks":[],"sequence":1}`))
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	a.Stream(ctx) // first call
	_, errCh := a.Stream(ctx)

	select {
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed without error on second Stream call")
		}
		if err == nil {
			t.Error("expected non-nil error on second Stream call")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for error from second Stream call")
	}
}

func TestAdapter_Stream_EmitsNormalizedSnapshot(t *testing.T) {
	const symbol = "BTC-USD"
	var gotPath string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(bookResponse))
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.Connect(ctx, []string{symbol}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	snapCh, errCh := a.Stream(ctx)

	select {
	case snap, ok := <-snapCh:
		if !ok {
			t.Fatal("snapshot channel closed before first value")
		}
		if snap.Exchange != "coinbase" {
			t.Errorf("Exchange = %q, want %q", snap.Exchange, "coinbase")
		}
		if snap.Token != symbol {
			t.Errorf("Token = %q, want %q", snap.Token, symbol)
		}
		if snap.CapturedAt.IsZero() {
			t.Error("CapturedAt must not be zero")
		}
		if len(snap.Bids) != 2 {
			t.Errorf("len(Bids) = %d, want 2", len(snap.Bids))
		}
		if len(snap.Asks) != 2 {
			t.Errorf("len(Asks) = %d, want 2", len(snap.Asks))
		}
		if snap.Bids[0].Price != "50000.00" || snap.Bids[0].Qty != "1.2" {
			t.Errorf("Bids[0] = %+v, want {Price:50000.00 Qty:1.2}", snap.Bids[0])
		}
		if snap.Asks[0].Price != "50001.00" || snap.Asks[0].Qty != "0.8" {
			t.Errorf("Asks[0] = %+v, want {Price:50001.00 Qty:0.8}", snap.Asks[0])
		}
	case err := <-errCh:
		t.Fatalf("unexpected error: %v", err)
	case <-ctx.Done():
		t.Fatal("timeout waiting for snapshot")
	}

	if gotPath != "/products/BTC-USD/book" {
		t.Errorf("request path = %q, want /products/BTC-USD/book", gotPath)
	}
}

func TestAdapter_Stream_Non2xxReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	_, errCh := a.Stream(ctx)

	select {
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed without error")
		}
		if err == nil {
			t.Error("expected non-nil error for non-2xx response")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for error")
	}
}

func TestAdapter_Stream_MalformedJSONReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{invalid`))
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	_, errCh := a.Stream(ctx)

	select {
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed without error")
		}
		if err == nil {
			t.Error("expected non-nil error for malformed JSON")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for error")
	}
}

func TestAdapter_Close_StopsStreaming(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"bids":[],"asks":[],"sequence":1}`))
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := a.Connect(ctx, []string{"BTC-USD"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	snapCh, _ := a.Stream(ctx)
	_ = a.Close()

	timeout := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-snapCh:
			if !ok {
				return
			}
		case <-timeout:
			t.Fatal("snapshot channel not closed after Close()")
		}
	}
}
