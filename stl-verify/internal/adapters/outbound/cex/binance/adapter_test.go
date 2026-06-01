package binance_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex/binance"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time interface check.
var _ outbound.ExchangeOrderBookStreamer = (*binance.Adapter)(nil)

const depthResponse = `{
	"lastUpdateId": 12345,
	"bids": [["50000.00","1.2"],["49999.50","0.4"]],
	"asks": [["50001.00","0.8"],["50002.00","1.1"]]
}`

func newTestAdapter(t *testing.T, server *httptest.Server) *binance.Adapter {
	t.Helper()
	a, err := binance.NewAdapter(binance.Config{
		BaseURL:           server.URL,
		PollInterval:      10 * time.Millisecond,
		ChannelBufferSize: 10,
		DepthLimit:        100,
	})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	return a
}

func TestAdapter_Name(t *testing.T) {
	a, err := binance.NewAdapter(binance.Config{})
	if err != nil {
		t.Fatalf("NewAdapter: %v", err)
	}
	if got := a.Name(); got != "binance" {
		t.Errorf("Name() = %q, want %q", got, "binance")
	}
}

func TestAdapter_Connect_RejectsEmptySymbols(t *testing.T) {
	a, _ := binance.NewAdapter(binance.Config{})
	err := a.Connect(context.Background(), []string{})
	if err == nil {
		t.Error("Connect(empty) should return error")
	}
}

func TestAdapter_Connect_RejectsNilSymbols(t *testing.T) {
	a, _ := binance.NewAdapter(binance.Config{})
	err := a.Connect(context.Background(), nil)
	if err == nil {
		t.Error("Connect(nil) should return error")
	}
}

func TestAdapter_Stream_EmitsNormalizedSnapshot(t *testing.T) {
	const symbol = "BTCUSDT"
	var gotPath, gotSymbol string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSymbol = r.URL.Query().Get("symbol")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(depthResponse))
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
		if snap.Exchange != "binance" {
			t.Errorf("Exchange = %q, want %q", snap.Exchange, "binance")
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

	if gotPath != "/api/v3/depth" {
		t.Errorf("request path = %q, want /api/v3/depth", gotPath)
	}
	if gotSymbol != symbol {
		t.Errorf("symbol query param = %q, want %q", gotSymbol, symbol)
	}
}

func TestAdapter_Stream_Non2xxReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.Connect(ctx, []string{"BTCUSDT"}); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	_, errCh := a.Stream(ctx)

	select {
	case err, ok := <-errCh:
		if !ok {
			t.Fatal("error channel closed without error")
		}
		if err == nil {
			t.Error("expected non-nil error for 500 response")
		}
	case <-ctx.Done():
		t.Fatal("timeout waiting for error")
	}
}

func TestAdapter_Stream_MalformedJSONReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`not valid json{`))
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := a.Connect(ctx, []string{"BTCUSDT"}); err != nil {
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
		_, _ = w.Write([]byte(`{"lastUpdateId":1,"bids":[],"asks":[]}`))
	}))
	defer server.Close()

	a := newTestAdapter(t, server)
	ctx := context.Background()

	if err := a.Connect(ctx, []string{"BTCUSDT"}); err != nil {
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
