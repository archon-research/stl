//go:build integration

package main

import (
	"context"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/orderbook"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// fakeProvider feeds run() one snapshot then keeps the channel open until ctx is
// cancelled, mimicking a real provider's lifecycle without dialing an exchange.
type fakeProvider struct {
	name string
}

func (p *fakeProvider) Name() string { return p.name }

func (p *fakeProvider) Watch(ctx context.Context, symbols []string) (<-chan entity.OrderbookUpdate, error) {
	ch := make(chan entity.OrderbookUpdate, len(symbols))
	for _, sym := range symbols {
		ob := entity.NewOrderbook(p.name, sym)
		ob.ApplyLevel(entity.Bid, "100", "1")
		ob.ApplyLevel(entity.Ask, "101", "2")
		ch <- entity.NewOrderbookUpdate(ob, true, time.Time{}, time.Now())
	}
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

// TestRunHappyPath wires run() end-to-end against a real TimescaleDB container and
// an injected fake provider, then verifies a snapshot row is persisted before the
// context is cancelled and run() returns cleanly.
func TestRunHappyPath(t *testing.T) {
	pool, dsn, cleanup := testutil.SetupTimescaleDB(t)
	defer cleanup()

	t.Setenv("EXCHANGE", "coinbase")
	t.Setenv("SYMBOLS", "BTC-USD,ETH-USD")
	t.Setenv("DATABASE_URL", dsn)
	t.Setenv("ORDERBOOK_DEPTH", "100")
	t.Setenv("ORDERBOOK_INTERVAL", "50ms")
	t.Setenv("ENVIRONMENT", "development")

	factory := func(exchange string, _ orderbook.Config) (outbound.OrderbookProvider, error) {
		return &fakeProvider{name: exchange}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- run(ctx, factory) }()

	// Wait until at least one tick has persisted a row, then stop.
	deadline := time.After(10 * time.Second)
	for {
		var count int
		if err := pool.QueryRow(context.Background(),
			`SELECT COUNT(*) FROM cex_orderbook_snapshots WHERE exchange = 'coinbase'`).Scan(&count); err != nil {
			t.Fatalf("count: %v", err)
		}
		if count >= 2 { // one row per symbol
			break
		}
		select {
		case <-deadline:
			t.Fatal("no snapshots persisted before deadline")
		case <-time.After(25 * time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after context cancel")
	}
}

// TestNewProviderFactory checks the exchange->constructor mapping and the
// unknown-exchange error. The real constructors only validate config (they do not
// dial until Watch), so calling them here is safe.
func TestNewProviderFactory(t *testing.T) {
	tests := []struct {
		exchange string
		want     string
		wantErr  bool
	}{
		{exchange: "coinbase", want: "coinbase"},
		{exchange: "okx", want: "okx"},
		{exchange: "kraken", want: "kraken"},
		{exchange: "bitmex", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.exchange, func(t *testing.T) {
			p, err := newProvider(tc.exchange, orderbook.DefaultConfig())
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if p.Name() != tc.want {
				t.Errorf("Name() = %s, want %s", p.Name(), tc.want)
			}
		})
	}
}

func TestParseConfig(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
		check   func(t *testing.T, c cliConfig)
	}{
		{
			name: "defaults applied",
			env:  map[string]string{"EXCHANGE": "Coinbase", "SYMBOLS": "BTC-USD, ETH-USD ,", "DATABASE_URL": "postgres://x"},
			check: func(t *testing.T, c cliConfig) {
				if c.exchange != "coinbase" {
					t.Errorf("exchange = %q, want lowercased coinbase", c.exchange)
				}
				if len(c.symbols) != 2 || c.symbols[0] != "BTC-USD" || c.symbols[1] != "ETH-USD" {
					t.Errorf("symbols = %v, want [BTC-USD ETH-USD]", c.symbols)
				}
				if c.depth != 100 {
					t.Errorf("depth = %d, want default 100", c.depth)
				}
				if c.interval != 5*time.Second {
					t.Errorf("interval = %s, want default 5s", c.interval)
				}
			},
		},
		{name: "missing exchange", env: map[string]string{"SYMBOLS": "BTC-USD", "DATABASE_URL": "x"}, wantErr: true},
		{name: "missing symbols", env: map[string]string{"EXCHANGE": "okx", "DATABASE_URL": "x"}, wantErr: true},
		{name: "missing db", env: map[string]string{"EXCHANGE": "okx", "SYMBOLS": "BTC-USD"}, wantErr: true},
		{name: "bad depth", env: map[string]string{"EXCHANGE": "okx", "SYMBOLS": "BTC-USD", "DATABASE_URL": "x", "ORDERBOOK_DEPTH": "0"}, wantErr: true},
		{name: "bad interval", env: map[string]string{"EXCHANGE": "okx", "SYMBOLS": "BTC-USD", "DATABASE_URL": "x", "ORDERBOOK_INTERVAL": "nope"}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, k := range []string{"EXCHANGE", "SYMBOLS", "DATABASE_URL", "ORDERBOOK_DEPTH", "ORDERBOOK_INTERVAL"} {
				t.Setenv(k, "")
			}
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			c, err := parseConfig()
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && tc.check != nil {
				tc.check(t, c)
			}
		})
	}
}
