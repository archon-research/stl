package coinbase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Adapter implements outbound.ExchangeOrderBookStreamer.
var _ outbound.ExchangeOrderBookStreamer = (*Adapter)(nil)

const (
	name         = "coinbase"
	maxBodyBytes = 4 << 20 // 4 MiB
)

type Config struct {
	BaseURL           string
	HTTPClient        *http.Client
	PollInterval      time.Duration
	ChannelBufferSize int
}

type Adapter struct {
	cfg       Config
	symbols   []string
	once      sync.Once
	streaming atomic.Bool
	closeCh   chan struct{}
}

func NewAdapter(cfg Config) (*Adapter, error) {
	if cfg.PollInterval < 0 {
		return nil, errors.New("poll interval must not be negative")
	}
	if cfg.ChannelBufferSize < 0 {
		return nil, errors.New("channel buffer size must not be negative")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.exchange.coinbase.com"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 1 * time.Second
	}
	if cfg.ChannelBufferSize == 0 {
		cfg.ChannelBufferSize = 100
	}
	return &Adapter{
		cfg:     cfg,
		closeCh: make(chan struct{}),
	}, nil
}

func (a *Adapter) Name() string {
	return name
}

func (a *Adapter) Connect(_ context.Context, symbols []string) error {
	if len(symbols) == 0 {
		return errors.New("symbols must not be empty")
	}
	for _, s := range symbols {
		if strings.TrimSpace(s) == "" {
			return errors.New("symbol must not be blank")
		}
		if strings.TrimSpace(s) != s {
			return fmt.Errorf("symbol %q must not have surrounding whitespace", s)
		}
	}
	cp := make([]string, len(symbols))
	copy(cp, symbols)
	a.symbols = cp
	return nil
}

func (a *Adapter) Stream(ctx context.Context) (<-chan entity.OrderBookSnapshot, <-chan error) {
	snapCh := make(chan entity.OrderBookSnapshot, a.cfg.ChannelBufferSize)
	errCh := make(chan error, 1)

	if len(a.symbols) == 0 {
		errCh <- errors.New("Connect must be called before Stream")
		close(snapCh)
		close(errCh)
		return snapCh, errCh
	}

	if !a.streaming.CompareAndSwap(false, true) {
		errCh <- errors.New("Stream already called; create a new Adapter to start a new stream")
		close(snapCh)
		close(errCh)
		return snapCh, errCh
	}

	symbols := make([]string, len(a.symbols))
	copy(symbols, a.symbols)

	go func() {
		defer close(snapCh)
		defer close(errCh)

		ticker := time.NewTicker(a.cfg.PollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-a.closeCh:
				return
			case <-ticker.C:
				for _, sym := range symbols {
					snap, err := a.fetchSnapshot(ctx, sym)
					if err != nil {
						errCh <- fmt.Errorf("fetching %s: %w", sym, err)
						return
					}
					select {
					case snapCh <- snap:
					case <-ctx.Done():
						return
					case <-a.closeCh:
						return
					}
				}
			}
		}
	}()

	return snapCh, errCh
}

func (a *Adapter) Close() error {
	a.once.Do(func() { close(a.closeCh) })
	return nil
}

// coinbaseBookResponse holds the raw Coinbase product book response.
// Levels are [price, size, num_orders] where price and size are strings.
// The level=2 endpoint always returns the full aggregated book (up to 50 levels per side).
type coinbaseBookResponse struct {
	Bids     [][]json.RawMessage `json:"bids"`
	Asks     [][]json.RawMessage `json:"asks"`
	Sequence int64               `json:"sequence"`
}

func (a *Adapter) fetchSnapshot(ctx context.Context, symbol string) (entity.OrderBookSnapshot, error) {
	reqURL := a.cfg.BaseURL + "/products/" + neturl.PathEscape(symbol) + "/book?level=2"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("creating request: %w", err)
	}

	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("doing request: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return entity.OrderBookSnapshot{}, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var raw coinbaseBookResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&raw); err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("decoding response: %w", err)
	}

	bids, err := parseLevels(raw.Bids)
	if err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("parsing bids: %w", err)
	}

	asks, err := parseLevels(raw.Asks)
	if err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("parsing asks: %w", err)
	}

	snap := entity.OrderBookSnapshot{
		Exchange:   name,
		Token:      symbol,
		CapturedAt: time.Now().UTC(),
		Bids:       bids,
		Asks:       asks,
	}
	if err := snap.Validate(); err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("invalid snapshot: %w", err)
	}
	return snap, nil
}

// parseLevels extracts price and size from Coinbase's [price, size, num_orders] tuples.
func parseLevels(raw [][]json.RawMessage) ([]entity.OrderBookLevel, error) {
	levels := make([]entity.OrderBookLevel, 0, len(raw))
	for i, row := range raw {
		if len(row) < 2 {
			return nil, fmt.Errorf("malformed level at index %d: got %d fields", i, len(row))
		}
		var price, qty string
		if err := json.Unmarshal(row[0], &price); err != nil {
			return nil, fmt.Errorf("index %d price: %w", i, err)
		}
		if err := json.Unmarshal(row[1], &qty); err != nil {
			return nil, fmt.Errorf("index %d qty: %w", i, err)
		}
		levels = append(levels, entity.OrderBookLevel{Price: price, Qty: qty})
	}
	return levels, nil
}
