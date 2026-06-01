package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
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
	name         = "binance"
	maxBodyBytes = 4 << 20 // 4 MiB
)

type Config struct {
	BaseURL           string
	Logger            *slog.Logger
	HTTPClient        *http.Client
	PollInterval      time.Duration
	ChannelBufferSize int
	DepthLimit        int
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
		return nil, errors.New("PollInterval must not be negative")
	}
	if cfg.ChannelBufferSize < 0 {
		return nil, errors.New("ChannelBufferSize must not be negative")
	}
	if cfg.DepthLimit < 0 {
		return nil, errors.New("DepthLimit must not be negative")
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = "https://api.binance.com"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
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
	if cfg.DepthLimit == 0 {
		cfg.DepthLimit = 100
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

type binanceDepthResponse struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

func (a *Adapter) fetchSnapshot(ctx context.Context, symbol string) (entity.OrderBookSnapshot, error) {
	params := neturl.Values{
		"symbol": {symbol},
		"limit":  {fmt.Sprintf("%d", a.cfg.DepthLimit)},
	}
	reqURL := a.cfg.BaseURL + "/api/v3/depth?" + params.Encode()

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

	var raw binanceDepthResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBodyBytes)).Decode(&raw); err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("decoding response: %w", err)
	}

	bids := make([]entity.OrderBookLevel, 0, len(raw.Bids))
	for i, b := range raw.Bids {
		if len(b) < 2 {
			a.cfg.Logger.Warn("skipping malformed bid level", "symbol", symbol, "index", i, "length", len(b))
			continue
		}
		bids = append(bids, entity.OrderBookLevel{Price: b[0], Qty: b[1]})
	}

	asks := make([]entity.OrderBookLevel, 0, len(raw.Asks))
	for i, ask := range raw.Asks {
		if len(ask) < 2 {
			a.cfg.Logger.Warn("skipping malformed ask level", "symbol", symbol, "index", i, "length", len(ask))
			continue
		}
		asks = append(asks, entity.OrderBookLevel{Price: ask[0], Qty: ask[1]})
	}

	return entity.OrderBookSnapshot{
		Exchange:   name,
		Token:      symbol,
		CapturedAt: time.Now().UTC(),
		Bids:       bids,
		Asks:       asks,
	}, nil
}
