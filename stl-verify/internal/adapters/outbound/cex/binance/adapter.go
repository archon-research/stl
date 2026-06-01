package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Adapter implements outbound.ExchangeOrderBookStreamer.
var _ outbound.ExchangeOrderBookStreamer = (*Adapter)(nil)

type Config struct {
	BaseURL           string
	Logger            *slog.Logger
	HTTPClient        *http.Client
	PollInterval      time.Duration
	ChannelBufferSize int
	DepthLimit        int
}

type Adapter struct {
	cfg     Config
	symbols []string
	once    sync.Once
	closeCh chan struct{}
}

func NewAdapter(cfg Config) (*Adapter, error) {
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
	return "binance"
}

func (a *Adapter) Connect(_ context.Context, symbols []string) error {
	if len(symbols) == 0 {
		return errors.New("symbols must not be empty")
	}
	cp := make([]string, len(symbols))
	copy(cp, symbols)
	a.symbols = cp
	return nil
}

func (a *Adapter) Stream(ctx context.Context) (<-chan entity.OrderBookSnapshot, <-chan error) {
	snapCh := make(chan entity.OrderBookSnapshot, a.cfg.ChannelBufferSize)
	errCh := make(chan error, 1)

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
				for _, sym := range a.symbols {
					snap, err := a.fetchSnapshot(ctx, sym)
					if err != nil {
						select {
						case errCh <- fmt.Errorf("fetching %s: %w", sym, err):
						default:
						}
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
	url := fmt.Sprintf("%s/api/v3/depth?symbol=%s&limit=%d", a.cfg.BaseURL, symbol, a.cfg.DepthLimit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("creating request: %w", err)
	}

	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("doing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return entity.OrderBookSnapshot{}, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var raw binanceDepthResponse
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return entity.OrderBookSnapshot{}, fmt.Errorf("decoding response: %w", err)
	}

	bids := make([]entity.OrderBookLevel, 0, len(raw.Bids))
	for _, b := range raw.Bids {
		if len(b) < 2 {
			continue
		}
		bids = append(bids, entity.OrderBookLevel{Price: b[0], Qty: b[1]})
	}

	asks := make([]entity.OrderBookLevel, 0, len(raw.Asks))
	for _, ask := range raw.Asks {
		if len(ask) < 2 {
			continue
		}
		asks = append(asks, entity.OrderBookLevel{Price: ask[0], Qty: ask[1]})
	}

	return entity.OrderBookSnapshot{
		Exchange:   "binance",
		Token:      symbol,
		CapturedAt: time.Now().UTC(),
		Bids:       bids,
		Asks:       asks,
	}, nil
}
