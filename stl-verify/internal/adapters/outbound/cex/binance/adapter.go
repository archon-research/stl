package binance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex/poller"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Adapter implements outbound.ExchangeOrderBookStreamer.
var _ outbound.ExchangeOrderBookStreamer = (*Adapter)(nil)

const (
	name = "binance"
	// maxBodyBytes caps the response body we decode to guard against a
	// malformed or hostile endpoint exhausting memory. Binance depth at the
	// maximum limit=5000 is a few hundred KB; 4 MiB is generous headroom.
	maxBodyBytes = 4 << 20 // 4 MiB
)

// This adapter is REST polling only. A live WebSocket streamer would be a
// distinct adapter shape: Binance diff-depth requires buffering depthUpdate
// events, fetching a REST snapshot, dropping events with u <= lastUpdateId,
// then applying the remainder with gap detection and resync handling.

type Config struct {
	BaseURL           string
	HTTPClient        *http.Client
	PollInterval      time.Duration
	ChannelBufferSize int
	DepthLimit        int
}

// withDefaults validates the config and fills zero-valued fields with defaults.
func (c Config) withDefaults() (Config, error) {
	if c.PollInterval < 0 {
		return Config{}, errors.New("poll interval must not be negative")
	}
	if c.ChannelBufferSize < 0 {
		return Config{}, errors.New("channel buffer size must not be negative")
	}
	if c.DepthLimit < 0 {
		return Config{}, errors.New("depth limit must not be negative")
	}
	if c.BaseURL == "" {
		c.BaseURL = "https://api.binance.com"
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: 10 * time.Second}
	}
	if c.PollInterval == 0 {
		c.PollInterval = 1 * time.Second
	}
	if c.ChannelBufferSize == 0 {
		c.ChannelBufferSize = 100
	}
	if c.DepthLimit == 0 {
		c.DepthLimit = 100
	}
	return c, nil
}

// Adapter streams Binance spot order books over REST. Lifecycle (Connect,
// Stream, Close) is provided by the embedded poller.Poller; this type supplies
// only the Binance-specific snapshot fetch.
type Adapter struct {
	*poller.Poller
	cfg Config
}

func NewAdapter(cfg Config) (*Adapter, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	a := &Adapter{cfg: cfg}
	a.Poller = poller.New(name, a.fetchSnapshot, cfg.PollInterval, cfg.ChannelBufferSize)
	return a, nil
}

type binanceDepthResponse struct {
	LastUpdateID int64      `json:"lastUpdateId"`
	Bids         [][]string `json:"bids"`
	Asks         [][]string `json:"asks"`
}

// fetchSnapshot pulls a single REST depth snapshot for one symbol.
// Binance GET /api/v3/depth returns {lastUpdateId, bids[[price,qty]], asks[[price,qty]]}
// with price and qty as decimal strings, ordered best-first.
// Docs: https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#order-book
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
		// Drain so the keep-alive connection can be reused on the next poll;
		// Close alone does not read the unread remainder, and the transport
		// only pools a connection whose body was consumed to EOF.
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
			return entity.OrderBookSnapshot{}, fmt.Errorf("malformed bid at index %d: got %d fields", i, len(b))
		}
		bids = append(bids, entity.OrderBookLevel{Price: b[0], Qty: b[1]})
	}

	asks := make([]entity.OrderBookLevel, 0, len(raw.Asks))
	for i, ask := range raw.Asks {
		if len(ask) < 2 {
			return entity.OrderBookSnapshot{}, fmt.Errorf("malformed ask at index %d: got %d fields", i, len(ask))
		}
		asks = append(asks, entity.OrderBookLevel{Price: ask[0], Qty: ask[1]})
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
