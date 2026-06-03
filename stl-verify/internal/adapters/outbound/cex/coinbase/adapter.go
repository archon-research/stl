package coinbase

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
	name = "coinbase"
	// maxBodyBytes caps the response body we decode to guard against a
	// malformed or hostile endpoint exhausting memory. A level=2 book is at
	// most ~50 levels per side (tens of KB); 4 MiB is generous headroom.
	maxBodyBytes = 4 << 20 // 4 MiB
)

// This adapter is REST polling only. A live WebSocket streamer would be a
// distinct adapter shape: the Coinbase level2 channel delivers an initial
// snapshot followed by l2update diffs that must be applied in order.

type Config struct {
	BaseURL           string
	HTTPClient        *http.Client
	PollInterval      time.Duration
	ChannelBufferSize int
}

// withDefaults validates the config and fills zero-valued fields with defaults.
func (c Config) withDefaults() (Config, error) {
	if c.PollInterval < 0 {
		return Config{}, errors.New("poll interval must not be negative")
	}
	if c.ChannelBufferSize < 0 {
		return Config{}, errors.New("channel buffer size must not be negative")
	}
	if c.BaseURL == "" {
		c.BaseURL = "https://api.exchange.coinbase.com"
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
	return c, nil
}

// Adapter streams Coinbase Exchange order books over REST. Lifecycle (Connect,
// Stream, Close) is provided by the embedded poller.Poller; this type supplies
// only the Coinbase-specific snapshot fetch.
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

// coinbaseBookResponse holds the raw Coinbase product book response.
// Levels are [price, size, num_orders] where price and size are strings.
// The level=2 endpoint always returns the full aggregated book (up to 50 levels per side).
type coinbaseBookResponse struct {
	Bids     [][]json.RawMessage `json:"bids"`
	Asks     [][]json.RawMessage `json:"asks"`
	Sequence int64               `json:"sequence"`
}

// fetchSnapshot pulls a single REST level=2 (aggregated) book for one symbol.
// Coinbase GET /products/{product_id}/book?level=2 returns bids/asks as
// [price, size, num_orders] tuples (price and size as decimal strings) ordered
// best-first, plus a sequence number.
// Docs: https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductbook
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
		// Drain so the keep-alive connection can be reused on the next poll;
		// Close alone does not read the unread remainder, and the transport
		// only pools a connection whose body was consumed to EOF.
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
