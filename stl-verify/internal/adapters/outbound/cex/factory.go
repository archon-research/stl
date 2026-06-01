package cex

import (
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex/binance"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/cex/coinbase"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type Config struct {
	Logger            *slog.Logger
	HTTPClient        *http.Client
	PollInterval      time.Duration
	ChannelBufferSize int
	DepthLimit        int
}

func NewExchangeOrderBookStreamer(exchange string, cfg Config) (outbound.ExchangeOrderBookStreamer, error) {
	switch strings.ToLower(strings.TrimSpace(exchange)) {
	case "binance":
		return binance.NewAdapter(binance.Config{
			Logger:            cfg.Logger,
			HTTPClient:        cfg.HTTPClient,
			PollInterval:      cfg.PollInterval,
			ChannelBufferSize: cfg.ChannelBufferSize,
			DepthLimit:        cfg.DepthLimit,
		})
	case "coinbase":
		return coinbase.NewAdapter(coinbase.Config{
			Logger:            cfg.Logger,
			HTTPClient:        cfg.HTTPClient,
			PollInterval:      cfg.PollInterval,
			ChannelBufferSize: cfg.ChannelBufferSize,
		})
	default:
		return nil, fmt.Errorf("unsupported exchange %q, supported: %v", exchange, SupportedExchanges())
	}
}

func SupportedExchanges() []string {
	exchanges := []string{"binance", "coinbase"}
	slices.Sort(exchanges)
	return exchanges
}
