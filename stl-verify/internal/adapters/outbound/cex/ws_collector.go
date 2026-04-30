package cex

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.OrderbookSubscriber = (*WSCollector)(nil)

type WSCollector struct {
	connections []*WSConnection
	out         chan entity.OrderbookSnapshot
	logger      *slog.Logger
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func NewWSCollector(configs []WSConnectionConfig, logger *slog.Logger) *WSCollector {
	if logger == nil {
		logger = slog.Default()
	}
	conns := make([]*WSConnection, len(configs))
	for i, cfg := range configs {
		conns[i] = NewWSConnection(cfg, logger)
	}
	return &WSCollector{
		connections: conns,
		out:         make(chan entity.OrderbookSnapshot, 500),
		logger:      logger.With("component", "ws-collector"),
	}
}

func (c *WSCollector) Subscribe(ctx context.Context) (<-chan entity.OrderbookSnapshot, error) {
	ctx, c.cancel = context.WithCancel(ctx)

	for _, conn := range c.connections {
		ch, err := conn.Connect(ctx)
		if err != nil {
			c.logger.Warn("failed to start connection",
				"exchange", conn.config.Parser.Exchange(), "error", err)
			continue
		}
		c.wg.Add(1)
		go func(exchange string, ch <-chan entity.OrderbookSnapshot) {
			defer c.wg.Done()
			for snap := range ch {
				select {
				case c.out <- snap:
				case <-ctx.Done():
					return
				}
			}
		}(conn.config.Parser.Exchange(), ch)
	}
	return c.out, nil
}

func (c *WSCollector) Unsubscribe() error {
	if c.cancel != nil {
		c.cancel()
	}
	var firstErr error
	for _, conn := range c.connections {
		if err := conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.wg.Wait()
	close(c.out)
	return firstErr
}

func (c *WSCollector) HealthCheck(ctx context.Context) error {
	var unhealthy []string
	for _, conn := range c.connections {
		if err := conn.HealthCheck(); err != nil {
			unhealthy = append(unhealthy, conn.config.Parser.Exchange())
		}
	}
	if len(unhealthy) > 0 {
		return fmt.Errorf("unhealthy exchanges: %v", unhealthy)
	}
	return nil
}

// BuildWSCollectorFromConfig creates a WSCollector using the global Exchanges config.
func BuildWSCollectorFromConfig(logger *slog.Logger) *WSCollector {
	var configs []WSConnectionConfig
	for _, exchange := range Exchanges {
		pairs := make([]string, 0, len(exchange.Symbols))
		for _, pair := range exchange.Symbols {
			pairs = append(pairs, pair)
		}
		configs = append(configs, WSConnectionConfig{
			URL:              exchange.WebSocketURL,
			Parser:           parserForExchange(exchange.Name),
			Pairs:            pairs,
			Depth:            exchange.MaxDepth,
			PingInterval:     exchange.PingInterval,
			PongTimeout:      exchange.PongTimeout,
			ReconnectBackoff: exchange.ReconnectBackoff,
			MaxBackoff:       exchange.MaxReconnectBackoff,
			ChannelBuffer:    200,
		})
	}
	return NewWSCollector(configs, logger)
}

func parserForExchange(name string) Parser {
	switch name {
	case "binance":
		return NewBinanceParser()
	case "bybit":
		return NewBybitParser()
	case "okx":
		return NewOKXParser()
	case "kraken":
		return NewKrakenParser()
	case "coinbase":
		return NewCoinbaseParser()
	default:
		return nil
	}
}
