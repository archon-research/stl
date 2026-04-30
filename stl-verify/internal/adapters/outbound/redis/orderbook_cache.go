package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

const orderbookKeyPrefix = "cex:orderbook"

// OrderbookCache is a Redis-backed cache for the latest orderbook snapshot
// per (exchange, symbol). Key pattern: cex:orderbook:{exchange}:{symbol}.
type OrderbookCache struct {
	client *goredis.Client
	ttl    time.Duration
	logger *slog.Logger
}

// NewOrderbookCache creates a new Redis orderbook cache using the existing Config type.
func NewOrderbookCache(cfg Config, logger *slog.Logger) *OrderbookCache {
	if logger == nil {
		logger = slog.Default()
	}
	client := goredis.NewClient(&goredis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	})
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = 5 * time.Minute
	}
	return &OrderbookCache{
		client: client,
		ttl:    ttl,
		logger: logger.With("component", "orderbook-cache"),
	}
}

func (c *OrderbookCache) key(exchange, symbol string) string {
	return fmt.Sprintf("%s:%s:%s", orderbookKeyPrefix, exchange, symbol)
}

// SetLatest stores the most recent orderbook snapshot for the given exchange and symbol.
func (c *OrderbookCache) SetLatest(ctx context.Context, snap *entity.OrderbookSnapshot) error {
	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return c.client.Set(ctx, c.key(snap.Exchange, snap.Symbol), data, c.ttl).Err()
}

// GetLatest retrieves the most recent orderbook snapshot for the given exchange and symbol.
func (c *OrderbookCache) GetLatest(ctx context.Context, exchange, symbol string) (*entity.OrderbookSnapshot, error) {
	data, err := c.client.Get(ctx, c.key(exchange, symbol)).Bytes()
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	var snap entity.OrderbookSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &snap, nil
}

// GetAllForSymbol retrieves the latest orderbook snapshots for a symbol across all exchanges.
func (c *OrderbookCache) GetAllForSymbol(ctx context.Context, symbol string) ([]*entity.OrderbookSnapshot, error) {
	pattern := fmt.Sprintf("%s:*:%s", orderbookKeyPrefix, symbol)
	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("keys: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	pipe := c.client.Pipeline()
	cmds := make([]*goredis.StringCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
		return nil, fmt.Errorf("pipeline: %w", err)
	}
	var result []*entity.OrderbookSnapshot
	for _, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			continue
		}
		var snap entity.OrderbookSnapshot
		if err := json.Unmarshal(data, &snap); err != nil {
			continue
		}
		result = append(result, &snap)
	}
	return result, nil
}

// Close closes the underlying Redis client.
func (c *OrderbookCache) Close() error {
	return c.client.Close()
}
