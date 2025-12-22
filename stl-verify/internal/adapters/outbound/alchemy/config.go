package alchemy

import (
	"errors"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Default configuration values for reconnection.
const (
	defaultInitialBackoff       = 1 * time.Second
	defaultMaxBackoff           = 60 * time.Second
	defaultBackoffFactor        = 2.0
	defaultPingInterval         = 30 * time.Second
	defaultPongTimeout          = 10 * time.Second
	defaultReadTimeout          = 60 * time.Second
	defaultChannelBufferSize    = 100
	defaultBlockRetention       = 1000 // Keep last 1000 blocks in state
	defaultHealthTimeout        = 30 * time.Second
	defaultFinalityBlockCount   = 64  // Blocks before considered finalized (Ethereum mainnet)
	defaultMaxUnfinalizedBlocks = 128 // Max unfinalized blocks to keep in memory
)

// Config holds the configuration for the Alchemy WebSocket subscriber.
type Config struct {
	// WebSocketURL is the Alchemy WebSocket endpoint URL.
	// Example: wss://eth-mainnet.g.alchemy.com/v2/<api-key>
	WebSocketURL string

	// HTTPURL is the Alchemy HTTP endpoint URL for backfilling blocks.
	// Example: https://eth-mainnet.g.alchemy.com/v2/<api-key>
	// If not set, backfilling will be disabled.
	HTTPURL string

	// ChainID is the blockchain chain ID (e.g., 1 for Ethereum mainnet).
	// Required for cache and event publishing.
	ChainID int64

	// BlockCache is the cache for storing block data.
	// If set, the subscriber will fetch and cache all data types.
	BlockCache outbound.BlockCache

	// EventSink is the sink for publishing events.
	// If set, events are published after data is cached.
	EventSink outbound.EventSink

	// InitialBackoff is the initial delay before reconnecting after a disconnect.
	// Defaults to 1 second if not set.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between reconnection attempts.
	// Defaults to 60 seconds if not set.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each failed attempt.
	// Defaults to 2.0 if not set.
	BackoffFactor float64

	// PingInterval is how often to send ping messages to keep the connection alive.
	// Defaults to 30 seconds if not set.
	PingInterval time.Duration

	// PongTimeout is how long to wait for a pong response before considering the connection dead.
	// Defaults to 10 seconds if not set.
	PongTimeout time.Duration

	// ReadTimeout is the maximum time to wait for a message before considering the connection dead.
	// Defaults to 60 seconds if not set.
	ReadTimeout time.Duration

	// ChannelBufferSize is the size of the block header channel buffer.
	// Defaults to 100 if not set. Helps with backpressure.
	ChannelBufferSize int

	// BlockRetention is the number of recent blocks to keep in the state repository.
	// Defaults to 1000 if not set.
	BlockRetention int

	// HealthTimeout is how long without receiving a block before considering unhealthy.
	// Defaults to 30 seconds if not set.
	HealthTimeout time.Duration

	// BlockStateRepo is the repository for tracking block state.
	// Required - the subscriber will not start without it.
	BlockStateRepo outbound.BlockStateRepository

	// FinalityBlockCount is how many blocks behind the tip before a block is considered finalized.
	// Reorgs deeper than this will cause an error. Defaults to 64 (Ethereum mainnet).
	FinalityBlockCount int

	// MaxUnfinalizedBlocks is the maximum number of unfinalized blocks to keep in memory.
	// Defaults to 128.
	MaxUnfinalizedBlocks int

	// Logger is the structured logger for the subscriber.
	// If not set, a default logger will be used.
	Logger *slog.Logger
}

// Validate checks that all required configuration fields are set.
func (c *Config) Validate() error {
	if c.WebSocketURL == "" {
		return errors.New("WebSocketURL is required")
	}
	if c.HTTPURL == "" {
		return errors.New("HTTPURL is required")
	}
	if c.BlockStateRepo == nil {
		return errors.New("BlockStateRepo is required")
	}
	return nil
}

// applyDefaults sets default values for unset configuration fields.
func (c *Config) applyDefaults() {
	if c.InitialBackoff == 0 {
		c.InitialBackoff = defaultInitialBackoff
	}
	if c.MaxBackoff == 0 {
		c.MaxBackoff = defaultMaxBackoff
	}
	if c.BackoffFactor == 0 {
		c.BackoffFactor = defaultBackoffFactor
	}
	if c.PingInterval == 0 {
		c.PingInterval = defaultPingInterval
	}
	if c.PongTimeout == 0 {
		c.PongTimeout = defaultPongTimeout
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = defaultReadTimeout
	}
	if c.ChannelBufferSize == 0 {
		c.ChannelBufferSize = defaultChannelBufferSize
	}
	if c.BlockRetention == 0 {
		c.BlockRetention = defaultBlockRetention
	}
	if c.HealthTimeout == 0 {
		c.HealthTimeout = defaultHealthTimeout
	}
	if c.FinalityBlockCount == 0 {
		c.FinalityBlockCount = defaultFinalityBlockCount
	}
	if c.MaxUnfinalizedBlocks == 0 {
		c.MaxUnfinalizedBlocks = defaultMaxUnfinalizedBlocks
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}
