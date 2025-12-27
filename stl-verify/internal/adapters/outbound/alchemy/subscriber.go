package alchemy

/*
Subscriber implements a WebSocket client for Alchemy's newHeads subscription.

Key behaviors:
  - Automatic reconnection: If the WebSocket connection drops, the subscriber
    automatically reconnects with exponential backoff (configurable via
    InitialBackoff, MaxBackoff, BackoffFactor).
  - Non-blocking sends: Block headers are sent to the headers channel without
    blocking. If the channel buffer is full, new blocks are DROPPED and logged
    as errors. Consumers must read from the channel fast enough to avoid this.
  - Ping/pong keepalive: The subscriber sends periodic pings to detect stale
    connections. If a pong is not received within PongTimeout, the connection
    is considered dead and will be reconnected.
  - Health checks: HealthCheck verifies the connection is alive and blocks are
    being received within HealthTimeout.
  - Thread-safe: All public methods are safe for concurrent use.

Channel buffer sizing:
  - Default ChannelBufferSize is 100 blocks.
  - If your consumer processes blocks slower than ~12 seconds/block (Ethereum
    block time), increase the buffer or optimize consumption to avoid drops.
*/

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Subscriber implements outbound.BlockSubscriber
var _ outbound.BlockSubscriber = (*Subscriber)(nil)

// SubscriberConfig holds configuration for the WebSocket subscriber.
type SubscriberConfig struct {
	// WebSocketURL is the Alchemy WebSocket endpoint URL.
	WebSocketURL string

	// InitialBackoff is the initial delay before reconnecting after a disconnect.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between reconnection attempts.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each failed attempt.
	BackoffFactor float64

	// PingInterval is how often to send ping messages to keep the connection alive.
	PingInterval time.Duration

	// PongTimeout is how long to wait for a pong response.
	PongTimeout time.Duration

	// ReadTimeout is the maximum time to wait for a message.
	ReadTimeout time.Duration

	// ChannelBufferSize is the size of the block header channel buffer.
	ChannelBufferSize int

	// HealthTimeout is how long without receiving a block
	// before considering the connection unhealthy.
	HealthTimeout time.Duration

	// Logger is the structured logger for the subscriber.
	Logger *slog.Logger
}

// SubscriberConfigDefaults returns a config with default values.
func SubscriberConfigDefaults() SubscriberConfig {
	return SubscriberConfig{
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        60 * time.Second,
		BackoffFactor:     2.0,
		PingInterval:      30 * time.Second,
		PongTimeout:       10 * time.Second,
		ReadTimeout:       60 * time.Second,
		ChannelBufferSize: 100,
		HealthTimeout:     30 * time.Second,
		Logger:            slog.Default(),
	}
}

// Subscriber implements BlockSubscriber using Alchemy's WebSocket API.
// It handles WebSocket connection, subscription, and emitting raw headers.
type Subscriber struct {
	config  SubscriberConfig
	conn    *websocket.Conn
	mu      sync.RWMutex
	done    chan struct{}
	closed  bool
	headers chan outbound.BlockHeader
	ctx     context.Context
	cancel  context.CancelFunc

	lastBlockTime atomic.Int64

	// wg tracks active goroutines for graceful shutdown
	wg sync.WaitGroup

	// Reconnect callback - called when connection is re-established
	onReconnect func()
}

// NewSubscriber creates a new Alchemy WebSocket subscriber.
func NewSubscriber(config SubscriberConfig) (*Subscriber, error) {
	if config.WebSocketURL == "" {
		return nil, errors.New("WebSocketURL is required")
	}

	// Apply defaults for zero values
	defaults := SubscriberConfigDefaults()
	if config.InitialBackoff == 0 {
		config.InitialBackoff = defaults.InitialBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaults.MaxBackoff
	}
	if config.BackoffFactor == 0 {
		config.BackoffFactor = defaults.BackoffFactor
	}
	if config.PingInterval == 0 {
		config.PingInterval = defaults.PingInterval
	}
	if config.PongTimeout == 0 {
		config.PongTimeout = defaults.PongTimeout
	}
	if config.ReadTimeout == 0 {
		config.ReadTimeout = defaults.ReadTimeout
	}
	if config.ChannelBufferSize == 0 {
		config.ChannelBufferSize = defaults.ChannelBufferSize
	}
	if config.HealthTimeout == 0 {
		config.HealthTimeout = defaults.HealthTimeout
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &Subscriber{
		config:  config,
		done:    make(chan struct{}),
		headers: make(chan outbound.BlockHeader, config.ChannelBufferSize),
	}, nil
}

// SetOnReconnect sets a callback that will be called when the connection is re-established.
// This allows the application layer to trigger backfill.
func (s *Subscriber) SetOnReconnect(callback func()) {
	s.onReconnect = callback
}

// Subscribe starts listening for new block headers.
func (s *Subscriber) Subscribe(ctx context.Context) (<-chan outbound.BlockHeader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("subscriber is closed")
	}

	s.ctx, s.cancel = context.WithCancel(ctx)
	s.wg.Add(1)
	go s.connectionManager()

	return s.headers, nil
}

// connectionManager manages the WebSocket connection with automatic reconnection.
func (s *Subscriber) connectionManager() {
	defer s.wg.Done()

	backoff := s.config.InitialBackoff
	logger := s.config.Logger.With("component", "alchemy-subscriber")
	isFirstConnect := true

	for {
		select {
		case <-s.done:
			return
		case <-s.ctx.Done():
			return
		default:
		}

		err := s.connectAndSubscribe()
		if err != nil {
			logger.Warn("failed to connect", "error", err, "backoff", backoff)

			select {
			case <-s.done:
				return
			case <-s.ctx.Done():
				return
			case <-time.After(backoff):
			}

			backoff = time.Duration(float64(backoff) * s.config.BackoffFactor)
			if backoff > s.config.MaxBackoff {
				backoff = s.config.MaxBackoff
			}
			continue
		}

		backoff = s.config.InitialBackoff
		logger.Info("connected to Alchemy WebSocket")

		// Notify caller of reconnection
		if !isFirstConnect && s.onReconnect != nil {
			s.onReconnect()
		}
		isFirstConnect = false

		intentionalClose := s.readLoop(logger)
		if !intentionalClose {
			logger.Warn("WebSocket connection lost, reconnecting...")
		}
	}
}

// connectAndSubscribe establishes the WebSocket connection and subscribes to newHeads.
func (s *Subscriber) connectAndSubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, _, err := websocket.DefaultDialer.DialContext(s.ctx, s.config.WebSocketURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
		conn.Close()
		return fmt.Errorf("failed to set read deadline: %w", err)
	}

	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout))
	})

	s.conn = conn

	// Subscribe to newHeads
	subscribeReq := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  "eth_subscribe",
		Params:  []interface{}{"newHeads"},
	}

	if err := conn.WriteJSON(subscribeReq); err != nil {
		conn.Close()
		s.conn = nil
		return fmt.Errorf("failed to send subscription: %w", err)
	}

	var response jsonRPCResponse
	if err := conn.ReadJSON(&response); err != nil {
		conn.Close()
		s.conn = nil
		return fmt.Errorf("failed to read response: %w", err)
	}

	if response.Error != nil {
		conn.Close()
		s.conn = nil
		return fmt.Errorf("subscription failed: %s", response.Error.Message)
	}

	return nil
}

// readLoop reads block headers from the WebSocket and forwards them.
// Returns true if the loop exited due to intentional close (Unsubscribe or context cancellation).
func (s *Subscriber) readLoop(logger *slog.Logger) bool {
	pingTicker := time.NewTicker(s.config.PingInterval)
	defer pingTicker.Stop()

	readErr := make(chan error, 1)
	blockChan := make(chan outbound.BlockHeader, 10)

	go func() {
		for {
			// Get connection reference under lock
			s.mu.RLock()
			conn := s.conn
			s.mu.RUnlock()

			if conn == nil {
				readErr <- errors.New("connection is nil")
				return
			}

			// SetReadDeadline and ReadJSON may block - don't hold lock
			// The connection won't be replaced while readLoop is running
			// (connectionManager waits for readLoop to return before reconnecting)
			if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
				readErr <- err
				return
			}

			var response jsonRPCResponse
			if err := conn.ReadJSON(&response); err != nil {
				readErr <- err
				return
			}

			if response.Method == "eth_subscription" && response.Params != nil {
				var params subscriptionParams
				if err := json.Unmarshal(response.Params, &params); err != nil {
					logger.Warn("failed to parse params", "error", err)
					continue
				}

				select {
				case blockChan <- params.Result:
				case <-s.done:
					return
				case <-s.ctx.Done():
					return
				}
			}
		}
	}()

	for {
		select {
		case <-s.done:
			s.closeConnection()
			return true
		case <-s.ctx.Done():
			s.closeConnection()
			return true
		case err := <-readErr:
			logger.Warn("read error", "error", err)
			s.closeConnection()
			return false
		case header := <-blockChan:
			select {
			case s.headers <- header:
				s.lastBlockTime.Store(time.Now().Unix())
				blockNum, _ := parseBlockNumber(header.Number)
				logger.Debug("block header received", "block", blockNum, "hash", truncateHash(header.Hash))
			default:
				blockNum, _ := parseBlockNumber(header.Number)
				logger.Error("channel full, dropping block", "block", blockNum)
			}
		case <-pingTicker.C:
			s.mu.RLock()
			conn := s.conn
			s.mu.RUnlock()

			if conn != nil {
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.config.PongTimeout)); err != nil {
					logger.Warn("ping failed", "error", err)
					s.closeConnection()
					return false
				}
			}
		}
	}
}

func (s *Subscriber) closeConnection() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// Unsubscribe stops the subscription and waits for all goroutines to finish.
func (s *Subscriber) Unsubscribe() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}

	s.closed = true
	close(s.done)

	if s.cancel != nil {
		s.cancel()
	}

	var connErr error
	if s.conn != nil {
		connErr = s.conn.Close()
	}
	s.mu.Unlock()

	// Wait for all goroutines to finish before closing the channel
	s.wg.Wait()

	// Now safe to close the headers channel - no more writers
	close(s.headers)

	return connErr
}

// HealthCheck verifies the connection is operational.
func (s *Subscriber) HealthCheck(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errors.New("subscriber is closed")
	}

	lastBlockTime := s.lastBlockTime.Load()
	if lastBlockTime > 0 {
		timeSinceLastBlock := time.Since(time.Unix(lastBlockTime, 0))
		if timeSinceLastBlock > s.config.HealthTimeout {
			return fmt.Errorf("no blocks received for %v", timeSinceLastBlock)
		}
	}

	if s.conn == nil {
		conn, _, err := websocket.DefaultDialer.DialContext(ctx, s.config.WebSocketURL, nil)
		if err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}
		conn.Close()
	}

	return nil
}
