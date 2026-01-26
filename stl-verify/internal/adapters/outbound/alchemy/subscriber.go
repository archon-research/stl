// subscriber.go implements a WebSocket client for Alchemy's newHeads subscription.
//
// Key behaviors:
//   - Automatic reconnection with exponential backoff (configurable via
//     InitialBackoff, MaxBackoff, BackoffFactor)
//   - Non-blocking sends to the headers channel; blocks are DROPPED if buffer is full
//   - Ping/pong keepalive to detect stale connections (PongTimeout configurable)
//   - Health checks via HealthCheck() to verify blocks are being received
//   - Thread-safe: All public methods are safe for concurrent use
//
// Channel buffer sizing:
//   - Default ChannelBufferSize is 100 blocks
//   - If your consumer processes blocks slower than ~12 seconds/block (Ethereum
//     block time), increase the buffer or optimize consumption to avoid drops
package alchemy

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

	"github.com/archon-research/stl/stl-verify/internal/pkg/hexutil"
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

	// HandshakeTimeout is the maximum time to wait for the WebSocket handshake to complete.
	HandshakeTimeout time.Duration

	// ChannelBufferSize is the size of the block header channel buffer.
	ChannelBufferSize int

	// HealthTimeout is how long without receiving a block
	// before considering the connection unhealthy.
	HealthTimeout time.Duration

	// Logger is the structured logger for the subscriber.
	Logger *slog.Logger

	// Telemetry is the optional OpenTelemetry instrumentation.
	// If nil, no metrics or traces are recorded.
	Telemetry *Telemetry
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
		HandshakeTimeout:  10 * time.Second,
		ChannelBufferSize: 100,
		HealthTimeout:     30 * time.Second,
		Logger:            slog.Default(),
	}
}

// Subscriber implements BlockSubscriber using Alchemy's WebSocket API.
// It handles WebSocket connection, subscription, and emitting raw headers.
type Subscriber struct {
	config    SubscriberConfig
	conn      *websocket.Conn
	mu        sync.RWMutex
	done      chan struct{}
	closed    bool
	headers   chan outbound.BlockHeader
	ctx       context.Context
	cancel    context.CancelFunc
	telemetry *Telemetry

	lastBlockTime atomic.Int64

	// disconnectedSince tracks when the connection was lost (unix timestamp).
	// 0 means currently connected.
	disconnectedSince atomic.Int64

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
	if config.HandshakeTimeout == 0 {
		config.HandshakeTimeout = defaults.HandshakeTimeout
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
		config:    config,
		done:      make(chan struct{}),
		headers:   make(chan outbound.BlockHeader, config.ChannelBufferSize),
		telemetry: config.Telemetry,
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

		// Mark as connected
		s.disconnectedSince.Store(0)

		// Record telemetry for connection state
		if s.telemetry != nil {
			s.telemetry.RecordConnectionUp(s.ctx)
		}

		// Notify caller of reconnection and record metric
		if !isFirstConnect {
			if s.telemetry != nil {
				s.telemetry.RecordReconnection(s.ctx)
			}
			if s.onReconnect != nil {
				s.onReconnect()
			}
		}
		isFirstConnect = false

		if err := s.readLoop(logger); err != nil {
			logger.Warn("WebSocket connection lost, reconnecting...", "error", err)
		}
	}
}

// connectAndSubscribe establishes the WebSocket connection and subscribes to newHeads.
func (s *Subscriber) connectAndSubscribe() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: s.config.HandshakeTimeout,
	}
	conn, _, err := dialer.DialContext(s.ctx, s.config.WebSocketURL, nil)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
		closeErr := conn.Close()
		return errors.Join(fmt.Errorf("failed to set read deadline: %w", err), closeErr)
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
		closeErr := conn.Close()
		s.conn = nil
		return errors.Join(fmt.Errorf("failed to send subscription: %w", err), closeErr)
	}

	var response jsonRPCResponse
	if err := conn.ReadJSON(&response); err != nil {
		closeErr := conn.Close()
		s.conn = nil
		return errors.Join(fmt.Errorf("failed to read response: %w", err), closeErr)
	}

	if response.Error != nil {
		closeErr := conn.Close()
		s.conn = nil
		return errors.Join(fmt.Errorf("subscription failed: %s", response.Error.Message), closeErr)
	}

	return nil
}

// readLoop reads block headers from the WebSocket and forwards them.
// Returns nil if the loop exited due to intentional close (Unsubscribe or context cancellation).
// Returns an error if the connection was lost unexpectedly.
func (s *Subscriber) readLoop(logger *slog.Logger) error {
	pingTicker := time.NewTicker(s.config.PingInterval)
	defer pingTicker.Stop()

	readErr := make(chan error, 1)
	blockChan := make(chan outbound.BlockHeader, 10)

	// readerDone signals the reader goroutine to exit when the main loop exits.
	// This prevents goroutine leaks when the main loop exits while the reader
	// is blocked trying to send to blockChan.
	readerDone := make(chan struct{})
	defer close(readerDone)

	go func() {
		for {
			// Get connection reference under lock
			s.mu.RLock()
			conn := s.conn
			s.mu.RUnlock()

			if conn == nil {
				select {
				case readErr <- errors.New("connection is nil"):
				case <-readerDone:
				}
				return
			}

			// SetReadDeadline and ReadJSON may block - don't hold lock
			// The connection won't be replaced while readLoop is running
			// (connectionManager waits for readLoop to return before reconnecting)
			if err := conn.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
				select {
				case readErr <- err:
				case <-readerDone:
				}
				return
			}

			var response jsonRPCResponse
			if err := conn.ReadJSON(&response); err != nil {
				select {
				case readErr <- err:
				case <-readerDone:
				}
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
				case <-readerDone:
					return
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
			return nil
		case <-s.ctx.Done():
			s.closeConnection()
			return nil
		case err := <-readErr:
			s.closeConnection()
			return fmt.Errorf("read error: %w", err)
		case header := <-blockChan:
			blockNum, err := hexutil.ParseInt64(header.Number)
			if err != nil {
				logger.Error("failed to parse block number, ignoring block", "raw", header.Number, "hash", header.Hash, "error", err)
				continue
			}
			select {
			case s.headers <- header:
				s.lastBlockTime.Store(time.Now().Unix())
				logger.Debug("block header received", "block", blockNum, "hash", truncateHash(header.Hash))
				if s.telemetry != nil {
					s.telemetry.RecordBlockReceived(s.ctx, blockNum)
				}
			default:
				logger.Error("channel full, dropping block", "block", blockNum)
				if s.telemetry != nil {
					s.telemetry.RecordBlockDropped(s.ctx, blockNum)
				}
			}
		case <-pingTicker.C:
			s.mu.RLock()
			conn := s.conn
			s.mu.RUnlock()

			if conn != nil {
				if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.config.PongTimeout)); err != nil {
					s.closeConnection()
					return fmt.Errorf("ping failed: %w", err)
				}
			}
		}
	}
}

func (s *Subscriber) closeConnection() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			s.config.Logger.Warn("error closing WebSocket connection", "error", err)
		}
		s.conn = nil
		// Track when we became disconnected
		s.disconnectedSince.Store(time.Now().Unix())
		// Record telemetry for connection state
		if s.telemetry != nil {
			s.telemetry.RecordConnectionDown(s.ctx)
		}
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

// HealthCheck verifies the subscription is operational.
// Returns an error if:
// - The subscriber is closed
// - No blocks have been received within HealthTimeout
// - The connection is currently disconnected (with duration)
func (s *Subscriber) HealthCheck(ctx context.Context) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return errors.New("subscriber is closed")
	}

	// Check if we're currently disconnected
	disconnectedSince := s.disconnectedSince.Load()
	if disconnectedSince > 0 {
		disconnectedFor := time.Since(time.Unix(disconnectedSince, 0))
		return fmt.Errorf("connection lost %v ago, reconnecting", disconnectedFor.Truncate(time.Second))
	}

	// Check time since last block
	lastBlockTime := s.lastBlockTime.Load()
	if lastBlockTime > 0 {
		timeSinceLastBlock := time.Since(time.Unix(lastBlockTime, 0))
		if timeSinceLastBlock > s.config.HealthTimeout {
			return fmt.Errorf("no blocks received for %v", timeSinceLastBlock.Truncate(time.Second))
		}
	}

	// Connection is established but no blocks yet - this is normal during startup
	if s.conn == nil {
		return errors.New("not yet connected")
	}

	return nil
}
