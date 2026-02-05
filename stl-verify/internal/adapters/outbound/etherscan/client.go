// Package etherscan implements the BlockVerifier interface using Etherscan's API.
// It provides methods for fetching block data with:
//   - Automatic retry logic with exponential backoff for transient failures
//   - Configurable timeouts and backoff parameters
//   - Rate limiting to stay within API limits
package etherscan

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"golang.org/x/time/rate"
)

// Compile-time check that Client implements outbound.BlockVerifier.
var _ outbound.BlockVerifier = (*Client)(nil)

// ClientConfig holds configuration for the Etherscan client.
type ClientConfig struct {
	// APIKey is the Etherscan API key.
	APIKey string

	// ChainID is the target blockchain network (e.g., 1 for Ethereum mainnet).
	// Defaults to 1.
	ChainID int64

	// BaseURL is the Etherscan API V2 base URL.
	// Defaults to https://api.etherscan.io/v2/api
	BaseURL string

	// Timeout is the maximum time to wait for a single HTTP request.
	Timeout time.Duration

	// MaxRetries is the maximum number of retry attempts for transient failures.
	// Use -1 to explicitly disable retries (0 uses default of 3).
	MaxRetries int

	// InitialBackoff is the initial delay before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each retry.
	BackoffFactor float64

	// RateLimitPerSec is the rate limit in requests per second.
	// Defaults to 5 (Etherscan free tier limit).
	RateLimitPerSec int

	// Logger is the structured logger for the client.
	Logger *slog.Logger

	// HTTPClient is an optional custom HTTP client.
	HTTPClient *http.Client
}

// ClientConfigDefaults returns a config with default values.
func ClientConfigDefaults() ClientConfig {
	return ClientConfig{
		ChainID:         1, // Ethereum mainnet
		BaseURL:         "https://api.etherscan.io/v2/api",
		Timeout:         30 * time.Second,
		MaxRetries:      3,
		InitialBackoff:  1 * time.Second,
		MaxBackoff:      10 * time.Second,
		BackoffFactor:   2.0,
		RateLimitPerSec: 2, // Free tier: 3 calls/sec, use 2 to be safe
		Logger:          slog.Default(),
	}
}

// Client implements BlockVerifier using Etherscan's API.
type Client struct {
	config      ClientConfig
	httpClient  *http.Client
	logger      *slog.Logger
	limiter     *rate.Limiter
	retryConfig retry.Config
}

// NewClient creates a new Etherscan API client.
func NewClient(config ClientConfig) (*Client, error) {
	if config.APIKey == "" {
		return nil, errors.New("APIKey is required")
	}

	defaults := ClientConfigDefaults()
	applyDefaults(&config, defaults)

	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: config.Timeout,
		}
	}

	limiter := rate.NewLimiter(rate.Limit(config.RateLimitPerSec), 1)

	return &Client{
		config:     config,
		httpClient: httpClient,
		logger:     config.Logger.With("component", "etherscan-client"),
		limiter:    limiter,
		retryConfig: retry.Config{
			MaxRetries:     config.MaxRetries,
			InitialBackoff: config.InitialBackoff,
			MaxBackoff:     config.MaxBackoff,
			BackoffFactor:  config.BackoffFactor,
			Jitter:         false, // Keep deterministic for API rate limiting
		},
	}, nil
}

func applyDefaults(config *ClientConfig, defaults ClientConfig) {
	if config.ChainID == 0 {
		config.ChainID = defaults.ChainID
	}
	if config.BaseURL == "" {
		config.BaseURL = defaults.BaseURL
	}
	if config.Timeout == 0 {
		config.Timeout = defaults.Timeout
	}
	// MaxRetries: 0 means use default, negative values disable retries (set to 0)
	if config.MaxRetries == 0 {
		config.MaxRetries = defaults.MaxRetries
	} else if config.MaxRetries < 0 {
		config.MaxRetries = 0
	}
	if config.InitialBackoff == 0 {
		config.InitialBackoff = defaults.InitialBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaults.MaxBackoff
	}
	if config.BackoffFactor == 0 {
		config.BackoffFactor = defaults.BackoffFactor
	}
	if config.RateLimitPerSec == 0 {
		config.RateLimitPerSec = defaults.RateLimitPerSec
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
}

// Name returns the verifier name.
func (c *Client) Name() string {
	return "etherscan"
}

// GetBlockByNumber fetches a block by its number from Etherscan.
func (c *Client) GetBlockByNumber(ctx context.Context, number int64) (*outbound.CanonicalBlock, error) {
	hexNumber := fmt.Sprintf("0x%x", number)
	params := url.Values{
		"chainid": {fmt.Sprintf("%d", c.config.ChainID)},
		"module":  {"proxy"},
		"action":  {"eth_getBlockByNumber"},
		"tag":     {hexNumber},
		"boolean": {"false"}, // Don't include full transaction objects
		"apikey":  {c.config.APIKey},
	}

	var response proxyResponse
	if err := c.doRequest(ctx, params, &response); err != nil {
		return nil, fmt.Errorf("fetching block %d: %w", number, err)
	}

	return c.parseBlockResponse(response, number)
}

// GetBlockByHash fetches a block by its hash from Etherscan.
func (c *Client) GetBlockByHash(ctx context.Context, hash string) (*outbound.CanonicalBlock, error) {
	params := url.Values{
		"chainid": {fmt.Sprintf("%d", c.config.ChainID)},
		"module":  {"proxy"},
		"action":  {"eth_getBlockByHash"},
		"tag":     {hash},
		"boolean": {"false"}, // Don't include full transaction objects
		"apikey":  {c.config.APIKey},
	}

	var response proxyResponse
	if err := c.doRequest(ctx, params, &response); err != nil {
		return nil, fmt.Errorf("fetching block by hash %s: %w", hash, err)
	}

	return c.parseBlockResponse(response, 0)
}

// GetLatestBlockNumber returns the latest block number from Etherscan.
func (c *Client) GetLatestBlockNumber(ctx context.Context) (int64, error) {
	params := url.Values{
		"chainid": {fmt.Sprintf("%d", c.config.ChainID)},
		"module":  {"proxy"},
		"action":  {"eth_blockNumber"},
		"apikey":  {c.config.APIKey},
	}

	var response proxyResponse
	if err := c.doRequest(ctx, params, &response); err != nil {
		return 0, fmt.Errorf("fetching latest block number: %w", err)
	}

	if response.Error != nil {
		return 0, &nonRetryableError{err: fmt.Errorf("API error: %s", response.Error.Message)}
	}

	hexStr, ok := response.Result.(string)
	if !ok {
		return 0, &nonRetryableError{err: fmt.Errorf("unexpected result type: %T", response.Result)}
	}

	return parseHexInt64(hexStr)
}

func (c *Client) parseBlockResponse(response proxyResponse, expectedNumber int64) (*outbound.CanonicalBlock, error) {
	if response.Error != nil {
		return nil, &nonRetryableError{err: fmt.Errorf("API error: %s", response.Error.Message)}
	}

	if response.Result == nil {
		return nil, nil // Block not found
	}

	// Re-marshal and unmarshal to parse the block response
	resultJSON, err := json.Marshal(response.Result)
	if err != nil {
		return nil, &nonRetryableError{err: fmt.Errorf("marshaling result: %w", err)}
	}

	var block blockResponse
	if err := json.Unmarshal(resultJSON, &block); err != nil {
		return nil, &nonRetryableError{err: fmt.Errorf("parsing block response: %w", err)}
	}

	// Check for null/empty response (block not found)
	if block.Hash == "" {
		return nil, nil
	}

	number, err := parseHexInt64(block.Number)
	if err != nil {
		return nil, &nonRetryableError{err: fmt.Errorf("parsing block number: %w", err)}
	}

	timestamp, err := parseHexInt64(block.Timestamp)
	if err != nil {
		return nil, &nonRetryableError{err: fmt.Errorf("parsing timestamp: %w", err)}
	}

	return &outbound.CanonicalBlock{
		Number:    number,
		Hash:      block.Hash,
		Timestamp: timestamp,
	}, nil
}

func (c *Client) doRequest(ctx context.Context, params url.Values, result any) error {
	fullURL := fmt.Sprintf("%s?%s", c.config.BaseURL, params.Encode())

	isRetryable := func(err error) bool {
		var nonRetryable *nonRetryableError
		return !errors.As(err, &nonRetryable)
	}

	onRetry := func(attempt int, err error, backoff time.Duration) {
		c.logger.Warn("request failed, retrying",
			"attempt", attempt,
			"maxRetries", c.retryConfig.MaxRetries,
			"backoff", backoff,
			"error", err,
		)
	}

	return retry.DoVoid(ctx, c.retryConfig, isRetryable, onRetry, func() error {
		if err := c.limiter.Wait(ctx); err != nil {
			return &nonRetryableError{err: fmt.Errorf("rate limiter: %w", err)}
		}
		return c.doSingleRequest(ctx, fullURL, result)
	})
}

func (c *Client) doSingleRequest(ctx context.Context, fullURL string, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return &nonRetryableError{err: fmt.Errorf("creating request: %w", err)}
	}

	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Warn("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited (HTTP 429)")
	}

	if resp.StatusCode >= 500 {
		return fmt.Errorf("server error (HTTP %d)", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode >= 400 {
		var apiErr etherscanError
		if jsonErr := json.Unmarshal(body, &apiErr); jsonErr == nil && apiErr.Message != "" {
			return &nonRetryableError{err: fmt.Errorf("API error (HTTP %d): %s - %s", resp.StatusCode, apiErr.Message, apiErr.Result)}
		}
		return &nonRetryableError{err: fmt.Errorf("client error (HTTP %d): %s", resp.StatusCode, string(body))}
	}

	// Check for Etherscan-specific error format (status: "0")
	var apiErr etherscanError
	if jsonErr := json.Unmarshal(body, &apiErr); jsonErr == nil && apiErr.Status == "0" {
		// Check for rate limit message
		if strings.Contains(strings.ToLower(apiErr.Result), "rate limit") {
			return fmt.Errorf("rate limited: %s", apiErr.Result)
		}
		return &nonRetryableError{err: fmt.Errorf("API error: %s - %s", apiErr.Message, apiErr.Result)}
	}

	if err := json.Unmarshal(body, result); err != nil {
		return &nonRetryableError{err: fmt.Errorf("parsing response: %w", err)}
	}

	return nil
}

// parseHexInt64 parses a hex string (with or without 0x prefix) to int64.
func parseHexInt64(hexStr string) (int64, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	if hexStr == "" {
		return 0, nil
	}
	return strconv.ParseInt(hexStr, 16, 64)
}

// nonRetryableError wraps errors that should not be retried.
type nonRetryableError struct {
	err error
}

func (e *nonRetryableError) Error() string {
	return e.err.Error()
}

func (e *nonRetryableError) Unwrap() error {
	return e.err
}
