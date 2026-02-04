// Package coingecko implements the PriceProvider interface using CoinGecko's API.
// It provides methods for fetching current and historical price data with:
//   - Automatic retry logic with exponential backoff for transient failures
//   - Configurable timeouts and backoff parameters
//   - Rate limiting to stay within API limits
package coingecko

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"golang.org/x/time/rate"
)

// Compile-time check that Client implements outbound.PriceProvider.
var _ outbound.PriceProvider = (*Client)(nil)

// ClientConfig holds configuration for the CoinGecko client.
type ClientConfig struct {
	// APIKey is the CoinGecko Pro API key.
	APIKey string

	// BaseURL is the CoinGecko API base URL.
	// Defaults to https://pro-api.coingecko.com/api/v3
	BaseURL string

	// Timeout is the maximum time to wait for a single HTTP request.
	Timeout time.Duration

	// MaxRetries is the maximum number of retry attempts for transient failures.
	MaxRetries int

	// InitialBackoff is the initial delay before the first retry.
	InitialBackoff time.Duration

	// MaxBackoff is the maximum delay between retries.
	MaxBackoff time.Duration

	// BackoffFactor is the multiplier applied to backoff after each retry.
	BackoffFactor float64

	// RateLimitPerMin is the rate limit in requests per minute.
	// Defaults to 450 to stay safely under CoinGecko Pro's 500/min limit.
	RateLimitPerMin int

	// Logger is the structured logger for the client.
	Logger *slog.Logger

	// HTTPClient is an optional custom HTTP client.
	HTTPClient *http.Client
}

// ClientConfigDefaults returns a config with default values.
func ClientConfigDefaults() ClientConfig {
	return ClientConfig{
		BaseURL:         "https://pro-api.coingecko.com/api/v3",
		Timeout:         30 * time.Second,
		MaxRetries:      3,
		InitialBackoff:  500 * time.Millisecond,
		MaxBackoff:      10 * time.Second,
		BackoffFactor:   2.0,
		RateLimitPerMin: 450,
		Logger:          slog.Default(),
	}
}

// Client implements PriceProvider using CoinGecko's API.
type Client struct {
	config      ClientConfig
	httpClient  *http.Client
	logger      *slog.Logger
	limiter     *rate.Limiter
	retryConfig retry.Config
}

// NewClient creates a new CoinGecko API client.
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

	// Calculate rate limiter: requests per second from requests per minute
	rps := float64(config.RateLimitPerMin) / 60.0
	limiter := rate.NewLimiter(rate.Limit(rps), 1)

	return &Client{
		config:     config,
		httpClient: httpClient,
		logger:     config.Logger.With("component", "coingecko-client"),
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
	if config.BaseURL == "" {
		config.BaseURL = defaults.BaseURL
	}
	if config.Timeout == 0 {
		config.Timeout = defaults.Timeout
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = defaults.MaxRetries
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
	if config.RateLimitPerMin == 0 {
		config.RateLimitPerMin = defaults.RateLimitPerMin
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
}

// Name returns the provider name.
func (c *Client) Name() string {
	return "coingecko"
}

// SupportsHistorical returns true since CoinGecko supports historical data.
func (c *Client) SupportsHistorical() bool {
	return true
}

// GetCurrentPrices fetches current prices for the given asset IDs.
// Uses the /simple/price endpoint which supports up to 250 coins per request.
func (c *Client) GetCurrentPrices(ctx context.Context, assetIDs []string) ([]outbound.PriceData, error) {
	if len(assetIDs) == 0 {
		return nil, nil
	}

	results := make([]outbound.PriceData, 0, len(assetIDs))
	batchSize := 250

	for i := 0; i < len(assetIDs); i += batchSize {
		end := i + batchSize
		if end > len(assetIDs) {
			end = len(assetIDs)
		}
		batch := assetIDs[i:end]

		batchResults, err := c.getCurrentPricesBatch(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("fetching prices for batch starting at %d: %w", i, err)
		}
		results = append(results, batchResults...)
	}

	return results, nil
}

func (c *Client) getCurrentPricesBatch(ctx context.Context, assetIDs []string) ([]outbound.PriceData, error) {
	endpoint := fmt.Sprintf("%s/simple/price", c.config.BaseURL)
	params := url.Values{
		"ids":                     {strings.Join(assetIDs, ",")},
		"vs_currencies":           {"usd"},
		"include_market_cap":      {"true"},
		"include_last_updated_at": {"true"},
	}

	var response simplePriceResponse
	if err := c.doRequest(ctx, endpoint, params, &response); err != nil {
		return nil, err
	}

	results := make([]outbound.PriceData, 0, len(response))
	for assetID, data := range response {
		var marketCap *float64
		if data.USDMarketCap > 0 {
			marketCap = &data.USDMarketCap
		}
		results = append(results, outbound.PriceData{
			SourceAssetID: assetID,
			PriceUSD:      data.USD,
			MarketCapUSD:  marketCap,
			Timestamp:     time.Unix(data.LastUpdated, 0),
		})
	}

	return results, nil
}

// GetHistoricalData fetches historical prices and volumes for a single asset.
// Uses the /coins/{id}/market_chart/range endpoint.
// Note: CoinGecko returns hourly data for ranges up to 90 days, daily for larger ranges.
// For best results, fetch in 30-day chunks.
func (c *Client) GetHistoricalData(ctx context.Context, assetID string, from, to time.Time) (*outbound.HistoricalData, error) {
	endpoint := fmt.Sprintf("%s/coins/%s/market_chart/range", c.config.BaseURL, assetID)
	params := url.Values{
		"vs_currency": {"usd"},
		"from":        {fmt.Sprintf("%d", from.Unix())},
		"to":          {fmt.Sprintf("%d", to.Unix())},
	}

	var response marketChartRangeResponse
	if err := c.doRequest(ctx, endpoint, params, &response); err != nil {
		return nil, err
	}

	result := &outbound.HistoricalData{
		SourceAssetID: assetID,
		Prices:        make([]outbound.PricePoint, 0, len(response.Prices)),
		Volumes:       make([]outbound.VolumePoint, 0, len(response.TotalVolumes)),
		MarketCaps:    make([]outbound.MarketCapPoint, 0, len(response.MarketCaps)),
	}

	for _, p := range response.Prices {
		if len(p) >= 2 {
			result.Prices = append(result.Prices, outbound.PricePoint{
				Timestamp: time.UnixMilli(int64(p[0])),
				PriceUSD:  p[1],
			})
		}
	}

	for _, v := range response.TotalVolumes {
		if len(v) >= 2 {
			result.Volumes = append(result.Volumes, outbound.VolumePoint{
				Timestamp: time.UnixMilli(int64(v[0])),
				VolumeUSD: v[1],
			})
		}
	}

	for _, m := range response.MarketCaps {
		if len(m) >= 2 {
			result.MarketCaps = append(result.MarketCaps, outbound.MarketCapPoint{
				Timestamp:    time.UnixMilli(int64(m[0])),
				MarketCapUSD: m[1],
			})
		}
	}

	return result, nil
}

func (c *Client) doRequest(ctx context.Context, endpoint string, params url.Values, result any) error {
	fullURL := endpoint
	if len(params) > 0 {
		fullURL = fmt.Sprintf("%s?%s", endpoint, params.Encode())
	}

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
	req.Header.Set("x-cg-pro-api-key", c.config.APIKey)

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
		var apiErr coinGeckoError
		if jsonErr := json.Unmarshal(body, &apiErr); jsonErr == nil && apiErr.Error != "" {
			return &nonRetryableError{err: fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, apiErr.Error)}
		}
		return &nonRetryableError{err: fmt.Errorf("client error (HTTP %d): %s", resp.StatusCode, string(body))}
	}

	if err := json.Unmarshal(body, result); err != nil {
		return &nonRetryableError{err: fmt.Errorf("parsing response: %w", err)}
	}

	return nil
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
