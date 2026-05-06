package sky

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/httpclient"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultPrimesURL = "https://info-sky.blockanalitica.com/star-monitoring/risk-capital/primes/"

// Compile-time check that Client implements outbound.RiskCapitalProvider.
var _ outbound.RiskCapitalProvider = (*Client)(nil)

// ClientConfig holds configuration for the Sky risk-capital client.
type ClientConfig struct {
	PrimesURL      string
	Timeout        time.Duration
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Logger         *slog.Logger
}

// Client fetches risk-capital snapshots from Sky's approved endpoint.
type Client struct {
	primesURL  string
	httpClient *httpclient.Client
	logger     *slog.Logger
}

// NewClient creates a new Sky risk-capital client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.PrimesURL == "" {
		cfg.PrimesURL = defaultPrimesURL
	}

	httpCfg := httpclient.DefaultConfig()
	if cfg.Timeout > 0 {
		httpCfg.Timeout = cfg.Timeout
	}
	if cfg.MaxRetries > 0 {
		httpCfg.MaxRetries = cfg.MaxRetries
	}
	if cfg.InitialBackoff > 0 {
		httpCfg.InitialBackoff = cfg.InitialBackoff
	}
	if cfg.MaxBackoff > 0 {
		httpCfg.MaxBackoff = cfg.MaxBackoff
	}
	if cfg.BackoffFactor > 0 {
		httpCfg.BackoffFactor = cfg.BackoffFactor
	}

	logger := cfg.Logger.With("component", "sky-risk-capital-client")
	return &Client{
		primesURL:  cfg.PrimesURL,
		httpClient: httpclient.NewClient(httpCfg, logger, nil),
		logger:     logger,
	}, nil
}

// FetchPrimeRows fetches prime-level risk-capital rows.
func (c *Client) FetchPrimeRows(ctx context.Context) ([]outbound.RiskCapitalPrimeRow, error) {
	var payload primesResponse
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: c.primesURL}, &payload); err != nil {
		return nil, fmt.Errorf("fetching sky risk-capital primes: %w", err)
	}

	rows := make([]outbound.RiskCapitalPrimeRow, 0, len(payload.Data.Results))
	for _, row := range payload.Data.Results {
		primeName := strings.TrimSpace(row.Star)
		if primeName == "" {
			c.logger.Warn("skipping sky row with empty prime name")
			continue
		}

		totalRC := strings.TrimSpace(row.TotalRC.String())
		financialRRC := strings.TrimSpace(row.FinancialRRC.String())
		exposure := strings.TrimSpace(row.Exposure.String())
		if totalRC == "" || financialRRC == "" || exposure == "" {
			c.logger.Warn("skipping sky row with missing numeric fields", "prime", primeName)
			continue
		}

		rows = append(rows, outbound.RiskCapitalPrimeRow{
			PrimeName:          primeName,
			TotalRC:            totalRC,
			FinancialRRC:       financialRRC,
			Exposure:           exposure,
			RiskToleranceRatio: strings.TrimSpace(row.RiskToleranceRatio.String()),
		})
	}

	return rows, nil
}

type primesResponse struct {
	Data struct {
		Results []primeRow `json:"results"`
	} `json:"data"`
}

type primeRow struct {
	Star               string      `json:"star"`
	Exposure           json.Number `json:"exposure"`
	FinancialRRC       json.Number `json:"financial_rrc"`
	TotalRC            json.Number `json:"total_rc"`
	RiskToleranceRatio json.Number `json:"risk_tolerance_ratio"`
}
