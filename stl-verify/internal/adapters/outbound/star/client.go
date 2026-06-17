// Package star adapts the upstream Star risk-capital monitor HTTP API to the
// CapitalMetricsProvider port.
package star

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// DefaultBaseURL is the public Star risk-capital monitor endpoint. It is also
// recorded as the benchmark source on each stored snapshot.
const DefaultBaseURL = "https://info-sky.blockanalitica.com/star-monitoring/risk-capital/primes/"

var _ outbound.CapitalMetricsProvider = (*Client)(nil)

type ClientConfig struct {
	BaseURL    string
	Timeout    time.Duration
	HTTPClient *http.Client
	Logger     *slog.Logger
}

type Client struct {
	baseURL string
	http    *http.Client
	logger  *slog.Logger
}

func NewClient(cfg ClientConfig) (*Client, error) {
	baseURL := cfg.BaseURL
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}

	httpClient := cfg.HTTPClient
	if httpClient == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		httpClient = &http.Client{Timeout: timeout}
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Client{
		baseURL: baseURL,
		http:    httpClient,
		logger:  logger.With("component", "star-client"),
	}, nil
}

func (c *Client) Name() string { return "star-risk-capital" }

type riskCapitalRowDTO struct {
	Star               string `json:"star"`
	Exposure           string `json:"exposure"`
	TotalRC            string `json:"total_rc"`
	FinancialRRC       string `json:"financial_rrc"`
	ExposureShare      string `json:"exposure_share"`
	RiskToleranceRatio string `json:"risk_tolerance_ratio"`
}

type riskCapitalResponseDTO struct {
	Data *struct {
		Results []riskCapitalRowDTO `json:"results"`
	} `json:"data"`
	Status  *int  `json:"status"`
	Success *bool `json:"success"`
}

func (c *Client) FetchRiskCapital(ctx context.Context) ([]outbound.RiskCapitalRow, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL, nil)
	if err != nil {
		return nil, fmt.Errorf("building request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching star risk capital: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading star risk capital response: %w", err)
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("star risk capital upstream returned status %d: %s", resp.StatusCode, truncate(body, 500))
	}

	var parsed riskCapitalResponseDTO
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("decoding star risk capital response: %w", err)
	}

	if parsed.Success != nil && !*parsed.Success {
		return nil, fmt.Errorf("star risk capital upstream reported failure (status=%v)", parsed.Status)
	}
	if parsed.Status != nil && *parsed.Status >= http.StatusBadRequest {
		return nil, fmt.Errorf("star risk capital upstream reported status %d", *parsed.Status)
	}
	if parsed.Data == nil {
		return nil, nil
	}

	rows := make([]outbound.RiskCapitalRow, 0, len(parsed.Data.Results))
	for _, r := range parsed.Data.Results {
		rows = append(rows, outbound.RiskCapitalRow{
			Star:               r.Star,
			Exposure:           r.Exposure,
			TotalRC:            r.TotalRC,
			FinancialRRC:       r.FinancialRRC,
			ExposureShare:      r.ExposureShare,
			RiskToleranceRatio: r.RiskToleranceRatio,
		})
	}
	return rows, nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n])
}
