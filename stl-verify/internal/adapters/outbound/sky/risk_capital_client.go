// Package sky reads Sky's Star Agents Risk Capital & Requirements Monitor.
//
// Two routes are used per cycle: the list route enumerates the primes the
// monitor tracks, and the per-prime detail route carries the figures. The list
// is not redundant — the detail route answers an unknown star with a 500 that
// is indistinguishable from a genuine fault, so the list is the only safe way
// to tell "not covered" from "monitor is down".
//
// Upstream query parameters are not trustworthy: ?days_ago=, ?date= and
// ?order= are accepted and silently ignored (verified by byte-identical
// responses across values), so none are sent and no ordering is assumed.
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

const defaultBaseURL = "https://info-sky.blockanalitica.com/star-monitoring/risk-capital"

// Compile-time check that Client implements outbound.RiskCapitalProvider.
var _ outbound.RiskCapitalProvider = (*Client)(nil)

// ClientConfig holds configuration for the Sky risk-capital client.
type ClientConfig struct {
	BaseURL        string
	Timeout        time.Duration
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Logger         *slog.Logger
}

// Client fetches risk-capital snapshots from Sky's Star monitor.
type Client struct {
	baseURL    string
	httpClient *httpclient.Client
	logger     *slog.Logger
}

// NewClient creates a new Sky risk-capital client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
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
		baseURL:    strings.TrimRight(cfg.BaseURL, "/"),
		httpClient: httpclient.NewClient(httpCfg, logger, nil),
		logger:     logger,
	}, nil
}

// FetchPrimeSnapshots returns a snapshot for every prime the monitor tracks.
func (c *Client) FetchPrimeSnapshots(ctx context.Context) ([]outbound.RiskCapitalPrimeSnapshot, error) {
	stars, err := c.trackedStars(ctx)
	if err != nil {
		return nil, err
	}

	snapshots := make([]outbound.RiskCapitalPrimeSnapshot, 0, len(stars))
	for _, star := range stars {
		snapshot, err := c.fetchPrimeDetail(ctx, star)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

func (c *Client) trackedStars(ctx context.Context) ([]string, error) {
	var payload primesResponse
	url := c.baseURL + "/primes/"
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: url}, &payload); err != nil {
		return nil, fmt.Errorf("fetching sky risk-capital primes: %w", err)
	}

	stars := make([]string, 0, len(payload.Data.Results))
	for i, row := range payload.Data.Results {
		star := strings.TrimSpace(row.Star)
		if star == "" {
			return nil, fmt.Errorf("sky risk-capital primes row %d has an empty star name", i)
		}
		stars = append(stars, star)
	}
	return stars, nil
}

func (c *Client) fetchPrimeDetail(ctx context.Context, star string) (outbound.RiskCapitalPrimeSnapshot, error) {
	var payload primeDetailResponse
	url := fmt.Sprintf("%s/primes/%s/", c.baseURL, star)
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: url}, &payload); err != nil {
		return outbound.RiskCapitalPrimeSnapshot{}, fmt.Errorf("fetching sky risk capital for prime %q: %w", star, err)
	}

	detail := payload.Data
	snapshot := outbound.RiskCapitalPrimeSnapshot{
		Star:                       star,
		Exposure:                   detail.TotalExposure.String(),
		RequiredRiskCapital:        detail.TotalRRC.String(),
		TotalRiskCapital:           detail.TotalRC.String(),
		JuniorRiskCapital:          detail.TotalJRC.String(),
		SeniorRiskCapital:          detail.TotalSRC.String(),
		InternalJuniorRiskCapital:  detail.InternalJRC.String(),
		ExternalJuniorRiskCapital:  detail.ExternalJRC.String(),
		TokenizedJuniorRiskCapital: detail.TokenizedJRC.String(),
		InternalSeniorRiskCapital:  detail.InternalSRC.String(),
		ExternalSeniorRiskCapital:  detail.ExternalSRC.String(),
		EncumbranceRatio:           optionalNumber(detail.EncumbranceRatio),
		ExposureShare:              detail.TotalExposureShare.String(),
		EPIUtilization:             detail.EPIUtilization.String(),
		SPJUtilization:             detail.SPJUtilization.String(),
	}

	if err := requireAmounts(star, snapshot); err != nil {
		return outbound.RiskCapitalPrimeSnapshot{}, err
	}
	return snapshot, nil
}

// requireAmounts rejects a snapshot missing any figure the monitor is expected
// to report. Persisting a zero in place of an absent value would look like a
// prime with no capital, so an incomplete payload fails the cycle instead.
func requireAmounts(star string, s outbound.RiskCapitalPrimeSnapshot) error {
	required := map[string]string{
		"total_exposure":       s.Exposure,
		"total_rrc":            s.RequiredRiskCapital,
		"total_rc":             s.TotalRiskCapital,
		"total_jrc":            s.JuniorRiskCapital,
		"total_src":            s.SeniorRiskCapital,
		"internal_jrc":         s.InternalJuniorRiskCapital,
		"external_jrc":         s.ExternalJuniorRiskCapital,
		"tokenized_jrc":        s.TokenizedJuniorRiskCapital,
		"internal_src":         s.InternalSeniorRiskCapital,
		"external_src":         s.ExternalSeniorRiskCapital,
		"total_exposure_share": s.ExposureShare,
		"epi_utilization":      s.EPIUtilization,
		"spj_utilization":      s.SPJUtilization,
	}
	for field, value := range required {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("sky risk capital for prime %q is missing field %q", star, field)
		}
	}
	return nil
}

func optionalNumber(value json.Number) *string {
	raw := strings.TrimSpace(value.String())
	if raw == "" {
		return nil
	}
	return &raw
}

type primesResponse struct {
	Data struct {
		Results []primeRow `json:"results"`
	} `json:"data"`
}

type primeRow struct {
	Star string `json:"star"`
}

type primeDetailResponse struct {
	Data primeDetail `json:"data"`
}

type primeDetail struct {
	TotalExposure      json.Number `json:"total_exposure"`
	TotalRRC           json.Number `json:"total_rrc"`
	TotalRC            json.Number `json:"total_rc"`
	TotalJRC           json.Number `json:"total_jrc"`
	TotalSRC           json.Number `json:"total_src"`
	InternalJRC        json.Number `json:"internal_jrc"`
	ExternalJRC        json.Number `json:"external_jrc"`
	TokenizedJRC       json.Number `json:"tokenized_jrc"`
	InternalSRC        json.Number `json:"internal_src"`
	ExternalSRC        json.Number `json:"external_src"`
	EncumbranceRatio   json.Number `json:"encumbrance_ratio"`
	TotalExposureShare json.Number `json:"total_exposure_share"`
	EPIUtilization     json.Number `json:"epi_utilization"`
	SPJUtilization     json.Number `json:"spj_utilization"`
}
