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
	"net/url"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/httpclient"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultBaseURL = "https://info-sky.blockanalitica.com/star-monitoring/risk-capital"

// Upstream paginates at 20 by default. Asked for explicitly, and the reported
// total is checked against what arrives, so a set outgrowing the page fails
// rather than silently losing rows.
const allocationsPageLimit = 500

// The monitor spells networks its own way — "ethereum" where the axis-synome
// contract and the allocation trackers say "mainnet". Translated here with the
// other upstream encodings so no consumer has to know the vendor's vocabulary.
// The skydata client repeats this map rather than sharing it: they are two
// vendors' vocabularies that happen to agree today, and a change to one must
// not silently move the other.
var networkToChainID = map[string]int64{
	"ethereum":  1,
	"optimism":  10,
	"unichain":  130,
	"base":      8453,
	"arbitrum":  42161,
	"avalanche": 43114,
}

// Compile-time checks that Client implements both monitor ports.
var (
	_ outbound.RiskCapitalProvider           = (*Client)(nil)
	_ outbound.RiskCapitalAllocationProvider = (*Client)(nil)
)

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
	baseURL, err := validateBaseURL(cfg.BaseURL)
	if err != nil {
		return nil, err
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
		baseURL:    baseURL,
		httpClient: httpclient.NewClient(httpCfg, logger, nil),
		logger:     logger,
	}, nil
}

// validateBaseURL returns the risk-capital root, rejecting the shapes that fail
// silently. A URL already ending in /primes is the likely mistake — the routes
// append it, so it would request /primes/primes/, which upstream answers with a
// 500 that reads as an outage rather than as misconfiguration.
func validateBaseURL(raw string) (string, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parsing sky base URL %q: %w", raw, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("sky base URL %q must be an absolute http(s) URL", raw)
	}
	if strings.HasSuffix(parsed.Path, "/primes") {
		return "", fmt.Errorf("sky base URL %q must be the risk-capital root, without the /primes route", raw)
	}
	return trimmed, nil
}

// FetchPrimeSnapshots returns a snapshot for each of `stars` the monitor covers.
func (c *Client) FetchPrimeSnapshots(
	ctx context.Context,
	stars []string,
) ([]outbound.RiskCapitalPrimeSnapshot, error) {
	covered, err := c.coveredStars(ctx)
	if err != nil {
		return nil, err
	}

	snapshots := make([]outbound.RiskCapitalPrimeSnapshot, 0, len(stars))
	for _, star := range stars {
		if !covered[normalizeStar(star)] {
			// The monitor covers a subset of the primes STL tracks. Absence is a
			// fact about its coverage, so the cycle records the rest rather than
			// failing; a zero here would read as a prime holding nothing.
			c.logger.Info("prime is not covered by the Star monitor; no reference snapshot", "star", star)
			continue
		}
		snapshot, err := c.fetchPrimeDetail(ctx, star)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}

// coveredStars lists the primes the monitor reports, so a star it does not know
// is never asked for directly: the detail route answers an unknown star with a
// 500 indistinguishable from a genuine fault.
func (c *Client) coveredStars(ctx context.Context) (map[string]bool, error) {
	var payload primesResponse
	requestURL := c.baseURL + "/primes/"
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: requestURL}, &payload); err != nil {
		return nil, fmt.Errorf("fetching sky risk-capital primes: %w", err)
	}

	covered := make(map[string]bool, len(payload.Data.Results))
	for i, row := range payload.Data.Results {
		star := normalizeStar(row.Star)
		if star == "" {
			return nil, fmt.Errorf("sky risk-capital primes row %d has an empty star name", i)
		}
		covered[star] = true
	}
	return covered, nil
}

// normalizeStar folds a star name for comparison. Both sides of the coverage
// lookup go through it, so a difference in case or padding between the contract
// and the monitor cannot silently mark a tracked prime uncovered. Matches the
// balance-sheet client and the Python reference client.
func normalizeStar(star string) string {
	return strings.ToLower(strings.TrimSpace(star))
}

func (c *Client) fetchPrimeDetail(ctx context.Context, star string) (outbound.RiskCapitalPrimeSnapshot, error) {
	var payload primeDetailResponse
	requestURL := fmt.Sprintf("%s/primes/%s/", c.baseURL, url.PathEscape(star))
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: requestURL}, &payload); err != nil {
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
	// Ordered, not a map: which field a broken payload is blamed on must be
	// reproducible across runs, or the same fault reads as a different bug.
	required := []struct{ field, value string }{
		{"total_exposure", s.Exposure},
		{"total_rrc", s.RequiredRiskCapital},
		{"total_rc", s.TotalRiskCapital},
		{"total_jrc", s.JuniorRiskCapital},
		{"total_src", s.SeniorRiskCapital},
		{"internal_jrc", s.InternalJuniorRiskCapital},
		{"external_jrc", s.ExternalJuniorRiskCapital},
		{"tokenized_jrc", s.TokenizedJuniorRiskCapital},
		{"internal_src", s.InternalSeniorRiskCapital},
		{"external_src", s.ExternalSeniorRiskCapital},
		{"total_exposure_share", s.ExposureShare},
		{"epi_utilization", s.EPIUtilization},
		{"spj_utilization", s.SPJUtilization},
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			return fmt.Errorf("sky risk capital for prime %q is missing field %q", star, r.field)
		}
	}
	return nil
}

// FetchPrimeAllocations returns the per-allocation breakdown for each star.
// Callers pass only stars the monitor covers (from the same cycle's
// snapshots): the route answers an unknown star with a 500 indistinguishable
// from a fault.
func (c *Client) FetchPrimeAllocations(
	ctx context.Context,
	stars []string,
) ([]outbound.RiskCapitalAllocationRow, error) {
	rows := make([]outbound.RiskCapitalAllocationRow, 0, len(stars)*16)
	for _, star := range stars {
		starRows, err := c.fetchStarAllocations(ctx, star)
		if err != nil {
			return nil, err
		}
		rows = append(rows, starRows...)
	}
	return rows, nil
}

func (c *Client) fetchStarAllocations(ctx context.Context, star string) ([]outbound.RiskCapitalAllocationRow, error) {
	var payload allocationsResponse
	requestURL := fmt.Sprintf("%s/primes/%s/allocations/?limit=%d", c.baseURL, url.PathEscape(star), allocationsPageLimit)
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: requestURL}, &payload); err != nil {
		return nil, fmt.Errorf("fetching sky risk-capital allocations for prime %q: %w", star, err)
	}

	results := payload.Data.Results
	if err := requireFullPage(payload.Data.Pagination, len(results), allocationsPageLimit, requestURL); err != nil {
		return nil, err
	}

	// Row identity is (network, token_address) — the table's key. A duplicate
	// in one fetch would silently conflict away at insert, so it fails here.
	seen := make(map[string]bool, len(results))
	rows := make([]outbound.RiskCapitalAllocationRow, 0, len(results))
	for i, row := range results {
		parsed, err := toAllocationRow(star, row, i)
		if err != nil {
			return nil, err
		}
		key := parsed.Network + "|" + parsed.TokenAddress
		if seen[key] {
			return nil, fmt.Errorf(
				"sky risk-capital allocations for prime %q repeat identity %s on %s; the row identity assumption no longer holds",
				star, parsed.TokenAddress, parsed.Network)
		}
		seen[key] = true
		rows = append(rows, parsed)
	}
	return rows, nil
}

// toAllocationRow rejects a row missing any identifying or numeric field the
// monitor is expected to report; persisting a blank in their place would read
// as a real answer. name and the loan-token pair are the fields the monitor
// may omit.
func toAllocationRow(star string, row allocationPayloadRow, index int) (outbound.RiskCapitalAllocationRow, error) {
	// Ordered, not a map: which field a broken payload is blamed on must be
	// reproducible across runs, or the same fault reads as a different bug.
	required := []struct{ field, value string }{
		{"protocol", row.Protocol},
		{"network", row.Network},
		{"symbol", row.Symbol},
		{"token_address", row.TokenAddress},
		{"exposure", row.Exposure.String()},
		{"rrc", row.RRC.String()},
		{"crr", row.CRR.String()},
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			return outbound.RiskCapitalAllocationRow{}, fmt.Errorf(
				"sky risk-capital allocation row %d for prime %q is missing field %q", index, star, r.field)
		}
	}

	network := strings.TrimSpace(row.Network)
	return outbound.RiskCapitalAllocationRow{
		Star:                star,
		Protocol:            strings.TrimSpace(row.Protocol),
		Network:             network,
		ChainID:             chainIDFor(network),
		Symbol:              strings.TrimSpace(row.Symbol),
		Name:                optionalText(row.Name),
		TokenAddress:        strings.TrimSpace(row.TokenAddress),
		LoanTokenAddress:    optionalText(row.LoanTokenAddress),
		LoanTokenSymbol:     optionalText(row.LoanTokenSymbol),
		Exposure:            row.Exposure.String(),
		RequiredRiskCapital: row.RRC.String(),
		CRR:                 row.CRR.String(),
	}, nil
}

func chainIDFor(network string) *int64 {
	id, ok := networkToChainID[network]
	if !ok {
		return nil
	}
	return &id
}

// requireFullPage rejects a page that may be truncated, which would read as
// rows that do not exist. With a usable total, a short page means the set
// outgrew the limit; without one, a page at the limit cannot be told from a
// cut-off one, so it is refused rather than served as a silent partial set.
func requireFullPage(p *pagination, received, limit int, requestURL string) error {
	if p != nil && p.Total != nil {
		if *p.Total > received {
			return fmt.Errorf(
				"sky reported %d rows but returned %d; the page limit is too low: %s", *p.Total, received, requestURL)
		}
		return nil
	}
	if received >= limit {
		return fmt.Errorf(
			"sky returned a full page of %d rows with no usable total; the set may be truncated: %s", received, requestURL)
	}
	return nil
}

func optionalText(value string) *string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
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

type allocationsResponse struct {
	Data struct {
		Results    []allocationPayloadRow `json:"results"`
		Pagination *pagination            `json:"pagination"`
	} `json:"data"`
}

type pagination struct {
	Total *int `json:"total"`
}

type allocationPayloadRow struct {
	Protocol         string      `json:"protocol"`
	Network          string      `json:"network"`
	Symbol           string      `json:"symbol"`
	Name             string      `json:"name"`
	TokenAddress     string      `json:"token_address"`
	LoanTokenAddress string      `json:"loan_token_address"`
	LoanTokenSymbol  string      `json:"loan_token_symbol"`
	Exposure         json.Number `json:"exposure"`
	RRC              json.Number `json:"rrc"`
	CRR              json.Number `json:"crr"`
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
