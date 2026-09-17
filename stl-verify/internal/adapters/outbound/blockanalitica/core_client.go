// Package blockanalitica reads the CORE risk model results Block Analitica
// publishes behind core.blockanalitica.com.
//
// One route is used per cycle: /overview/ carries every market and vault the
// model covers, with today's figures. The feed publishes one result per
// calendar day and accepts a ?date= parameter that it silently ignores
// (verified by byte-identical responses across values), so nothing is sent
// and the response is always read as "the current day".
package blockanalitica

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

const defaultBaseURL = "https://core.data.blockanalitica.com/core"

// The feed spells networks its own way — "ethereum" where the allocation
// trackers say "mainnet". Translated here so no consumer has to know the
// vendor's vocabulary; it is this vendor's, so a change to it must not move
// any other client's map.
var networkToChainID = map[string]int64{
	"ethereum":  1,
	"optimism":  10,
	"unichain":  130,
	"robinhood": 4663,
	"base":      8453,
	"arbitrum":  42161,
	"avalanche": 43114,
}

// Compile-time check that Client implements the provider port.
var _ outbound.ReferenceCoreProvider = (*Client)(nil)

// ClientConfig holds configuration for the CORE feed client.
type ClientConfig struct {
	BaseURL        string
	Timeout        time.Duration
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	Logger         *slog.Logger
}

// Client fetches CORE model results from the upstream dashboard feed.
type Client struct {
	baseURL    string
	httpClient *httpclient.Client
	logger     *slog.Logger
}

// NewClient creates a new CORE feed client.
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

	logger := cfg.Logger.With("component", "blockanalitica-core-client")
	return &Client{
		baseURL:    baseURL,
		httpClient: httpclient.NewClient(httpCfg, logger, nil),
		logger:     logger,
	}, nil
}

// validateBaseURL returns the feed root, rejecting the shapes that fail
// silently. A URL already ending in /overview is the likely mistake — the
// route appends it, so it would request /overview/overview/, which upstream
// answers with a 404 that reads as an outage rather than as misconfiguration.
func validateBaseURL(raw string) (string, error) {
	trimmed := strings.TrimRight(strings.TrimSpace(raw), "/")

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parsing core feed base URL %q: %w", raw, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", fmt.Errorf("core feed base URL %q must be an absolute http(s) URL", raw)
	}
	if strings.HasSuffix(parsed.Path, "/overview") {
		return "", fmt.Errorf("core feed base URL %q must be the /core root, without the /overview route", raw)
	}
	return trimmed, nil
}

// FetchOverview returns every market and vault the dashboard reports today.
func (c *Client) FetchOverview(ctx context.Context) (outbound.ReferenceCoreOverview, error) {
	var payload overviewResponse
	requestURL := c.baseURL + "/overview/"
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: requestURL}, &payload); err != nil {
		return outbound.ReferenceCoreOverview{}, fmt.Errorf("fetching core overview: %w", err)
	}
	if !payload.Success {
		return outbound.ReferenceCoreOverview{}, fmt.Errorf("core overview reported success=false (status %d): %s", payload.Status, requestURL)
	}

	markets, err := toMarketRows(payload.Data.Markets)
	if err != nil {
		return outbound.ReferenceCoreOverview{}, err
	}
	vaults, err := toVaultRows(payload.Data.Vaults)
	if err != nil {
		return outbound.ReferenceCoreOverview{}, err
	}
	return outbound.ReferenceCoreOverview{Markets: markets, Vaults: vaults}, nil
}

// toMarketRows converts and validates the payload's markets. Row identity is
// (network, protocol, market_uid) — the table's key — case-folded because the
// three are otherwise stored verbatim: a casing change would silently mint a
// second identity for one market. A duplicate in one fetch would conflict away
// at insert, so it fails here instead.
func toMarketRows(rows []marketPayloadRow) ([]outbound.ReferenceCoreMarketRow, error) {
	seen := make(map[string]bool, len(rows))
	out := make([]outbound.ReferenceCoreMarketRow, 0, len(rows))
	for i, row := range rows {
		parsed, err := toMarketRow(row, i)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(parsed.Network + "|" + parsed.Protocol + "|" + parsed.MarketUID)
		if seen[key] {
			return nil, fmt.Errorf(
				"core overview repeats market identity %s/%s/%s; the row identity assumption no longer holds",
				parsed.Network, parsed.Protocol, parsed.MarketUID)
		}
		seen[key] = true
		out = append(out, parsed)
	}
	return out, nil
}

// toMarketRow rejects a row missing any field the feed is expected to report;
// persisting a blank or a zero in their place would read as a real answer.
func toMarketRow(row marketPayloadRow, index int) (outbound.ReferenceCoreMarketRow, error) {
	// Ordered, not a map: which field a broken payload is blamed on must be
	// reproducible across runs, or the same fault reads as a different bug.
	required := []struct{ field, value string }{
		{"network", row.Network},
		{"protocol", row.Protocol},
		{"market_uid", row.MarketUID},
		{"market_symbol", row.MarketSymbol},
		{"loan_token_symbol", row.LoanTokenSymbol},
		{"loan_token_address", row.LoanTokenAddress},
		{"date", row.Date},
		{"tot_supply_usd", row.TotalSupply.String()},
		{"prob_no_bad_debt", row.ProbNoBadDebt.String()},
		{"crr_el", row.CRREL.String()},
		{"crr_var", row.CRRVaR.String()},
		{"crr_es", row.CRRES.String()},
		{"crr_el_se", row.CRRELSE.String()},
		{"crr_var_se", row.CRRVaRSE.String()},
		{"crr_es_se", row.CRRESSE.String()},
		{"crr_floor", row.CRRFloor.String()},
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			return outbound.ReferenceCoreMarketRow{}, fmt.Errorf(
				"core overview market row %d is missing field %q", index, r.field)
		}
	}
	// Ordered for the same reason as above.
	requiredScalars := []struct {
		field string
		set   bool
	}{
		{"n_scenarios", row.NScenarios != nil},
		{"horizon_days", row.HorizonDays != nil},
		{"effective_horizon_days", row.EffectiveHorizonDays != nil},
		{"external_flow_enabled", row.ExternalFlowEnabled != nil},
	}
	for _, r := range requiredScalars {
		if !r.set {
			return outbound.ReferenceCoreMarketRow{}, fmt.Errorf(
				"core overview market row %d is missing field %q", index, r.field)
		}
	}

	network := strings.TrimSpace(row.Network)
	return outbound.ReferenceCoreMarketRow{
		Network:              network,
		ChainID:              chainIDFor(network),
		Protocol:             strings.TrimSpace(row.Protocol),
		MarketUID:            strings.TrimSpace(row.MarketUID),
		MarketSymbol:         strings.TrimSpace(row.MarketSymbol),
		LoanTokenSymbol:      strings.TrimSpace(row.LoanTokenSymbol),
		LoanTokenAddress:     strings.TrimSpace(row.LoanTokenAddress),
		Date:                 strings.TrimSpace(row.Date),
		NScenarios:           *row.NScenarios,
		HorizonDays:          *row.HorizonDays,
		EffectiveHorizonDays: *row.EffectiveHorizonDays,
		TotalSupply:          row.TotalSupply.String(),
		ProbNoBadDebt:        row.ProbNoBadDebt.String(),
		CRREL:                row.CRREL.String(),
		CRRVaR:               row.CRRVaR.String(),
		CRRES:                row.CRRES.String(),
		CRRELSE:              row.CRRELSE.String(),
		CRRVaRSE:             row.CRRVaRSE.String(),
		CRRESSE:              row.CRRESSE.String(),
		CRRFloor:             row.CRRFloor.String(),
		ExternalFlowEnabled:  *row.ExternalFlowEnabled,
	}, nil
}

// toVaultRows converts and validates the payload's vaults, with the same
// duplicate-identity guard as toMarketRows on (network, protocol, vault_address).
func toVaultRows(rows []vaultPayloadRow) ([]outbound.ReferenceCoreVaultRow, error) {
	seen := make(map[string]bool, len(rows))
	out := make([]outbound.ReferenceCoreVaultRow, 0, len(rows))
	for i, row := range rows {
		parsed, err := toVaultRow(row, i)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(parsed.Network + "|" + parsed.Protocol + "|" + parsed.VaultAddress)
		if seen[key] {
			return nil, fmt.Errorf(
				"core overview repeats vault identity %s/%s/%s; the row identity assumption no longer holds",
				parsed.Network, parsed.Protocol, parsed.VaultAddress)
		}
		seen[key] = true
		out = append(out, parsed)
	}
	return out, nil
}

// The vault method whose figure is a governance-set constant rather than a
// simulation result; it is the only one that legitimately lacks a standard
// error and an expected shortfall (verified live: groveUSDG).
const overrideMethod = "override"

// toVaultRow rejects a row missing any field the feed is expected to report.
// crr_el_se and crr_es are structurally absent on an override vault and
// required on every other method, so their absence is gated on method rather
// than folded to NULL across the board.
func toVaultRow(row vaultPayloadRow, index int) (outbound.ReferenceCoreVaultRow, error) {
	required := []struct{ field, value string }{
		{"network", row.Network},
		{"protocol", row.Protocol},
		{"vault_address", row.VaultAddress},
		{"vault_symbol", row.VaultSymbol},
		{"vault_name", row.VaultName},
		{"version", row.Version},
		{"loan_token_symbol", row.LoanTokenSymbol},
		{"loan_token_address", row.LoanTokenAddress},
		{"method", row.Method},
		{"date", row.Date},
		{"total_assets_usd", row.TotalAssets.String()},
		{"idle_usd", row.IdleAssets.String()},
		{"crr_el", row.CRREL.String()},
	}
	if strings.TrimSpace(row.Method) != overrideMethod {
		required = append(required,
			struct{ field, value string }{"crr_el_se", row.CRRELSE.String()},
			struct{ field, value string }{"crr_es", row.CRRES.String()},
		)
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			return outbound.ReferenceCoreVaultRow{}, fmt.Errorf(
				"core overview vault row %d is missing field %q", index, r.field)
		}
	}
	if row.NMarkets == nil {
		return outbound.ReferenceCoreVaultRow{}, fmt.Errorf(
			"core overview vault row %d is missing field %q", index, "n_markets")
	}

	network := strings.TrimSpace(row.Network)
	return outbound.ReferenceCoreVaultRow{
		Network:          network,
		ChainID:          chainIDFor(network),
		Protocol:         strings.TrimSpace(row.Protocol),
		VaultAddress:     strings.TrimSpace(row.VaultAddress),
		VaultSymbol:      strings.TrimSpace(row.VaultSymbol),
		VaultName:        strings.TrimSpace(row.VaultName),
		Version:          strings.TrimSpace(row.Version),
		LoanTokenSymbol:  strings.TrimSpace(row.LoanTokenSymbol),
		LoanTokenAddress: strings.TrimSpace(row.LoanTokenAddress),
		Method:           strings.TrimSpace(row.Method),
		Date:             strings.TrimSpace(row.Date),
		NMarkets:         *row.NMarkets,
		TotalAssets:      row.TotalAssets.String(),
		IdleAssets:       row.IdleAssets.String(),
		CRREL:            row.CRREL.String(),
		CRRELSE:          optionalNumber(row.CRRELSE),
		CRRES:            optionalNumber(row.CRRES),
	}, nil
}

// optionalNumber folds an omitted or null numeric field to nil, keeping the
// figure as upstream's literal string otherwise.
func optionalNumber(value json.Number) *string {
	raw := strings.TrimSpace(value.String())
	if raw == "" {
		return nil
	}
	return &raw
}

// chainIDFor looks up by a case-folded network, since the vendor vocabulary
// this map encodes is lowercase and upstream's own casing is not trustworthy.
func chainIDFor(network string) *int64 {
	id, ok := networkToChainID[strings.ToLower(network)]
	if !ok {
		return nil
	}
	return &id
}

type overviewResponse struct {
	Data struct {
		Markets []marketPayloadRow `json:"markets"`
		Vaults  []vaultPayloadRow  `json:"vaults"`
	} `json:"data"`
	Status  int  `json:"status"`
	Success bool `json:"success"`
}

// Numeric figures arrive as JSON strings carrying 18 decimals; json.Number
// accepts a quoted literal and keeps it unrounded.
type marketPayloadRow struct {
	Network              string      `json:"network"`
	Protocol             string      `json:"protocol"`
	MarketUID            string      `json:"market_uid"`
	MarketSymbol         string      `json:"market_symbol"`
	LoanTokenSymbol      string      `json:"loan_token_symbol"`
	LoanTokenAddress     string      `json:"loan_token_address"`
	Date                 string      `json:"date"`
	NScenarios           *int        `json:"n_scenarios"`
	HorizonDays          *int        `json:"horizon_days"`
	EffectiveHorizonDays *int        `json:"effective_horizon_days"`
	TotalSupply          json.Number `json:"tot_supply_usd"`
	ProbNoBadDebt        json.Number `json:"prob_no_bad_debt"`
	CRREL                json.Number `json:"crr_el"`
	CRRVaR               json.Number `json:"crr_var"`
	CRRES                json.Number `json:"crr_es"`
	CRRELSE              json.Number `json:"crr_el_se"`
	CRRVaRSE             json.Number `json:"crr_var_se"`
	CRRESSE              json.Number `json:"crr_es_se"`
	CRRFloor             json.Number `json:"crr_floor"`
	ExternalFlowEnabled  *bool       `json:"external_flow_enabled"`
}

type vaultPayloadRow struct {
	Network          string      `json:"network"`
	Protocol         string      `json:"protocol"`
	VaultAddress     string      `json:"vault_address"`
	VaultSymbol      string      `json:"vault_symbol"`
	VaultName        string      `json:"vault_name"`
	Version          string      `json:"version"`
	LoanTokenSymbol  string      `json:"loan_token_symbol"`
	LoanTokenAddress string      `json:"loan_token_address"`
	Method           string      `json:"method"`
	Date             string      `json:"date"`
	NMarkets         *int        `json:"n_markets"`
	TotalAssets      json.Number `json:"total_assets_usd"`
	IdleAssets       json.Number `json:"idle_usd"`
	CRREL            json.Number `json:"crr_el"`
	CRRELSE          json.Number `json:"crr_el_se"`
	CRRES            json.Number `json:"crr_es"`
}
