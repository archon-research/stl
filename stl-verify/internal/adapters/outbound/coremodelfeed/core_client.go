// Package coremodelfeed reads the results the upstream CORE risk model
// publishes on its dashboard's data API.
//
// One route is used per cycle: /overview/ carries every market and vault the
// model covers, with today's figures. The feed publishes one result per
// calendar day and accepts a ?date= parameter that it silently ignores
// (verified by byte-identical responses across values), so nothing is sent
// and the response is always read as "the current day".
//
// Validation is per row. A row missing a field, carrying a figure out of its
// range, or carrying a figure its method forbids is dropped and reported as a
// rejection; the other rows of the cycle still land. Only a transport fault, a
// failed envelope, a payload that does not decode (a non-numeric literal in a
// numeric field is caught by json.Number for the whole document) or a duplicate
// identity fails the whole fetch: those mean there is no trustworthy payload at
// all, and writing a duplicate would corrupt identity rather than lose one row.
package coremodelfeed

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/url"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/httpclient"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// The feed spells networks its own way — "ethereum" where the allocation
// trackers say "mainnet". Translated here so no consumer has to know the
// vendor's vocabulary; it is this vendor's, so a change to it must not move
// any other client's map. A network absent here lands with a nil chain id and
// is counted by the service, which is what asks for the map to be extended.
var networkToChainID = map[string]int64{
	"ethereum":  1,
	"optimism":  10,
	"unichain":  130,
	"robinhood": 4663,
	"base":      8453,
	"arbitrum":  42161,
	"avalanche": 43114,
}

// The vault method whose figure is a governance-set constant rather than a
// simulation result; it is the only one that legitimately lacks a standard
// error and an expected shortfall (verified live: groveUSDG), and the only one
// that must not carry them.
const overrideMethod = "override"

// Compile-time check that Client implements the provider port.
var _ outbound.CoreModelReferenceProvider = (*Client)(nil)

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

// NewClient creates a new CORE feed client. BaseURL is the feed root
// (deployment configuration, CORE_MODEL_REFERENCE_URL); there is no built-in
// default, so a missing value fails at wiring time rather than at first fetch.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if strings.TrimSpace(cfg.BaseURL) == "" {
		return nil, fmt.Errorf("core feed base URL is required")
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

	logger := cfg.Logger.With("component", "core-model-feed-client")
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

// FetchOverview returns every market and vault the dashboard reports today,
// with the rows that failed validation listed under Rejected.
func (c *Client) FetchOverview(ctx context.Context) (outbound.CoreModelReferenceOverview, error) {
	var payload overviewResponse
	requestURL := c.baseURL + "/overview/"
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: requestURL}, &payload); err != nil {
		return outbound.CoreModelReferenceOverview{}, fmt.Errorf("fetching core overview: %w", err)
	}
	if !payload.Success {
		return outbound.CoreModelReferenceOverview{}, fmt.Errorf("core overview reported success=false (status %d): %s", payload.Status, requestURL)
	}

	markets, rejectedMarkets, err := toMarketRows(payload.Data.Markets)
	if err != nil {
		return outbound.CoreModelReferenceOverview{}, err
	}
	vaults, rejectedVaults, err := toVaultRows(payload.Data.Vaults)
	if err != nil {
		return outbound.CoreModelReferenceOverview{}, err
	}
	return outbound.CoreModelReferenceOverview{
		Markets:  markets,
		Vaults:   vaults,
		Rejected: append(rejectedMarkets, rejectedVaults...),
	}, nil
}

// toMarketRows converts the payload's markets, dropping the rows that fail
// validation. Row identity is (network, protocol, market_uid) — the table's
// key — case-folded because the three are otherwise stored verbatim: a casing
// change would silently mint a second identity for one market. A duplicate in
// one fetch would conflict away at insert, so it fails the fetch instead.
func toMarketRows(rows []marketPayloadRow) ([]outbound.CoreModelReferenceMarketRow, []outbound.RowRejection, error) {
	seen := make(map[string]bool, len(rows))
	out := make([]outbound.CoreModelReferenceMarketRow, 0, len(rows))
	var rejected []outbound.RowRejection
	for i, row := range rows {
		parsed, err := toMarketRow(row)
		if err != nil {
			rejected = append(rejected, outbound.RowRejection{
				Kind:     "market",
				Identity: rowIdentity(i, row.Network, row.Protocol, row.MarketUID),
				Reason:   err.Error(),
			})
			continue
		}
		key := strings.ToLower(parsed.Network + "|" + parsed.Protocol + "|" + parsed.MarketUID)
		if seen[key] {
			return nil, nil, fmt.Errorf(
				"core overview repeats market identity %s/%s/%s; the row identity assumption no longer holds",
				parsed.Network, parsed.Protocol, parsed.MarketUID)
		}
		seen[key] = true
		out = append(out, parsed)
	}
	return out, rejected, nil
}

// toVaultRows converts the payload's vaults under the same rules as
// toMarketRows, with the identity guard on (network, protocol, vault_address).
func toVaultRows(rows []vaultPayloadRow) ([]outbound.CoreModelReferenceVaultRow, []outbound.RowRejection, error) {
	seen := make(map[string]bool, len(rows))
	out := make([]outbound.CoreModelReferenceVaultRow, 0, len(rows))
	var rejected []outbound.RowRejection
	for i, row := range rows {
		parsed, err := toVaultRow(row)
		if err != nil {
			rejected = append(rejected, outbound.RowRejection{
				Kind:     "vault",
				Identity: rowIdentity(i, row.Network, row.Protocol, row.VaultAddress),
				Reason:   err.Error(),
			})
			continue
		}
		key := strings.ToLower(parsed.Network + "|" + parsed.Protocol + "|" + parsed.VaultAddress)
		if seen[key] {
			return nil, nil, fmt.Errorf(
				"core overview repeats vault identity %s/%s/%s; the row identity assumption no longer holds",
				parsed.Network, parsed.Protocol, parsed.VaultAddress)
		}
		seen[key] = true
		out = append(out, parsed)
	}
	return out, rejected, nil
}

// rowIdentity names a rejected row by its identity fields, falling back to the
// row's position when those are what is missing.
func rowIdentity(index int, network, protocol, uid string) string {
	if strings.TrimSpace(network) == "" || strings.TrimSpace(protocol) == "" || strings.TrimSpace(uid) == "" {
		return fmt.Sprintf("row %d", index)
	}
	return strings.TrimSpace(network) + "/" + strings.TrimSpace(protocol) + "/" + strings.TrimSpace(uid)
}

// requiredField is one field a row must carry, in the order a broken payload
// is blamed: ordered, not a map, so the same fault reads as the same bug.
type requiredField struct{ field, value string }

// requireFields rejects a row missing any field the feed is expected to
// report; persisting a blank or a zero in their place would read as a real answer.
func requireFields(fields []requiredField) error {
	for _, r := range fields {
		if strings.TrimSpace(r.value) == "" {
			return fmt.Errorf("missing field %q", r.field)
		}
	}
	return nil
}

var one = big.NewRat(1, 1)

// requireNonNegative rejects a figure that does not parse or is below zero;
// requireFraction additionally rejects one above 1. Checked here so a single
// bad figure costs one row, not the whole cycle at the table's CHECK.
func requireNonNegative(field, raw string) error {
	return requireInRange(field, raw, nil)
}

func requireFraction(field, raw string) error {
	return requireInRange(field, raw, one)
}

func requireInRange(field, raw string, maximum *big.Rat) error {
	value, ok := new(big.Rat).SetString(strings.TrimSpace(raw))
	if !ok {
		return fmt.Errorf("field %q is not a number: %q", field, raw)
	}
	if value.Sign() < 0 {
		return fmt.Errorf("field %q is negative: %s", field, raw)
	}
	if maximum != nil && value.Cmp(maximum) > 0 {
		return fmt.Errorf("field %q is above 1: %s", field, raw)
	}
	return nil
}

func toMarketRow(row marketPayloadRow) (outbound.CoreModelReferenceMarketRow, error) {
	if err := requireFields([]requiredField{
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
	}); err != nil {
		return outbound.CoreModelReferenceMarketRow{}, err
	}
	for _, scalar := range []struct {
		field string
		set   bool
	}{
		{"n_scenarios", row.NScenarios != nil},
		{"horizon_days", row.HorizonDays != nil},
		{"effective_horizon_days", row.EffectiveHorizonDays != nil},
		{"external_flow_enabled", row.ExternalFlowEnabled != nil},
	} {
		if !scalar.set {
			return outbound.CoreModelReferenceMarketRow{}, fmt.Errorf("missing field %q", scalar.field)
		}
	}
	if err := requireMarketRanges(row); err != nil {
		return outbound.CoreModelReferenceMarketRow{}, err
	}

	network := strings.TrimSpace(row.Network)
	return outbound.CoreModelReferenceMarketRow{
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

// requireMarketRanges mirrors the table's CHECK constraints one row at a time.
func requireMarketRanges(row marketPayloadRow) error {
	if err := requireFraction("prob_no_bad_debt", row.ProbNoBadDebt.String()); err != nil {
		return err
	}
	for _, f := range []requiredField{
		{"tot_supply_usd", row.TotalSupply.String()},
		{"crr_el", row.CRREL.String()},
		{"crr_var", row.CRRVaR.String()},
		{"crr_es", row.CRRES.String()},
		{"crr_el_se", row.CRRELSE.String()},
		{"crr_var_se", row.CRRVaRSE.String()},
		{"crr_es_se", row.CRRESSE.String()},
		{"crr_floor", row.CRRFloor.String()},
	} {
		if err := requireNonNegative(f.field, f.value); err != nil {
			return err
		}
	}
	for _, s := range []struct {
		field string
		value int
	}{
		{"n_scenarios", *row.NScenarios},
		{"horizon_days", *row.HorizonDays},
		{"effective_horizon_days", *row.EffectiveHorizonDays},
	} {
		if s.value < 0 {
			return fmt.Errorf("field %q is negative: %d", s.field, s.value)
		}
	}
	return nil
}

// toVaultRow validates one vault. crr_el_se and crr_es are structurally absent
// on an override vault and required on every other method, and the rule holds
// both ways: an override row that carries them is a shape drift that would
// let a governance constant read as a simulation result.
func toVaultRow(row vaultPayloadRow) (outbound.CoreModelReferenceVaultRow, error) {
	if err := requireFields([]requiredField{
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
	}); err != nil {
		return outbound.CoreModelReferenceVaultRow{}, err
	}
	if row.NMarkets == nil {
		return outbound.CoreModelReferenceVaultRow{}, fmt.Errorf("missing field %q", "n_markets")
	}
	if err := requireVaultSimulationFigures(row); err != nil {
		return outbound.CoreModelReferenceVaultRow{}, err
	}
	if err := requireVaultRanges(row); err != nil {
		return outbound.CoreModelReferenceVaultRow{}, err
	}

	network := strings.TrimSpace(row.Network)
	return outbound.CoreModelReferenceVaultRow{
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

// requireVaultSimulationFigures enforces the override rule in both directions.
func requireVaultSimulationFigures(row vaultPayloadRow) error {
	se, es := optionalNumber(row.CRRELSE), optionalNumber(row.CRRES)
	if strings.TrimSpace(row.Method) == overrideMethod {
		if se != nil {
			return fmt.Errorf("override vault carries %q", "crr_el_se")
		}
		if es != nil {
			return fmt.Errorf("override vault carries %q", "crr_es")
		}
		return nil
	}
	return requireFields([]requiredField{
		{"crr_el_se", row.CRRELSE.String()},
		{"crr_es", row.CRRES.String()},
	})
}

// requireVaultRanges mirrors the table's CHECK constraints one row at a time.
func requireVaultRanges(row vaultPayloadRow) error {
	for _, f := range []requiredField{
		{"total_assets_usd", row.TotalAssets.String()},
		{"idle_usd", row.IdleAssets.String()},
		{"crr_el", row.CRREL.String()},
	} {
		if err := requireNonNegative(f.field, f.value); err != nil {
			return err
		}
	}
	for _, f := range []struct {
		field string
		value *string
	}{
		{"crr_el_se", optionalNumber(row.CRRELSE)},
		{"crr_es", optionalNumber(row.CRRES)},
	} {
		if f.value == nil {
			continue
		}
		if err := requireNonNegative(f.field, *f.value); err != nil {
			return err
		}
	}
	if *row.NMarkets < 0 {
		return fmt.Errorf("field %q is negative: %d", "n_markets", *row.NMarkets)
	}
	return nil
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

// optionalNumber folds an omitted or null numeric field to nil, keeping the
// figure as upstream's literal string otherwise.
func optionalNumber(value json.Number) *string {
	raw := strings.TrimSpace(value.String())
	if raw == "" {
		return nil
	}
	return &raw
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
