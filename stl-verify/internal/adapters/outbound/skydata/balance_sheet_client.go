// Package skydata reads Sky's per-prime daily balance-sheet history.
//
// This is a different feed from the Star monitor in internal/adapters/outbound/sky:
// it publishes a balance sheet and no risk capital, and it is the only feed with
// per-prime history. The two share one provenance downstream — the API serves
// both as reference data — but not one shape, which is why they have separate
// clients, entities and tables.
//
// The route returns every tracked prime in one response, so the caller's star
// list filters what is kept rather than what is requested.
package skydata

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/httpclient"
	"github.com/archon-research/stl/stl-verify/internal/pkg/skyenvelope"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultBaseURL = "https://sky.data.blockanalitica.com/internal"

// Upstream paginates at 20 by default — spark alone holds 59 positions, so an
// unset limit silently serves a third of them. Asked for explicitly, and the
// reported total is checked against what arrives.
const positionsPageLimit = 1000

// This host spells networks its own way — "ethereum" where the axis-synome
// contract and the allocation trackers say "mainnet". The same mapping the
// Star-monitor client applies, repeated rather than shared: they are two
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

// Compile-time checks that Client implements both feed ports.
var (
	_ outbound.BalanceSheetProvider      = (*Client)(nil)
	_ outbound.ReferencePositionProvider = (*Client)(nil)
)

// ClientConfig holds configuration for the Sky balance-sheet client.
type ClientConfig struct {
	BaseURL string
	Timeout time.Duration
	Logger  *slog.Logger
	// Now decides which day is still in progress; injected so the cutoff is testable.
	Now func() time.Time
}

// Client fetches per-prime daily balance sheets from Sky.
type Client struct {
	baseURL    string
	httpClient *httpclient.Client
	logger     *slog.Logger
	now        func() time.Time
}

// NewClient creates a new Sky balance-sheet client.
func NewClient(cfg ClientConfig) (*Client, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.BaseURL == "" {
		cfg.BaseURL = defaultBaseURL
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	trimmed := strings.TrimRight(strings.TrimSpace(cfg.BaseURL), "/")
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("parsing sky-data base URL %q: %w", cfg.BaseURL, err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return nil, fmt.Errorf("sky-data base URL %q must be an absolute http(s) URL", cfg.BaseURL)
	}

	httpCfg := httpclient.DefaultConfig()
	// A year of history for every prime is a large single response, so the read
	// budget is well above the per-request default.
	httpCfg.Timeout = 120 * time.Second
	if cfg.Timeout > 0 {
		httpCfg.Timeout = cfg.Timeout
	}

	logger := cfg.Logger.With("component", "sky-balance-sheet-client")
	return &Client{
		baseURL:    trimmed,
		httpClient: httpclient.NewClient(httpCfg, logger, nil),
		logger:     logger,
		now:        cfg.Now,
	}, nil
}

// FetchHistory returns every day the feed holds within daysAgo for each star.
func (c *Client) FetchHistory(ctx context.Context, stars []string, daysAgo int) ([]outbound.BalanceSheetDay, error) {
	if daysAgo <= 0 {
		return nil, fmt.Errorf("daysAgo must be positive, got %d", daysAgo)
	}

	wanted := make(map[string]bool, len(stars))
	for _, star := range stars {
		wanted[strings.ToLower(strings.TrimSpace(star))] = true
	}

	var payload historicResponse
	requestURL := fmt.Sprintf("%s/primes/historic/?days_ago=%d", c.baseURL, daysAgo)
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: requestURL}, &payload); err != nil {
		return nil, fmt.Errorf("fetching sky balance-sheet history: %w", err)
	}
	if len(payload.Data) == 0 {
		// The feed holds a year for every prime, so an empty body is a broken
		// feed rather than an absence of history.
		return nil, fmt.Errorf("sky balance-sheet history returned no rows for %d days", daysAgo)
	}

	// The feed's row for the current UTC day is provisional — it moves as the day
	// runs (verified live: today's assets differ from yesterday's close). Dropped
	// here rather than persisted, because the store cannot revise it: the
	// processing-version trigger is build-aware, so a second write for the same
	// day under the same deployment is discarded, freezing whatever the first
	// cycle happened to see. A day is recorded once it can no longer change.
	today := c.now().UTC().Format(time.DateOnly)

	days := make([]outbound.BalanceSheetDay, 0, len(payload.Data))
	for i, row := range payload.Data {
		if !wanted[strings.ToLower(strings.TrimSpace(row.Star))] {
			continue
		}
		if strings.TrimSpace(row.Date) == today {
			continue
		}
		day, err := toDay(row, i)
		if err != nil {
			return nil, err
		}
		days = append(days, day)
	}

	if len(days) == 0 {
		return nil, fmt.Errorf("sky balance-sheet history covered none of the %d requested primes", len(stars))
	}
	return days, nil
}

// toDay rejects a row missing any figure, rather than defaulting it to zero and
// publishing a prime with no treasury.
//
// Every figure is populated on every day of the year for the primes STL tracks
// (verified live), but not for every prime the feed carries — obex has a null
// allocated_assets on 2025-11-17. So this fails a backfill that newly includes
// such a prime, which is the intended signal: the gap needs a decision, not a
// silent zero.
func toDay(row historicRow, index int) (outbound.BalanceSheetDay, error) {
	date := strings.TrimSpace(row.Date)
	if date == "" {
		return outbound.BalanceSheetDay{}, fmt.Errorf("sky balance-sheet row %d has no date", index)
	}

	day := outbound.BalanceSheetDay{
		Star:            strings.ToLower(strings.TrimSpace(row.Star)),
		Date:            date,
		TreasuryBalance: row.TreasuryBalance.String(),
		Assets:          row.Assets.String(),
		AllocatedAssets: row.AllocatedAssets.String(),
		IdleAssets:      row.IdleAssets.String(),
		Debt:            row.Debt.String(),
		BackstopCapital: row.BackstopCapital.String(),
	}

	// Ordered, not a map: the field a broken payload is blamed on must be
	// reproducible across runs.
	required := []struct{ field, value string }{
		{"treasury_balance", day.TreasuryBalance},
		{"assets", day.Assets},
		{"allocated_assets", day.AllocatedAssets},
		{"idle_assets", day.IdleAssets},
		{"debt", day.Debt},
		{"backstop_capital", day.BackstopCapital},
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			return outbound.BalanceSheetDay{}, fmt.Errorf(
				"sky balance-sheet row for prime %q on %s is missing field %q", day.Star, date, r.field)
		}
	}
	return day, nil
}

// FetchPositions returns every balance-sheet position the feed holds for each
// star. Callers pass only stars whose coverage is already established: the
// route answers an unknown star with 200 and an empty list, so an empty result
// cannot be told apart from a prime that genuinely holds nothing — a passed
// star returning zero rows therefore fails the fetch.
func (c *Client) FetchPositions(ctx context.Context, stars []string) ([]outbound.ReferencePositionRow, error) {
	rows := make([]outbound.ReferencePositionRow, 0, len(stars)*64)
	for _, star := range stars {
		starRows, err := c.fetchStarPositions(ctx, star)
		if err != nil {
			return nil, err
		}
		rows = append(rows, starRows...)
	}
	return rows, nil
}

func (c *Client) fetchStarPositions(ctx context.Context, star string) ([]outbound.ReferencePositionRow, error) {
	var payload positionsResponse
	requestURL := fmt.Sprintf("%s/allocations/?prime=%s&limit=%d", c.baseURL, url.QueryEscape(star), positionsPageLimit)
	if err := c.httpClient.DoRequest(ctx, httpclient.RequestConfig{URL: requestURL}, &payload); err != nil {
		return nil, fmt.Errorf("fetching sky positions for prime %q: %w", star, err)
	}

	results := payload.Data.Results
	if len(results) == 0 {
		return nil, fmt.Errorf(
			"sky positions feed returned no rows for prime %q; an untracked star and an empty holder are indistinguishable, so this fails rather than recording an empty balance sheet", star)
	}
	if err := skyenvelope.RequireFullPage(payload.Data.Pagination, len(results), positionsPageLimit, requestURL); err != nil {
		return nil, err
	}

	// Row identity is (network, token_address, wallet_address) — the table's
	// key. Case-folded here because the three are otherwise stored verbatim: a
	// casing change on any would silently mint a second identity for the same
	// position. A duplicate in one fetch would silently conflict away at
	// insert, so it fails here instead. wallet_address is real identity, not
	// incidental data: grove legitimately carries the same (network,
	// token_address) under two proxy wallets, with materially different
	// balances on the same Uni V3 LP position (verified live).
	seen := make(map[string]bool, len(results))
	rows := make([]outbound.ReferencePositionRow, 0, len(results))
	for i, row := range results {
		parsed, err := toPositionRow(star, row, i)
		if err != nil {
			return nil, err
		}
		key := strings.ToLower(parsed.Network) + "|" + strings.ToLower(parsed.TokenAddress) + "|" + strings.ToLower(parsed.WalletAddress)
		if seen[key] {
			return nil, fmt.Errorf(
				"sky positions for prime %q repeat identity %s on %s for wallet %s; the row identity assumption no longer holds",
				star, parsed.TokenAddress, parsed.Network, parsed.WalletAddress)
		}
		seen[key] = true
		rows = append(rows, parsed)
	}
	return rows, nil
}

// toPositionRow rejects a row missing any identifying field or its balance;
// persisting a blank in their place would read as a real answer. token_name,
// allocated_assets and idle_assets are the fields the feed may omit.
func toPositionRow(star string, row positionPayloadRow, index int) (outbound.ReferencePositionRow, error) {
	// Ordered, not a map: which field a broken payload is blamed on must be
	// reproducible across runs, or the same fault reads as a different bug.
	required := []struct{ field, value string }{
		{"protocol", row.Protocol},
		{"network", row.Network},
		{"token_symbol", row.TokenSymbol},
		{"address", row.Address},
		{"assets", row.Assets.String()},
		{"wallet_address", row.WalletAddress},
	}
	for _, r := range required {
		if strings.TrimSpace(r.value) == "" {
			return outbound.ReferencePositionRow{}, fmt.Errorf(
				"sky position row %d for prime %q is missing field %q", index, star, r.field)
		}
	}

	network := strings.TrimSpace(row.Network)
	return outbound.ReferencePositionRow{
		Star:            star,
		Protocol:        strings.TrimSpace(row.Protocol),
		Network:         network,
		ChainID:         chainIDFor(network),
		TokenSymbol:     strings.TrimSpace(row.TokenSymbol),
		TokenName:       skyenvelope.OptionalText(row.TokenName),
		TokenAddress:    strings.TrimSpace(row.Address),
		WalletAddress:   strings.TrimSpace(row.WalletAddress),
		Assets:          row.Assets.String(),
		AllocatedAssets: skyenvelope.OptionalNumber(row.AllocatedAssets),
		IdleAssets:      skyenvelope.OptionalNumber(row.IdleAssets),
	}, nil
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

type historicResponse struct {
	Data []historicRow `json:"data"`
}

type historicRow struct {
	Star            string      `json:"star"`
	Date            string      `json:"date"`
	TreasuryBalance json.Number `json:"treasury_balance"`
	Assets          json.Number `json:"assets"`
	AllocatedAssets json.Number `json:"allocated_assets"`
	IdleAssets      json.Number `json:"idle_assets"`
	Debt            json.Number `json:"debt"`
	BackstopCapital json.Number `json:"backstop_capital"`
}

type positionsResponse struct {
	Data struct {
		Results    []positionPayloadRow    `json:"results"`
		Pagination *skyenvelope.Pagination `json:"pagination"`
	} `json:"data"`
}

type positionPayloadRow struct {
	Protocol        string      `json:"protocol"`
	Network         string      `json:"network"`
	TokenSymbol     string      `json:"token_symbol"`
	TokenName       string      `json:"token_name"`
	Address         string      `json:"address"`
	WalletAddress   string      `json:"wallet_address"`
	Assets          json.Number `json:"assets"`
	AllocatedAssets json.Number `json:"allocated_assets"`
	IdleAssets      json.Number `json:"idle_assets"`
}
