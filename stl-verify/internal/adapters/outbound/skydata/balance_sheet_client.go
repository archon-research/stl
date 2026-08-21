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
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultBaseURL = "https://sky.data.blockanalitica.com/internal"

// Compile-time check that Client implements outbound.BalanceSheetProvider.
var _ outbound.BalanceSheetProvider = (*Client)(nil)

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
