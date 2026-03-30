package curve

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	defaultBaseURL = "https://api.curve.finance/v1"
	defaultTimeout = 15 * time.Second
)

// Client interacts with the Curve Finance REST API.
// Implements outbound.CurveAPYFetcher.
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *slog.Logger
}

var _ outbound.CurveAPYFetcher = (*Client)(nil)

func NewClient(logger *slog.Logger) *Client {
	return &Client{
		baseURL: defaultBaseURL,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
		logger: logger.With("component", "curve-api"),
	}
}

// SetBaseURL overrides the default API base URL (used for testing).
func (c *Client) SetBaseURL(url string) {
	c.baseURL = url
}

// baseApyResponse is the API response structure for getBaseApys.
type baseApyResponse struct {
	Success bool `json:"success"`
	Data    struct {
		BaseApys []struct {
			Address         string   `json:"address"`
			LatestDailyAPY  *float64 `json:"latestDailyApyPcent"`
			LatestWeeklyAPY *float64 `json:"latestWeeklyApyPcent"`
		} `json:"baseApys"`
	} `json:"data"`
}

// gaugeEntry represents a single gauge in the getAllGauges response.
type gaugeEntry struct {
	BlockchainID string    `json:"blockchainId"`
	GaugeCrvAPY  []float64 `json:"gaugeCrvApy"` // [minApy, maxApy]
	IsKilled     bool      `json:"is_killed"`
	HasNoCrv     bool      `json:"hasNoCrv"`
	Swap         string    `json:"swap"`
}

// FetchAPYs returns APY data for the given pool addresses.
// Merges fee APY from getBaseApys with CRV APY from getAllGauges.
// Returns partial results alongside any errors — the caller decides
// whether to proceed with incomplete data.
func (c *Client) FetchAPYs(ctx context.Context, chainName string, pools []common.Address) (map[common.Address]*outbound.APYData, error) {
	results := make(map[common.Address]*outbound.APYData, len(pools))

	poolSet := make(map[string]common.Address, len(pools))
	for _, p := range pools {
		poolSet[strings.ToLower(p.Hex())] = p
	}

	feeAPYs, err := c.fetchBaseAPYs(ctx, chainName)
	if err != nil {
		return nil, fmt.Errorf("base APYs: %w", err)
	}
	for addrHex, apy := range feeAPYs {
		if pool, ok := poolSet[addrHex]; ok {
			results[pool] = &outbound.APYData{
				FeeAPYDaily:  apy.daily,
				FeeAPYWeekly: apy.weekly,
			}
		}
	}

	crvAPYs, err := c.fetchGaugeAPYs(ctx, chainName)
	if err != nil {
		return results, fmt.Errorf("gauge APYs: %w", err)
	}
	for addrHex, crv := range crvAPYs {
		if pool, ok := poolSet[addrHex]; ok {
			minVal, maxVal := crv[0], crv[1]
			if existing, ok := results[pool]; ok {
				existing.CrvAPYMin = &minVal
				existing.CrvAPYMax = &maxVal
			} else {
				results[pool] = &outbound.APYData{CrvAPYMin: &minVal, CrvAPYMax: &maxVal}
			}
		}
	}

	return results, nil
}

type feeAPY struct {
	daily  *float64
	weekly *float64
}

// fetchBaseAPYs returns fee APYs (both daily and weekly) keyed by lowercase pool address.
func (c *Client) fetchBaseAPYs(ctx context.Context, chainName string) (map[string]*feeAPY, error) {
	url := fmt.Sprintf("%s/getBaseApys/%s", c.baseURL, chainName)

	body, err := c.doGet(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("getBaseApys: %w", err)
	}

	var resp baseApyResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("parse getBaseApys: %w", err)
	}
	if !resp.Success {
		return nil, fmt.Errorf("getBaseApys returned success=false")
	}

	result := make(map[string]*feeAPY, len(resp.Data.BaseApys))
	for _, entry := range resp.Data.BaseApys {
		addr := strings.ToLower(entry.Address)
		apy := &feeAPY{}
		if entry.LatestDailyAPY != nil {
			v := *entry.LatestDailyAPY
			apy.daily = &v
		}
		if entry.LatestWeeklyAPY != nil {
			v := *entry.LatestWeeklyAPY
			apy.weekly = &v
		}
		if apy.daily != nil || apy.weekly != nil {
			result[addr] = apy
		}
	}

	return result, nil
}

// fetchGaugeAPYs returns CRV APYs [min, max] keyed by lowercase pool address.
// The getAllGauges response is a mixed-type map: most entries are gauge objects,
// but some (e.g. "generatedTimeMs") are booleans/numbers. We parse each entry
// individually and skip non-gauge values.
func (c *Client) fetchGaugeAPYs(ctx context.Context, chainName string) (map[string][2]float64, error) {
	url := fmt.Sprintf("%s/getAllGauges", c.baseURL)

	body, err := c.doGet(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("getAllGauges: %w", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("parse getAllGauges: %w", err)
	}

	result := make(map[string][2]float64)
	for _, v := range raw {
		var gauge gaugeEntry
		if err := json.Unmarshal(v, &gauge); err != nil {
			continue // skip non-gauge entries (booleans, numbers, etc.)
		}
		if !strings.EqualFold(gauge.BlockchainID, chainName) {
			continue
		}
		if gauge.IsKilled || gauge.HasNoCrv {
			continue
		}
		if len(gauge.GaugeCrvAPY) < 2 {
			continue
		}
		addr := strings.ToLower(gauge.Swap)
		result[addr] = [2]float64{gauge.GaugeCrvAPY[0], gauge.GaugeCrvAPY[1]}
	}

	return result, nil
}

func (c *Client) doGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}

	return body, nil
}
