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
)

const (
	defaultBaseURL = "https://api.curve.finance/v1"
	defaultTimeout = 15 * time.Second
)

// Client interacts with the Curve Finance REST API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	logger     *slog.Logger
}

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

// PoolAPY holds APY data for a single pool.
type PoolAPY struct {
	FeeAPY    float64
	CrvAPYMin float64
	CrvAPYMax float64
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
func (c *Client) FetchAPYs(ctx context.Context, chainName string, pools []common.Address) (map[common.Address]*PoolAPY, error) {
	results := make(map[common.Address]*PoolAPY, len(pools))

	// Build lookup set (lowercase)
	poolSet := make(map[string]common.Address, len(pools))
	for _, p := range pools {
		poolSet[strings.ToLower(p.Hex())] = p
	}

	// Fetch base APYs (trading fees)
	feeAPYs, err := c.fetchBaseAPYs(ctx, chainName)
	if err != nil {
		c.logger.Warn("failed to fetch base APYs", "error", err)
	} else {
		for addrHex, apy := range feeAPYs {
			if pool, ok := poolSet[addrHex]; ok {
				results[pool] = &PoolAPY{FeeAPY: apy}
			}
		}
	}

	// Fetch CRV gauge APYs
	crvAPYs, err := c.fetchGaugeAPYs(ctx, chainName)
	if err != nil {
		c.logger.Warn("failed to fetch gauge APYs", "error", err)
	} else {
		for addrHex, crv := range crvAPYs {
			if pool, ok := poolSet[addrHex]; ok {
				if existing, ok := results[pool]; ok {
					existing.CrvAPYMin = crv[0]
					existing.CrvAPYMax = crv[1]
				} else {
					results[pool] = &PoolAPY{CrvAPYMin: crv[0], CrvAPYMax: crv[1]}
				}
			}
		}
	}

	return results, nil
}

// fetchBaseAPYs returns fee APYs keyed by lowercase pool address.
func (c *Client) fetchBaseAPYs(ctx context.Context, chainName string) (map[string]float64, error) {
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

	result := make(map[string]float64, len(resp.Data.BaseApys))
	for _, entry := range resp.Data.BaseApys {
		addr := strings.ToLower(entry.Address)
		if entry.LatestWeeklyAPY != nil {
			result[addr] = *entry.LatestWeeklyAPY
		} else if entry.LatestDailyAPY != nil {
			result[addr] = *entry.LatestDailyAPY
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
		if gauge.BlockchainID != chainName {
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
