package maple

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check.
var _ outbound.MapleClient = (*Client)(nil)

// Config holds settings for the Maple GraphQL client.
type Config struct {
	Endpoint   string
	HTTPClient *http.Client
	Logger     *slog.Logger
	Timeout    time.Duration
}

// ConfigDefaults returns a Config with sensible defaults.
func ConfigDefaults() Config {
	return Config{
		Endpoint: "https://api.maple.finance/v2/graphql",
		Timeout:  15 * time.Second,
	}
}

// Client queries the Maple Finance GraphQL API.
type Client struct {
	endpoint   string
	httpClient *http.Client
	logger     *slog.Logger
}

// NewClient creates a new Maple GraphQL client.
func NewClient(cfg Config) (*Client, error) {
	defaults := ConfigDefaults()
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaults.Endpoint
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = defaults.Timeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: cfg.Timeout}
	}

	return &Client{
		endpoint:   cfg.Endpoint,
		httpClient: cfg.HTTPClient,
		logger:     cfg.Logger.With("component", "maple-graphql-client"),
	}, nil
}

// ListPools queries all Maple pools.
func (c *Client) ListPools(ctx context.Context) ([]outbound.MaplePoolInfo, error) {
	query := `query {
		poolV2S(first: 1000) {
			id
			name
			asset { symbol decimals }
		}
	}`

	var resp poolsResponse
	if err := c.execute(ctx, query, nil, &resp); err != nil {
		return nil, fmt.Errorf("querying pools: %w", err)
	}

	pools := make([]outbound.MaplePoolInfo, 0, len(resp.Data.PoolV2S))
	for _, pool := range resp.Data.PoolV2S {
		pools = append(pools, outbound.MaplePoolInfo{
			Address:       common.HexToAddress(pool.ID),
			Name:          pool.Name,
			AssetSymbol:   pool.Asset.Symbol,
			AssetDecimals: pool.Asset.Decimals,
		})
	}

	return pools, nil
}

// GetPoolCollateral queries a pool's TVL and collateral composition.
func (c *Client) GetPoolCollateral(ctx context.Context, poolAddress common.Address) (*outbound.MaplePoolData, error) {
	poolID := strings.ToLower(poolAddress.Hex())

	query := `query($poolId: ID!) {
		poolV2(id: $poolId) {
			tvl
			poolMeta {
				poolCollaterals {
					asset
					assetValueUsd
					assetDecimals
				}
			}
		}
	}`

	variables := map[string]any{"poolId": poolID}

	var resp poolResponse
	if err := c.execute(ctx, query, variables, &resp); err != nil {
		return nil, fmt.Errorf("querying pool collateral: %w", err)
	}

	if resp.Data.PoolV2 == nil {
		return nil, fmt.Errorf("pool %s not found", poolID)
	}

	tvl, ok := new(big.Int).SetString(resp.Data.PoolV2.TVL, 10)
	if !ok {
		return nil, fmt.Errorf("parsing TVL %q for pool %s", resp.Data.PoolV2.TVL, poolID)
	}

	var collaterals []outbound.MapleCollateral
	for _, col := range resp.Data.PoolV2.PoolMeta.PoolCollaterals {
		val, ok := new(big.Int).SetString(col.AssetValueUSD, 10)
		if !ok {
			return nil, fmt.Errorf("parsing asset value %q for %s", col.AssetValueUSD, col.Asset)
		}
		// Skip zero-value collateral entries.
		if val.Sign() == 0 {
			continue
		}
		collaterals = append(collaterals, outbound.MapleCollateral{
			Asset:         col.Asset,
			AssetValueUSD: val,
			AssetDecimals: col.AssetDecimals,
		})
	}

	return &outbound.MaplePoolData{
		TVL:         tvl,
		Collaterals: collaterals,
	}, nil
}

// GetBorrowerCollateralAtBlock queries active loans and their collateral for a pool at a specific block.
func (c *Client) GetBorrowerCollateralAtBlock(ctx context.Context, poolAddress common.Address, blockNumber uint64) ([]outbound.MapleBorrowerLoan, error) {
	poolID := strings.ToLower(poolAddress.Hex())

	query := `query GetBorrowerCollateral($block: Block_height!, $poolId: ID!) {
		poolV2(block: $block, id: $poolId) {
			name
			asset { symbol decimals }
			openTermLoans(first: 1000, where: { state: Active }) {
				id
				borrower { id }
				state
				principalOwed
				acmRatio
				collateral {
					asset
					assetAmount
					assetValueUsd
					decimals
					state
					custodian
					liquidationLevel
				}
			}
		}
	}`

	variables := map[string]any{
		"poolId": poolID,
		"block":  map[string]any{"number": blockNumber},
	}

	var resp borrowerCollateralResponse
	if err := c.execute(ctx, query, variables, &resp); err != nil {
		return nil, fmt.Errorf("querying borrower collateral: %w", err)
	}

	if resp.Data.PoolV2 == nil {
		return nil, fmt.Errorf("pool %s not found", poolID)
	}

	loans := make([]outbound.MapleBorrowerLoan, 0, len(resp.Data.PoolV2.OpenTermLoans))
	for _, l := range resp.Data.PoolV2.OpenTermLoans {
		principalOwed, ok := new(big.Int).SetString(l.PrincipalOwed, 10)
		if !ok {
			return nil, fmt.Errorf("parsing principal owed %q for loan %s", l.PrincipalOwed, l.ID)
		}

		acmRatio, ok := new(big.Int).SetString(l.AcmRatio, 10)
		if !ok {
			return nil, fmt.Errorf("parsing acm ratio %q for loan %s", l.AcmRatio, l.ID)
		}

		assetAmount, ok := new(big.Int).SetString(l.Collateral.AssetAmount, 10)
		if !ok {
			return nil, fmt.Errorf("parsing asset amount %q for loan %s", l.Collateral.AssetAmount, l.ID)
		}

		assetValueUSD, ok := new(big.Int).SetString(l.Collateral.AssetValueUSD, 10)
		if !ok {
			return nil, fmt.Errorf("parsing asset value usd %q for loan %s", l.Collateral.AssetValueUSD, l.ID)
		}

		var liquidationLevel *big.Int
		if l.Collateral.LiquidationLevel != "" {
			liquidationLevel, ok = new(big.Int).SetString(l.Collateral.LiquidationLevel, 10)
			if !ok {
				return nil, fmt.Errorf("parsing liquidation level %q for loan %s", l.Collateral.LiquidationLevel, l.ID)
			}
		}

		loans = append(loans, outbound.MapleBorrowerLoan{
			LoanID:        common.HexToAddress(l.ID),
			Borrower:      common.HexToAddress(l.Borrower.ID),
			State:         l.State,
			PrincipalOwed: principalOwed,
			AcmRatio:      acmRatio,
			Collateral: outbound.MapleLoanCollateral{
				Asset:            l.Collateral.Asset,
				AssetAmount:      assetAmount,
				AssetValueUSD:    assetValueUSD,
				Decimals:         l.Collateral.Decimals,
				State:            l.Collateral.State,
				Custodian:        l.Collateral.Custodian,
				LiquidationLevel: liquidationLevel,
			},
		})
	}

	return loans, nil
}

// execute sends a GraphQL request and decodes the response.
func (c *Client) execute(ctx context.Context, query string, variables map[string]any, result any) error {
	reqBody := graphqlRequest{
		Query:     query,
		Variables: variables,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody))
	}

	// Check for GraphQL-level errors.
	var errResp graphqlErrorResponse
	if err := json.Unmarshal(respBody, &errResp); err == nil && len(errResp.Errors) > 0 {
		return fmt.Errorf("graphql error: %s", errResp.Errors[0].Message)
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return fmt.Errorf("decoding response: %w", err)
	}

	return nil
}

// --- GraphQL request/response types ---

type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

type graphqlErrorResponse struct {
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// Pool list response.
type poolsResponse struct {
	Data struct {
		PoolV2S []poolInfo `json:"poolV2S"`
	} `json:"data"`
}

type poolV2Position struct {
	Pool           poolInfo `json:"pool"`
	LendingBalance string   `json:"lendingBalance"`
}

type poolInfo struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Asset assetInfo `json:"asset"`
}

type assetInfo struct {
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
}

// Pool collateral response.
type poolResponse struct {
	Data struct {
		PoolV2 *poolV2Data `json:"poolV2"`
	} `json:"data"`
}

type poolV2Data struct {
	TVL      string   `json:"tvl"`
	PoolMeta poolMeta `json:"poolMeta"`
}

type poolMeta struct {
	PoolCollaterals []poolCollateral `json:"poolCollaterals"`
}

type poolCollateral struct {
	Asset         string `json:"asset"`
	AssetValueUSD string `json:"assetValueUsd"`
	AssetDecimals int    `json:"assetDecimals"`
}

// Borrower collateral response.
type borrowerCollateralResponse struct {
	Data struct {
		PoolV2 *borrowerCollateralPool `json:"poolV2"`
	} `json:"data"`
}

type borrowerCollateralPool struct {
	Name          string         `json:"name"`
	Asset         assetInfo      `json:"asset"`
	OpenTermLoans []openTermLoan `json:"openTermLoans"`
}

type openTermLoan struct {
	ID            string         `json:"id"`
	Borrower      borrowerInfo   `json:"borrower"`
	State         string         `json:"state"`
	PrincipalOwed string         `json:"principalOwed"`
	AcmRatio      string         `json:"acmRatio"`
	Collateral    loanCollateral `json:"collateral"`
}

type borrowerInfo struct {
	ID string `json:"id"`
}

type loanCollateral struct {
	Asset            string `json:"asset"`
	AssetAmount      string `json:"assetAmount"`
	AssetValueUSD    string `json:"assetValueUsd"`
	Decimals         int    `json:"decimals"`
	State            string `json:"state"`
	Custodian        string `json:"custodian"`
	LiquidationLevel string `json:"liquidationLevel"`
}
