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

// configDefaults returns a Config with sensible defaults.
func configDefaults() Config {
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
	defaults := configDefaults()
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

// GetAllActiveLoansAtBlock queries all active open-term loans across all pools at a specific block.
// Uses pagination to handle cases where there are more than 1000 loans.
func (c *Client) GetAllActiveLoansAtBlock(ctx context.Context, blockNumber uint64) ([]outbound.MapleActiveLoan, error) {
	const batchSize = 1000
	var allLoans []outbound.MapleActiveLoan
	query := `query GetAllActiveLoans($block: Block_height!, $first: Int!, $skip: Int!) {
		openTermLoans(block: $block, first: $first, skip: $skip, where: { state: Active }) {
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
			loanMeta {
				type
				assetSymbol
				dexName
				location
				walletAddress
				walletType
			}
			fundingPool {
				id
				name
				asset { symbol decimals }
			}
		}
	}`

	for skip := 0; ; skip += batchSize {
		variables := map[string]any{
			"block": map[string]any{"number": blockNumber},
			"first": batchSize,
			"skip":  skip,
		}

		var resp allActiveLoansResponse
		if err := c.execute(ctx, query, variables, &resp); err != nil {
			return nil, fmt.Errorf("querying all active loans: %w", err)
		}

		parsed, err := parseLoanData(resp.Data.OpenTermLoans)
		if err != nil {
			return nil, err
		}
		allLoans = append(allLoans, parsed...)

		if len(resp.Data.OpenTermLoans) < batchSize {
			break
		}
	}

	return allLoans, nil
}

// parseLoanData converts raw GraphQL loan responses into typed MapleActiveLoan structs.
func parseLoanData(loans []activeOpenTermLoan) ([]outbound.MapleActiveLoan, error) {
	result := make([]outbound.MapleActiveLoan, 0, len(loans))

	for _, l := range loans {
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

		var loanMeta *outbound.MapleLoanMeta
		if l.LoanMeta != nil {
			loanMeta = &outbound.MapleLoanMeta{
				Type:          l.LoanMeta.Type,
				AssetSymbol:   l.LoanMeta.AssetSymbol,
				DexName:       l.LoanMeta.DexName,
				Location:      l.LoanMeta.Location,
				WalletAddress: l.LoanMeta.WalletAddress,
				WalletType:    l.LoanMeta.WalletType,
			}
		}

		result = append(result, outbound.MapleActiveLoan{
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
			LoanMeta:          loanMeta,
			PoolAddress:       common.HexToAddress(l.FundingPool.ID),
			PoolName:          l.FundingPool.Name,
			PoolAssetSymbol:   l.FundingPool.Asset.Symbol,
			PoolAssetDecimals: l.FundingPool.Asset.Decimals,
		})
	}

	return result, nil
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

	// Decode into a unified envelope that captures both errors and the typed data.
	// This avoids parsing the body twice.
	var envelope struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		c.logger.Warn("decoding GraphQL response", "error", err, "response", string(respBody))
		return fmt.Errorf("decoding GraphQL response: %w", err)
	}
	if len(envelope.Errors) > 0 {
		messages := make([]string, 0, len(envelope.Errors))
		for _, entry := range envelope.Errors {
			messages = append(messages, entry.Message)
		}
		return fmt.Errorf("graphql error: %s", strings.Join(messages, "; "))
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

type assetInfo struct {
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
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

type loanMetaResponse struct {
	Type          string `json:"type"`
	AssetSymbol   string `json:"assetSymbol"`
	DexName       string `json:"dexName"`
	Location      string `json:"location"`
	WalletAddress string `json:"walletAddress"`
	WalletType    string `json:"walletType"`
}

// All active loans response (top-level openTermLoans query).
type allActiveLoansResponse struct {
	Data struct {
		OpenTermLoans []activeOpenTermLoan `json:"openTermLoans"`
	} `json:"data"`
}

type activeOpenTermLoan struct {
	ID            string            `json:"id"`
	Borrower      borrowerInfo      `json:"borrower"`
	State         string            `json:"state"`
	PrincipalOwed string            `json:"principalOwed"`
	AcmRatio      string            `json:"acmRatio"`
	Collateral    loanCollateral    `json:"collateral"`
	LoanMeta      *loanMetaResponse `json:"loanMeta"`
	FundingPool   fundingPool       `json:"fundingPool"`
}

type fundingPool struct {
	ID    string    `json:"id"`
	Name  string    `json:"name"`
	Asset assetInfo `json:"asset"`
}
