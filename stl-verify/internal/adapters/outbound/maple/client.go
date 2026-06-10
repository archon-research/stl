// Package maple implements the MapleGraphQLClient port against the Maple
// Finance GraphQL API (https://api.maple.finance/v2/graphql).
//
// The API is public and unauthenticated, Apollo-served with introspection
// disabled. It can return HTTP 200 with a GraphQL errors[] envelope, so every
// response is checked for errors before decoding data. Integer values are
// returned as decimal strings (except collateral.liquidationLevel and
// skyStrategy.version, which are JSON numbers) and parsed into big.Int;
// any malformed value fails the whole call — rows are never skipped.
package maple

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/time/rate"

	"github.com/archon-research/stl/stl-verify/internal/pkg/httpclient"
	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that Client implements outbound.MapleGraphQLClient.
var _ outbound.MapleGraphQLClient = (*Client)(nil)

const (
	// DefaultEndpoint is the production Maple GraphQL API endpoint.
	DefaultEndpoint = "https://api.maple.finance/v2/graphql"

	userAgent = "stl-verify-maple-graphql-indexer (github.com/archon-research/stl)"

	// poolBatchSize and strategyBatchSize cover small collections (~21 pools,
	// ~4 strategies today); loanBatchSize covers ~61 active loans with ample
	// headroom. The-Graph-style APIs commonly cap skip at 5000, so pagination
	// reaching that is a hard error (see fetchAll).
	poolBatchSize     = 100
	strategyBatchSize = 100
	loanBatchSize     = 1000
	skipCap           = 5000
)

// Config holds configuration for the Maple GraphQL client.
type Config struct {
	// Endpoint is the GraphQL API URL. Defaults to DefaultEndpoint.
	Endpoint string

	// Timeout is the maximum time for a single HTTP request. Defaults to 15s.
	Timeout time.Duration

	// MaxRetries is the number of retry attempts for transient failures
	// (HTTP 429/5xx, network errors). Defaults to 3.
	MaxRetries int

	// InitialBackoff is the delay before the first retry. Defaults to 500ms.
	InitialBackoff time.Duration

	// MaxBackoff caps the exponential backoff. Defaults to 10s.
	MaxBackoff time.Duration

	// BackoffFactor multiplies the backoff after each retry. Defaults to 2.0.
	BackoffFactor float64

	// RequestsPerSecond is the client-side rate limit. The cronjob makes ~6
	// requests per cycle, so this only matters for pagination bursts.
	// Defaults to 2.
	RequestsPerSecond float64

	// Logger is the structured logger for the client.
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.Endpoint == "" {
		c.Endpoint = DefaultEndpoint
	}
	if c.Timeout <= 0 {
		c.Timeout = 15 * time.Second
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	if c.InitialBackoff <= 0 {
		c.InitialBackoff = 500 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 10 * time.Second
	}
	if c.BackoffFactor <= 0 {
		c.BackoffFactor = 2.0
	}
	if c.RequestsPerSecond <= 0 {
		c.RequestsPerSecond = 2
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Client implements outbound.MapleGraphQLClient over plain HTTP POST.
type Client struct {
	endpoint    string
	httpClient  *http.Client
	limiter     *rate.Limiter
	retryConfig retry.Config
	logger      *slog.Logger
}

// NewClient creates a new Maple GraphQL client.
func NewClient(cfg Config) (*Client, error) {
	cfg.applyDefaults()

	return &Client{
		endpoint:   cfg.Endpoint,
		httpClient: &http.Client{Timeout: cfg.Timeout},
		limiter:    rate.NewLimiter(rate.Limit(cfg.RequestsPerSecond), 1),
		retryConfig: retry.Config{
			MaxRetries:     cfg.MaxRetries,
			InitialBackoff: cfg.InitialBackoff,
			MaxBackoff:     cfg.MaxBackoff,
			BackoffFactor:  cfg.BackoffFactor,
			Jitter:         true,
		},
		logger: cfg.Logger.With("component", "maple-graphql-client"),
	}, nil
}

// ---------------------------------------------------------------------------
// GraphQL queries (validated by execution against the live API; introspection
// is disabled). NOTE: poolV2S(orderBy: tvl) is invalid — do not order.
// ---------------------------------------------------------------------------

const poolsQuery = `query GetPools($first: Int!, $skip: Int!) {
  poolV2S(first: $first, skip: $skip) {
    id
    name
    monthlyApy
    spotApy
    assets
    collateralValue
    principalOut
    tvl
    asset { id symbol decimals }
    syrupRouter { id }
  }
}`

const activeLoansQuery = `query GetActiveLoans($first: Int!, $skip: Int!) {
  openTermLoans(first: $first, skip: $skip, where: { state: Active }) {
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
    fundingPool { id }
  }
}`

const skyStrategiesQuery = `query GetSkyStrategies($first: Int!, $skip: Int!) {
  skyStrategies(first: $first, skip: $skip) {
    id
    state
    currentlyDeployed
    depositedAssets
    withdrawnAssets
    strategyFeeRate
    totalFeesCollected
    version
    pool { id name }
  }
}`

const syrupGlobalsQuery = `query GetSyrupGlobals {
  syrupGlobals { apy collateralApy poolApy dripsYieldBoost tvl }
}`

// ---------------------------------------------------------------------------
// Wire types (GraphQL response shapes)
// ---------------------------------------------------------------------------

type assetWire struct {
	ID       string `json:"id"`
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
}

type poolWire struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	MonthlyApy      *string   `json:"monthlyApy"`
	SpotApy         *string   `json:"spotApy"`
	Assets          string    `json:"assets"`
	CollateralValue string    `json:"collateralValue"`
	PrincipalOut    string    `json:"principalOut"`
	TVL             string    `json:"tvl"`
	Asset           assetWire `json:"asset"`
	SyrupRouter     *struct {
		ID string `json:"id"`
	} `json:"syrupRouter"`
}

type collateralWire struct {
	Asset            string       `json:"asset"`
	AssetAmount      string       `json:"assetAmount"`
	AssetValueUSD    string       `json:"assetValueUsd"`
	Decimals         int          `json:"decimals"`
	State            *string      `json:"state"`
	Custodian        *string      `json:"custodian"`
	LiquidationLevel *json.Number `json:"liquidationLevel"`
}

type loanMetaWire struct {
	Type          *string `json:"type"`
	AssetSymbol   *string `json:"assetSymbol"`
	DexName       *string `json:"dexName"`
	Location      *string `json:"location"`
	WalletAddress *string `json:"walletAddress"`
	WalletType    *string `json:"walletType"`
}

type loanWire struct {
	ID       string `json:"id"`
	Borrower struct {
		ID string `json:"id"`
	} `json:"borrower"`
	State         string          `json:"state"`
	PrincipalOwed string          `json:"principalOwed"`
	AcmRatio      *string         `json:"acmRatio"` // null on uncollateralized loans
	Collateral    *collateralWire `json:"collateral"`
	LoanMeta      *loanMetaWire   `json:"loanMeta"`
	FundingPool   struct {
		ID string `json:"id"`
	} `json:"fundingPool"`
}

type skyStrategyWire struct {
	ID                 string  `json:"id"`
	State              string  `json:"state"`
	CurrentlyDeployed  string  `json:"currentlyDeployed"`
	DepositedAssets    string  `json:"depositedAssets"`
	WithdrawnAssets    string  `json:"withdrawnAssets"`
	StrategyFeeRate    *string `json:"strategyFeeRate"`
	TotalFeesCollected *string `json:"totalFeesCollected"`
	Version            int     `json:"version"`
	Pool               struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"pool"`
}

type syrupGlobalsWire struct {
	APY             string  `json:"apy"`
	CollateralAPY   string  `json:"collateralApy"`
	PoolAPY         string  `json:"poolApy"`
	DripsYieldBoost *string `json:"dripsYieldBoost"`
	TVL             string  `json:"tvl"`
}

// ---------------------------------------------------------------------------
// Port methods
// ---------------------------------------------------------------------------

// GetPools fetches all PoolV2 lending pools, paginating transparently.
func (c *Client) GetPools(ctx context.Context) ([]outbound.MaplePool, error) {
	wires, err := fetchAll(poolBatchSize, func(first, skip int) ([]poolWire, error) {
		var resp struct {
			Data struct {
				PoolV2S []poolWire `json:"poolV2S"`
			} `json:"data"`
		}
		if err := c.execute(ctx, poolsQuery, pageVariables(first, skip), &resp); err != nil {
			return nil, fmt.Errorf("querying pools (skip=%d): %w", skip, err)
		}
		return resp.Data.PoolV2S, nil
	})
	if err != nil {
		return nil, err
	}

	pools := make([]outbound.MaplePool, 0, len(wires))
	for _, w := range wires {
		pool, err := parsePool(w)
		if err != nil {
			return nil, err
		}
		pools = append(pools, pool)
	}
	return pools, nil
}

// GetActiveLoans fetches all Open Term Loans with state Active, paginating
// transparently.
func (c *Client) GetActiveLoans(ctx context.Context) ([]outbound.MapleActiveLoan, error) {
	wires, err := fetchAll(loanBatchSize, func(first, skip int) ([]loanWire, error) {
		var resp struct {
			Data struct {
				OpenTermLoans []loanWire `json:"openTermLoans"`
			} `json:"data"`
		}
		if err := c.execute(ctx, activeLoansQuery, pageVariables(first, skip), &resp); err != nil {
			return nil, fmt.Errorf("querying active loans (skip=%d): %w", skip, err)
		}
		return resp.Data.OpenTermLoans, nil
	})
	if err != nil {
		return nil, err
	}

	loans := make([]outbound.MapleActiveLoan, 0, len(wires))
	for _, w := range wires {
		loan, err := parseLoan(w)
		if err != nil {
			return nil, err
		}
		loans = append(loans, loan)
	}
	return loans, nil
}

// GetSkyStrategies fetches all Sky strategies, paginating transparently.
func (c *Client) GetSkyStrategies(ctx context.Context) ([]outbound.MapleSkyStrategy, error) {
	wires, err := fetchAll(strategyBatchSize, func(first, skip int) ([]skyStrategyWire, error) {
		var resp struct {
			Data struct {
				SkyStrategies []skyStrategyWire `json:"skyStrategies"`
			} `json:"data"`
		}
		if err := c.execute(ctx, skyStrategiesQuery, pageVariables(first, skip), &resp); err != nil {
			return nil, fmt.Errorf("querying sky strategies (skip=%d): %w", skip, err)
		}
		return resp.Data.SkyStrategies, nil
	})
	if err != nil {
		return nil, err
	}

	strategies := make([]outbound.MapleSkyStrategy, 0, len(wires))
	for _, w := range wires {
		strategy, err := parseSkyStrategy(w)
		if err != nil {
			return nil, err
		}
		strategies = append(strategies, strategy)
	}
	return strategies, nil
}

// GetSyrupGlobals fetches the protocol-wide Syrup aggregates (singleton).
func (c *Client) GetSyrupGlobals(ctx context.Context) (*outbound.MapleSyrupGlobals, error) {
	var resp struct {
		Data struct {
			SyrupGlobals *syrupGlobalsWire `json:"syrupGlobals"`
		} `json:"data"`
	}
	if err := c.execute(ctx, syrupGlobalsQuery, nil, &resp); err != nil {
		return nil, fmt.Errorf("querying syrup globals: %w", err)
	}
	if resp.Data.SyrupGlobals == nil {
		return nil, fmt.Errorf("syrup globals: API returned null")
	}
	return parseSyrupGlobals(*resp.Data.SyrupGlobals)
}

// ---------------------------------------------------------------------------
// Wire -> port DTO parsing. Every malformed value fails the whole call with
// the owning entity's ID in the error — rows are never silently skipped.
// ---------------------------------------------------------------------------

func parsePool(w poolWire) (outbound.MaplePool, error) {
	address, err := parseAddress(w.ID, "pool id", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}
	assetAddress, err := parseAddress(w.Asset.ID, "asset id", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}

	tvl, err := parseBigInt(w.TVL, "tvl", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}
	liquidAssets, err := parseBigInt(w.Assets, "assets", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}
	collateralUSD, err := parseBigInt(w.CollateralValue, "collateralValue", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}
	principalOut, err := parseBigInt(w.PrincipalOut, "principalOut", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}
	monthlyAPY, err := parseOptionalBigInt(w.MonthlyApy, "monthlyApy", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}
	spotAPY, err := parseOptionalBigInt(w.SpotApy, "spotApy", w.ID)
	if err != nil {
		return outbound.MaplePool{}, err
	}

	return outbound.MaplePool{
		Address:       address,
		Name:          w.Name,
		AssetAddress:  assetAddress,
		AssetSymbol:   w.Asset.Symbol,
		AssetDecimals: w.Asset.Decimals,
		IsSyrup:       w.SyrupRouter != nil,
		TVL:           tvl,
		LiquidAssets:  liquidAssets,
		CollateralUSD: collateralUSD,
		PrincipalOut:  principalOut,
		MonthlyAPY:    monthlyAPY,
		SpotAPY:       spotAPY,
	}, nil
}

func parseLoan(w loanWire) (outbound.MapleActiveLoan, error) {
	loanID, err := parseAddress(w.ID, "loan id", w.ID)
	if err != nil {
		return outbound.MapleActiveLoan{}, err
	}
	borrower, err := parseAddress(w.Borrower.ID, "borrower id", w.ID)
	if err != nil {
		return outbound.MapleActiveLoan{}, err
	}
	poolAddress, err := parseAddress(w.FundingPool.ID, "funding pool id", w.ID)
	if err != nil {
		return outbound.MapleActiveLoan{}, err
	}
	principalOwed, err := parseBigInt(w.PrincipalOwed, "principalOwed", w.ID)
	if err != nil {
		return outbound.MapleActiveLoan{}, err
	}
	acmRatio, err := parseOptionalBigInt(w.AcmRatio, "acmRatio", w.ID)
	if err != nil {
		return outbound.MapleActiveLoan{}, err
	}
	collateral, err := parseCollateral(w.Collateral, w.ID)
	if err != nil {
		return outbound.MapleActiveLoan{}, err
	}

	var loanMeta *outbound.MapleLoanMeta
	if w.LoanMeta != nil {
		loanMeta = &outbound.MapleLoanMeta{
			Type:          deref(w.LoanMeta.Type),
			AssetSymbol:   deref(w.LoanMeta.AssetSymbol),
			DexName:       deref(w.LoanMeta.DexName),
			Location:      deref(w.LoanMeta.Location),
			WalletAddress: deref(w.LoanMeta.WalletAddress), // may be non-EVM; never hex-validated
			WalletType:    deref(w.LoanMeta.WalletType),
		}
	}

	return outbound.MapleActiveLoan{
		LoanID:        loanID,
		Borrower:      borrower,
		State:         w.State,
		PrincipalOwed: principalOwed,
		AcmRatio:      acmRatio,
		Collateral:    collateral,
		LoanMeta:      loanMeta,
		PoolAddress:   poolAddress,
	}, nil
}

func parseCollateral(w *collateralWire, loanID string) (*outbound.MapleLoanCollateral, error) {
	if w == nil {
		return nil, nil
	}

	amount, err := parseBigInt(w.AssetAmount, "collateral.assetAmount", loanID)
	if err != nil {
		return nil, err
	}
	valueUSD, err := parseBigInt(w.AssetValueUSD, "collateral.assetValueUsd", loanID)
	if err != nil {
		return nil, err
	}

	// liquidationLevel is a JSON number on the wire (unlike every other
	// integer field, which is a string).
	var liquidationLevel *big.Int
	if w.LiquidationLevel != nil {
		liquidationLevel, err = parseBigInt(w.LiquidationLevel.String(), "collateral.liquidationLevel", loanID)
		if err != nil {
			return nil, err
		}
	}

	return &outbound.MapleLoanCollateral{
		Asset:            w.Asset,
		AssetAmount:      amount,
		AssetValueUSD:    valueUSD,
		Decimals:         w.Decimals,
		State:            deref(w.State),
		Custodian:        deref(w.Custodian),
		LiquidationLevel: liquidationLevel,
	}, nil
}

func parseSkyStrategy(w skyStrategyWire) (outbound.MapleSkyStrategy, error) {
	address, err := parseAddress(w.ID, "strategy id", w.ID)
	if err != nil {
		return outbound.MapleSkyStrategy{}, err
	}
	poolAddress, err := parseAddress(w.Pool.ID, "pool id", w.ID)
	if err != nil {
		return outbound.MapleSkyStrategy{}, err
	}
	currentlyDeployed, err := parseBigInt(w.CurrentlyDeployed, "currentlyDeployed", w.ID)
	if err != nil {
		return outbound.MapleSkyStrategy{}, err
	}
	depositedAssets, err := parseBigInt(w.DepositedAssets, "depositedAssets", w.ID)
	if err != nil {
		return outbound.MapleSkyStrategy{}, err
	}
	withdrawnAssets, err := parseBigInt(w.WithdrawnAssets, "withdrawnAssets", w.ID)
	if err != nil {
		return outbound.MapleSkyStrategy{}, err
	}
	strategyFeeRate, err := parseOptionalBigInt(w.StrategyFeeRate, "strategyFeeRate", w.ID)
	if err != nil {
		return outbound.MapleSkyStrategy{}, err
	}
	totalFeesCollected, err := parseOptionalBigInt(w.TotalFeesCollected, "totalFeesCollected", w.ID)
	if err != nil {
		return outbound.MapleSkyStrategy{}, err
	}

	return outbound.MapleSkyStrategy{
		Address:            address,
		PoolAddress:        poolAddress,
		State:              w.State,
		Version:            w.Version,
		CurrentlyDeployed:  currentlyDeployed,
		DepositedAssets:    depositedAssets,
		WithdrawnAssets:    withdrawnAssets,
		StrategyFeeRate:    strategyFeeRate,
		TotalFeesCollected: totalFeesCollected,
	}, nil
}

func parseSyrupGlobals(w syrupGlobalsWire) (*outbound.MapleSyrupGlobals, error) {
	tvl, err := parseBigInt(w.TVL, "tvl", "syrupGlobals")
	if err != nil {
		return nil, err
	}
	apy, err := parseBigInt(w.APY, "apy", "syrupGlobals")
	if err != nil {
		return nil, err
	}
	collateralAPY, err := parseBigInt(w.CollateralAPY, "collateralApy", "syrupGlobals")
	if err != nil {
		return nil, err
	}
	poolAPY, err := parseBigInt(w.PoolAPY, "poolApy", "syrupGlobals")
	if err != nil {
		return nil, err
	}
	dripsYieldBoost, err := parseOptionalBigInt(w.DripsYieldBoost, "dripsYieldBoost", "syrupGlobals")
	if err != nil {
		return nil, err
	}

	return &outbound.MapleSyrupGlobals{
		TVL:             tvl,
		APY:             apy,
		CollateralAPY:   collateralAPY,
		PoolAPY:         poolAPY,
		DripsYieldBoost: dripsYieldBoost,
	}, nil
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

func parseBigInt(s, field, id string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("parsing %s for %s: invalid integer string %q", field, id, s)
	}
	return n, nil
}

func parseOptionalBigInt(s *string, field, id string) (*big.Int, error) {
	if s == nil {
		return nil, nil
	}
	return parseBigInt(*s, field, id)
}

// parseAddress validates with IsHexAddress before converting:
// common.HexToAddress silently coerces garbage.
func parseAddress(s, field, id string) (common.Address, error) {
	if !common.IsHexAddress(s) {
		return common.Address{}, fmt.Errorf("parsing %s for %s: invalid address %q", field, id, s)
	}
	return common.HexToAddress(s), nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func pageVariables(first, skip int) map[string]any {
	return map[string]any{"first": first, "skip": skip}
}

// fetchAll paginates skip += batchSize until a page returns fewer than
// batchSize rows. Paginating past skipCap fails hard: The-Graph-style APIs
// commonly cap skip at 5000, so continuing would either persist a silently
// truncated snapshot or, if the API ignores skip, loop forever.
func fetchAll[T any](batchSize int, page func(first, skip int) ([]T, error)) ([]T, error) {
	var all []T
	for skip := 0; ; skip += batchSize {
		if skip >= skipCap {
			return nil, fmt.Errorf("pagination reached skip=%d (cap %d); refusing to return a possibly truncated result set", skip, skipCap)
		}
		items, err := page(batchSize, skip)
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
		if len(items) < batchSize {
			return all, nil
		}
	}
}

// ---------------------------------------------------------------------------
// GraphQL transport
// ---------------------------------------------------------------------------

type graphqlRequest struct {
	Query     string         `json:"query"`
	Variables map[string]any `json:"variables,omitempty"`
}

// execute sends a GraphQL request with rate limiting and retries on transient
// failures (HTTP 429/5xx, network errors), then decodes the response into
// result. GraphQL errors[] and HTTP 4xx are not retried.
func (c *Client) execute(ctx context.Context, query string, variables map[string]any, result any) error {
	body, err := json.Marshal(graphqlRequest{Query: query, Variables: variables})
	if err != nil {
		return fmt.Errorf("marshalling request: %w", err)
	}

	isRetryable := func(err error) bool {
		var nonRetryable *httpclient.NonRetryableError
		return !errors.As(err, &nonRetryable)
	}
	onRetry := func(attempt int, err error, backoff time.Duration) {
		c.logger.Warn("request failed, retrying",
			"attempt", attempt,
			"maxRetries", c.retryConfig.MaxRetries,
			"backoff", backoff,
			"error", err,
		)
	}

	return retry.DoVoid(ctx, c.retryConfig, isRetryable, onRetry, func() error {
		if err := c.limiter.Wait(ctx); err != nil {
			return httpclient.WrapNonRetryable(fmt.Errorf("rate limiter: %w", err))
		}
		return c.doSingleRequest(ctx, body, result)
	})
}

func (c *Client) doSingleRequest(ctx context.Context, body []byte, result any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return httpclient.WrapNonRetryable(fmt.Errorf("creating request: %w", err))
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("executing request: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Warn("failed to close response body", "error", closeErr)
		}
	}()

	if resp.StatusCode == http.StatusTooManyRequests {
		return fmt.Errorf("rate limited (HTTP 429): %s", readBodySnippet(resp.Body))
	}
	if resp.StatusCode >= 500 {
		return fmt.Errorf("server error (HTTP %d): %s", resp.StatusCode, readBodySnippet(resp.Body))
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return httpclient.WrapNonRetryable(
			fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(respBody)))
	}

	// Apollo can return HTTP 200 with a GraphQL errors[] envelope — check it
	// before decoding data.
	var envelope struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return httpclient.WrapNonRetryable(fmt.Errorf("decoding GraphQL response: %w", err))
	}
	if len(envelope.Errors) > 0 {
		messages := make([]string, 0, len(envelope.Errors))
		for _, e := range envelope.Errors {
			messages = append(messages, e.Message)
		}
		return httpclient.WrapNonRetryable(
			fmt.Errorf("graphql error: %s", strings.Join(messages, "; ")))
	}

	if err := json.Unmarshal(respBody, result); err != nil {
		return httpclient.WrapNonRetryable(fmt.Errorf("decoding response: %w", err))
	}
	return nil
}

// maxErrorBodyBytes bounds how much of an error response body is included in
// error messages.
const maxErrorBodyBytes = 2048

// readBodySnippet reads a bounded snippet of an error response body for
// inclusion in the error message, so retry-exhausted 429/5xx failures carry
// the upstream diagnostic.
func readBodySnippet(r io.Reader) string {
	b, err := io.ReadAll(io.LimitReader(r, maxErrorBodyBytes))
	if err != nil {
		return fmt.Sprintf("<reading body: %v>", err)
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return "<empty body>"
	}
	return s
}
