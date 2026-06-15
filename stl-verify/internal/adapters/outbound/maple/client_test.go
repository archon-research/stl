package maple

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	poolAddr     = "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"
	usdcAddr     = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
	loanAddr     = "0x0009bff1fcb8c767e5894164124d3e42aaca0542"
	borrowerAddr = "0xfba4bc924ba50c3b3dd0c1aa6d2f499b4fa55c81"
	strategyAddr = "0x859c9980931fa0a63765fd8ef2e29918af5b038c"
)

// graphqlHandler routes requests by operation name in the query string.
type graphqlHandler struct {
	t          *testing.T
	handleFunc func(w http.ResponseWriter, query string, variables map[string]any)
}

func (h graphqlHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.t.Helper()
	if r.Method != http.MethodPost {
		h.t.Errorf("method = %s, want POST", r.Method)
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		h.t.Errorf("Content-Type = %q, want application/json", ct)
	}
	if ua := r.Header.Get("User-Agent"); !strings.Contains(ua, "stl-verify") {
		h.t.Errorf("User-Agent = %q, want a descriptive stl-verify agent", ua)
	}

	var req struct {
		Query     string         `json:"query"`
		Variables map[string]any `json:"variables"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.t.Errorf("decoding request: %v", err)
	}
	h.handleFunc(w, req.Query, req.Variables)
}

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	return newTestClientWithLogger(t, handler, nil)
}

// newTestClientWithLogger wires a logger so tests can assert emitted records
// (e.g. the null-downgrade warns).
func newTestClientWithLogger(t *testing.T, handler http.Handler, logger *slog.Logger) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := NewClient(Config{
		Endpoint:          server.URL,
		Timeout:           5 * time.Second,
		MaxRetries:        2,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        5 * time.Millisecond,
		RequestsPerSecond: 10000,
		Logger:            logger,
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func writeJSON(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(body))
}

func bigIntPtrEqual(a, b *big.Int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Cmp(b) == 0
}

func poolJSON(id string) string {
	return fmt.Sprintf(`{
		"id": %q, "name": "Syrup USDC",
		"monthlyApy": "46314953537216910976747498327", "spotApy": "0",
		"assets": "159965700", "collateralValue": "500", "principalOut": "600", "tvl": "1000",
		"asset": {"id": %q, "symbol": "USDC", "decimals": 6},
		"syrupRouter": {"id": "0x1234567890123456789012345678901234567890"}
	}`, id, usdcAddr)
}

func TestGetPools_HappyPath(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, query string, _ map[string]any) {
		if !strings.Contains(query, "poolV2S") {
			t.Errorf("unexpected query: %s", query)
		}
		writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [%s]}}`, poolJSON(poolAddr)))
	}})

	pools, err := client.GetPools(context.Background())
	if err != nil {
		t.Fatalf("GetPools: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("len(pools) = %d, want 1", len(pools))
	}
	p := pools[0]
	if strings.ToLower(p.Address.Hex()) != poolAddr {
		t.Errorf("Address = %s, want %s", p.Address.Hex(), poolAddr)
	}
	if !p.IsSyrup {
		t.Error("IsSyrup = false, want true (syrupRouter non-null)")
	}
	if p.AssetSymbol != "USDC" || p.AssetDecimals != 6 {
		t.Errorf("asset = %s/%d, want USDC/6", p.AssetSymbol, p.AssetDecimals)
	}
	if p.TVL.Cmp(big.NewInt(1000)) != 0 || p.LiquidAssets.Cmp(big.NewInt(159965700)) != 0 {
		t.Errorf("TVL/LiquidAssets = %s/%s", p.TVL, p.LiquidAssets)
	}
	wantAPY, _ := new(big.Int).SetString("46314953537216910976747498327", 10)
	if p.MonthlyAPY.Cmp(wantAPY) != 0 {
		t.Errorf("MonthlyAPY = %s, want %s", p.MonthlyAPY, wantAPY)
	}
}

func TestGetPools_NullSyrupRouterAndAPY(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [{
			"id": %q, "name": "Plain Pool",
			"monthlyApy": null, "spotApy": null,
			"assets": "1", "collateralValue": "2", "principalOut": "3", "tvl": "4",
			"asset": {"id": %q, "symbol": "USDC", "decimals": 6},
			"syrupRouter": null
		}]}}`, poolAddr, usdcAddr))
	}})

	pools, err := client.GetPools(context.Background())
	if err != nil {
		t.Fatalf("GetPools: %v", err)
	}
	if pools[0].IsSyrup {
		t.Error("IsSyrup = true, want false (syrupRouter null)")
	}
	if pools[0].MonthlyAPY != nil || pools[0].SpotAPY != nil {
		t.Errorf("APYs = %v/%v, want nil/nil", pools[0].MonthlyAPY, pools[0].SpotAPY)
	}
}

func TestGetPools_Pagination(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, variables map[string]any) {
		call := calls.Add(1)
		first := int(variables["first"].(float64))
		skip := int(variables["skip"].(float64))
		if first != poolBatchSize {
			t.Errorf("first = %d, want %d", first, poolBatchSize)
		}

		switch call {
		case 1:
			if skip != 0 {
				t.Errorf("call 1 skip = %d, want 0", skip)
			}
			// Full page: batchSize pools with distinct addresses.
			pools := make([]string, poolBatchSize)
			for i := range pools {
				pools[i] = poolJSON(fmt.Sprintf("0x%040x", i+1))
			}
			writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [%s]}}`, strings.Join(pools, ",")))
		case 2:
			if skip != poolBatchSize {
				t.Errorf("call 2 skip = %d, want %d", skip, poolBatchSize)
			}
			// Partial page terminates pagination.
			writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [%s]}}`, poolJSON(poolAddr)))
		default:
			t.Errorf("unexpected call %d", call)
		}
	}})

	pools, err := client.GetPools(context.Background())
	if err != nil {
		t.Fatalf("GetPools: %v", err)
	}
	if len(pools) != poolBatchSize+1 {
		t.Errorf("len(pools) = %d, want %d", len(pools), poolBatchSize+1)
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2", calls.Load())
	}
}

func TestGetActiveLoans_HappyPath(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, query string, _ map[string]any) {
		if !strings.Contains(query, "openTermLoans") || !strings.Contains(query, "state: Active") {
			t.Errorf("unexpected query: %s", query)
		}
		writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
			"id": %q,
			"borrower": {"id": %q},
			"state": "Active",
			"principalOwed": "10000000000000",
			"acmRatio": "1445731",
			"collateral": {
				"asset": "SOL", "assetAmount": "215100000", "assetValueUsd": "6357500000",
				"decimals": 9, "state": "Deposited", "custodian": "ANCHORAGE",
				"liquidationLevel": 1020000
			},
			"loanMeta": {
				"type": "amm", "assetSymbol": null, "dexName": "Uniswap",
				"location": null, "walletAddress": "solana-wallet-xyz", "walletType": "SOL"
			},
			"fundingPool": {"id": %q}
		}]}}`, loanAddr, borrowerAddr, poolAddr))
	}})

	loans, err := client.GetActiveLoans(context.Background())
	if err != nil {
		t.Fatalf("GetActiveLoans: %v", err)
	}
	if len(loans) != 1 {
		t.Fatalf("len(loans) = %d, want 1", len(loans))
	}
	l := loans[0]
	if strings.ToLower(l.LoanID.Hex()) != loanAddr || strings.ToLower(l.Borrower.Hex()) != borrowerAddr {
		t.Errorf("ids = %s/%s", l.LoanID.Hex(), l.Borrower.Hex())
	}
	if l.PrincipalOwed.Cmp(big.NewInt(10000000000000)) != 0 || l.AcmRatio.Cmp(big.NewInt(1445731)) != 0 {
		t.Errorf("principal/acm = %s/%s", l.PrincipalOwed, l.AcmRatio)
	}
	if l.Collateral == nil {
		t.Fatal("Collateral = nil, want value")
	}
	// liquidationLevel arrives as a JSON number, not a string.
	if l.Collateral.LiquidationLevel.Cmp(big.NewInt(1020000)) != 0 {
		t.Errorf("LiquidationLevel = %s, want 1020000", l.Collateral.LiquidationLevel)
	}
	if l.Collateral.Asset != "SOL" || l.Collateral.Decimals != 9 || l.Collateral.Custodian != "ANCHORAGE" {
		t.Errorf("collateral = %+v", l.Collateral)
	}
	if l.LoanMeta == nil {
		t.Fatal("LoanMeta = nil, want value")
	}
	if l.LoanMeta.Type != "amm" || l.LoanMeta.DexName != "Uniswap" || l.LoanMeta.AssetSymbol != "" {
		t.Errorf("loanMeta = %+v", l.LoanMeta)
	}
	if l.LoanMeta.WalletAddress != "solana-wallet-xyz" {
		t.Errorf("WalletAddress = %q, want non-EVM string preserved", l.LoanMeta.WalletAddress)
	}
	if strings.ToLower(l.PoolAddress.Hex()) != poolAddr {
		t.Errorf("PoolAddress = %s, want %s", l.PoolAddress.Hex(), poolAddr)
	}
}

func TestGetActiveLoans_NullCollateralMetaAndAcmRatio(t *testing.T) {
	// Mirrors a live API observation: active uncollateralized loans return
	// null collateral, null loanMeta, AND null acmRatio.
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
			"id": %q, "borrower": {"id": %q}, "state": "Active",
			"principalOwed": "7000000", "acmRatio": null,
			"collateral": null, "loanMeta": null,
			"fundingPool": {"id": %q}
		}]}}`, loanAddr, borrowerAddr, poolAddr))
	}})

	loans, err := client.GetActiveLoans(context.Background())
	if err != nil {
		t.Fatalf("GetActiveLoans: %v", err)
	}
	if loans[0].Collateral != nil {
		t.Errorf("Collateral = %+v, want nil", loans[0].Collateral)
	}
	if loans[0].LoanMeta != nil {
		t.Errorf("LoanMeta = %+v, want nil", loans[0].LoanMeta)
	}
	if loans[0].AcmRatio != nil {
		t.Errorf("AcmRatio = %v, want nil", loans[0].AcmRatio)
	}
}

func TestGetPools_NullTVLAndCollateralValue(t *testing.T) {
	// tvl and collateralValue are nullable in the API schema; both surface
	// as nil (with a warn) rather than failing the call.
	handler := &testutil.SlogRecorder{}
	client := newTestClientWithLogger(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [{
			"id": %q, "name": "Bootstrapping Pool",
			"monthlyApy": null, "spotApy": null,
			"assets": "1", "collateralValue": null, "principalOut": "3", "tvl": null,
			"asset": {"id": %q, "symbol": "USDC", "decimals": 6},
			"syrupRouter": null
		}]}}`, poolAddr, usdcAddr))
	}}, slog.New(handler))

	pools, err := client.GetPools(context.Background())
	if err != nil {
		t.Fatalf("GetPools: %v", err)
	}
	if len(pools) != 1 {
		t.Fatalf("len(pools) = %d, want 1", len(pools))
	}
	if got := handler.CountWarn("storing as NULL"); got != 1 {
		t.Errorf("null-metric warn fired %d times, want exactly 1", got)
	}
	if pools[0].TVL != nil {
		t.Errorf("TVL = %v, want nil", pools[0].TVL)
	}
	if pools[0].CollateralUSD != nil {
		t.Errorf("CollateralUSD = %v, want nil", pools[0].CollateralUSD)
	}
	if pools[0].LiquidAssets.Cmp(big.NewInt(1)) != 0 || pools[0].PrincipalOut.Cmp(big.NewInt(3)) != 0 {
		t.Errorf("LiquidAssets/PrincipalOut = %s/%s, want 1/3", pools[0].LiquidAssets, pools[0].PrincipalOut)
	}
}

func TestNewClient_ValidatesEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint string
		wantErr  bool
	}{
		{name: "default endpoint", endpoint: "", wantErr: false},
		{name: "valid https", endpoint: "https://example.com/graphql", wantErr: false},
		{name: "valid http", endpoint: "http://localhost:8080/graphql", wantErr: false},
		{name: "missing scheme", endpoint: "example.com/graphql", wantErr: true},
		{name: "unsupported scheme", endpoint: "ftp://example.com/graphql", wantErr: true},
		{name: "scheme only", endpoint: "https://", wantErr: true},
		{name: "unparseable", endpoint: "http://[::1]:namedport", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClient(Config{Endpoint: tc.endpoint})
			if tc.wantErr && err == nil {
				t.Fatalf("NewClient(%q): expected error, got nil", tc.endpoint)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("NewClient(%q): unexpected error: %v", tc.endpoint, err)
			}
		})
	}
}

func TestTransportErrorsAreRetried(t *testing.T) {
	// A plain connection failure (no HTTP response at all) is the most
	// common production transient; it must stay retryable. A regression
	// wrapping the httpClient.Do error in WrapNonRetryable would silently
	// disable retries for network errors.
	server := httptest.NewServer(http.NotFoundHandler())
	endpoint := server.URL
	server.Close() // every request now fails with connection refused

	handler := &testutil.SlogRecorder{}
	client, err := NewClient(Config{
		Endpoint:          endpoint,
		Timeout:           time.Second,
		MaxRetries:        2,
		InitialBackoff:    time.Millisecond,
		MaxBackoff:        5 * time.Millisecond,
		RequestsPerSecond: 10000,
		Logger:            slog.New(handler),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	_, err = client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "executing request") {
		t.Errorf("error %q should come from the transport branch", err.Error())
	}
	if got := handler.CountWarn("request failed, retrying"); got != 2 {
		t.Errorf("retry warns = %d, want 2 (MaxRetries exhausted)", got)
	}
}

func TestNullCollectionsFailHard(t *testing.T) {
	// A null top-level collection (data:null or a null collection field with
	// no errors[]) is upstream breakage and must fail the call. Decoding it
	// to an empty list would persist an "everything gone" snapshot.
	for _, tc := range []struct {
		name    string
		body    string
		call    func(c *Client) error
		wantSub string
	}{
		{
			name: "pools data null", body: `{"data": null}`,
			call:    func(c *Client) error { _, err := c.GetPools(context.Background()); return err },
			wantSub: "null poolV2S collection",
		},
		{
			name: "pools collection null", body: `{"data": {"poolV2S": null}}`,
			call:    func(c *Client) error { _, err := c.GetPools(context.Background()); return err },
			wantSub: "null poolV2S collection",
		},
		{
			name: "loans data null", body: `{"data": null}`,
			call:    func(c *Client) error { _, err := c.GetActiveLoans(context.Background()); return err },
			wantSub: "null openTermLoans collection",
		},
		{
			name: "loans collection null", body: `{"data": {"openTermLoans": null}}`,
			call:    func(c *Client) error { _, err := c.GetActiveLoans(context.Background()); return err },
			wantSub: "null openTermLoans collection",
		},
		{
			name: "strategies data null", body: `{"data": null}`,
			call:    func(c *Client) error { _, err := c.GetSkyStrategies(context.Background()); return err },
			wantSub: "null skyStrategies collection",
		},
		{
			name: "strategies collection null", body: `{"data": {"skyStrategies": null}}`,
			call:    func(c *Client) error { _, err := c.GetSkyStrategies(context.Background()); return err },
			wantSub: "null skyStrategies collection",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
				writeJSON(w, tc.body)
			}})
			err := tc.call(client)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q should contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

func TestEmptyCollectionsAreValid(t *testing.T) {
	// A genuine [] is a legitimate result set (e.g. an empty loan book) and
	// must not be confused with a null collection.
	for _, tc := range []struct {
		name string
		body string
		call func(c *Client) (int, error)
	}{
		{
			name: "pools", body: `{"data": {"poolV2S": []}}`,
			call: func(c *Client) (int, error) { ps, err := c.GetPools(context.Background()); return len(ps), err },
		},
		{
			name: "loans", body: `{"data": {"openTermLoans": []}}`,
			call: func(c *Client) (int, error) { ls, err := c.GetActiveLoans(context.Background()); return len(ls), err },
		},
		{
			name: "strategies", body: `{"data": {"skyStrategies": []}}`,
			call: func(c *Client) (int, error) { ss, err := c.GetSkyStrategies(context.Background()); return len(ss), err },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
				writeJSON(w, tc.body)
			}})
			n, err := tc.call(client)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if n != 0 {
				t.Errorf("len = %d, want 0", n)
			}
		})
	}
}

func TestGetActiveLoans_NullCollateralAmountsPersistedAsNull(t *testing.T) {
	// assetAmount and assetValueUsd are nullable in the API schema (plausibly
	// during DepositPending). The collateral is kept with nil values so
	// "collateral pending" stays distinguishable from "no collateral".
	for _, tc := range []struct {
		name                string
		amount, usd         string
		wantAmount, wantUSD *big.Int
	}{
		{name: "null assetAmount", amount: "null", usd: `"6357500000"`, wantAmount: nil, wantUSD: big.NewInt(6357500000)},
		{name: "null assetValueUsd", amount: `"215100000"`, usd: "null", wantAmount: big.NewInt(215100000), wantUSD: nil},
		{name: "both null", amount: "null", usd: "null", wantAmount: nil, wantUSD: nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := &testutil.SlogRecorder{}
			client := newTestClientWithLogger(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
				writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
					"id": %q, "borrower": {"id": %q}, "state": "Active",
					"principalOwed": "7000000", "acmRatio": "1445731",
					"collateral": {
						"asset": "SOL", "assetAmount": %s, "assetValueUsd": %s,
						"decimals": 9, "state": "DepositPending", "custodian": "ANCHORAGE",
						"liquidationLevel": 1020000
					},
					"loanMeta": null,
					"fundingPool": {"id": %q}
				}]}}`, loanAddr, borrowerAddr, tc.amount, tc.usd, poolAddr))
			}}, slog.New(handler))

			loans, err := client.GetActiveLoans(context.Background())
			if err != nil {
				t.Fatalf("GetActiveLoans: %v", err)
			}
			if len(loans) != 1 {
				t.Fatalf("len(loans) = %d, want 1 (loan must be kept)", len(loans))
			}
			col := loans[0].Collateral
			if col == nil {
				t.Fatalf("Collateral = nil, want a collateral with nil amounts")
			}
			if !bigIntPtrEqual(col.AssetAmount, tc.wantAmount) {
				t.Errorf("AssetAmount = %v, want %v", col.AssetAmount, tc.wantAmount)
			}
			if !bigIntPtrEqual(col.AssetValueUSD, tc.wantUSD) {
				t.Errorf("AssetValueUSD = %v, want %v", col.AssetValueUSD, tc.wantUSD)
			}
			if col.State != "DepositPending" || col.Custodian != "ANCHORAGE" {
				t.Errorf("State/Custodian = %s/%s, want DepositPending/ANCHORAGE", col.State, col.Custodian)
			}
			if loans[0].PrincipalOwed.Cmp(big.NewInt(7000000)) != 0 {
				t.Errorf("PrincipalOwed = %s, want 7000000", loans[0].PrincipalOwed)
			}
			if got := handler.CountWarn("storing as NULL"); got != 1 {
				t.Errorf("null-downgrade warn fired %d times, want exactly 1", got)
			}
		})
	}
}

func TestGetActiveLoans_MalformedCollateralValuesNameLoanID(t *testing.T) {
	// Non-null but malformed collateral values are still hard errors (only
	// API-sanctioned nulls downgrade to an absent collateral).
	for _, tc := range []struct {
		name             string
		amount, usd, liq string
		wantField        string
	}{
		{name: "malformed assetAmount", amount: `"not-a-number"`, usd: `"1"`, liq: "null", wantField: "collateral.assetAmount"},
		{name: "malformed assetValueUsd", amount: `"1"`, usd: `"not-a-number"`, liq: "null", wantField: "collateral.assetValueUsd"},
		// liquidationLevel is the one JSON-number field; a fractional value
		// must fail the whole call per the package contract.
		{name: "fractional liquidationLevel", amount: `"1"`, usd: `"1"`, liq: "1020000.5", wantField: "collateral.liquidationLevel"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
				writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
					"id": %q, "borrower": {"id": %q}, "state": "Active",
					"principalOwed": "7000000", "acmRatio": null,
					"collateral": {
						"asset": "SOL", "assetAmount": %s, "assetValueUsd": %s,
						"decimals": 9, "state": "Deposited", "custodian": null,
						"liquidationLevel": %s
					},
					"loanMeta": null,
					"fundingPool": {"id": %q}
				}]}}`, loanAddr, borrowerAddr, tc.amount, tc.usd, tc.liq, poolAddr))
			}})

			_, err := client.GetActiveLoans(context.Background())
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), loanAddr) || !strings.Contains(err.Error(), tc.wantField) {
				t.Errorf("error %q should name loan %s and field %s", err, loanAddr, tc.wantField)
			}
		})
	}
}

func TestGetActiveLoans_MalformedBigIntNamesLoanID(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
			"id": %q, "borrower": {"id": %q}, "state": "Active",
			"principalOwed": "not-a-number", "acmRatio": "1",
			"collateral": null, "loanMeta": null,
			"fundingPool": {"id": %q}
		}]}}`, loanAddr, borrowerAddr, poolAddr))
	}})

	_, err := client.GetActiveLoans(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), loanAddr) {
		t.Errorf("error %q should name the loan id %s", err.Error(), loanAddr)
	}
	if !strings.Contains(err.Error(), "principalOwed") {
		t.Errorf("error %q should name the field", err.Error())
	}
}

func TestGetActiveLoans_NonActiveStateRejected(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
			"id": %q, "borrower": {"id": %q}, "state": "Liquidated",
			"principalOwed": "1", "acmRatio": "1",
			"collateral": null, "loanMeta": null,
			"fundingPool": {"id": %q}
		}]}}`, loanAddr, borrowerAddr, poolAddr))
	}})

	_, err := client.GetActiveLoans(context.Background())
	if err == nil {
		t.Fatal("expected error for non-Active loan state, got nil")
	}
	if !strings.Contains(err.Error(), loanAddr) {
		t.Errorf("error %q should name the loan id %s", err.Error(), loanAddr)
	}
	if !strings.Contains(err.Error(), "Liquidated") {
		t.Errorf("error %q should name the unexpected state", err.Error())
	}
}

func TestGetActiveLoans_InvalidBorrowerAddress(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
			"id": %q, "borrower": {"id": "garbage"}, "state": "Active",
			"principalOwed": "1", "acmRatio": "1",
			"collateral": null, "loanMeta": null,
			"fundingPool": {"id": %q}
		}]}}`, loanAddr, poolAddr))
	}})

	_, err := client.GetActiveLoans(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "invalid address") {
		t.Errorf("error %q should mention invalid address", err.Error())
	}
}

func TestGetPools_MalformedValueNamesPoolID(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [{
			"id": %q, "name": "Pool",
			"monthlyApy": "0", "spotApy": "0",
			"assets": "1", "collateralValue": "2", "principalOut": "3", "tvl": "not-a-number",
			"asset": {"id": %q, "symbol": "USDC", "decimals": 6},
			"syrupRouter": null
		}]}}`, poolAddr, usdcAddr))
	}})

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), poolAddr) || !strings.Contains(err.Error(), "tvl") {
		t.Errorf("error %q should name the pool id and the field", err.Error())
	}
}

func TestGetSkyStrategies_MalformedValueNamesStrategyID(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"skyStrategies": [{
			"id": %q, "state": "Active",
			"currentlyDeployed": "0", "depositedAssets": "not-a-number", "withdrawnAssets": "0",
			"strategyFeeRate": null, "totalFeesCollected": null, "version": 100,
			"pool": {"id": %q, "name": "Pool"}
		}]}}`, strategyAddr, poolAddr))
	}})

	_, err := client.GetSkyStrategies(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), strategyAddr) || !strings.Contains(err.Error(), "depositedAssets") {
		t.Errorf("error %q should name the strategy id and the field", err.Error())
	}
}

func TestGetPools_NullAssetDecimalsRejected(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [{
			"id": %q, "name": "Pool",
			"monthlyApy": "0", "spotApy": "0",
			"assets": "1", "collateralValue": "2", "principalOut": "3", "tvl": "4",
			"asset": {"id": %q, "symbol": "USDC", "decimals": null},
			"syrupRouter": null
		}]}}`, poolAddr, usdcAddr))
	}})

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error for null asset decimals, got nil")
	}
	if !strings.Contains(err.Error(), poolAddr) || !strings.Contains(err.Error(), "asset.decimals") {
		t.Errorf("error %q should name the pool id and asset.decimals", err.Error())
	}
}

func TestGetPools_ZeroAssetDecimalsRejected(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [{
			"id": %q, "name": "Pool",
			"monthlyApy": "0", "spotApy": "0",
			"assets": "1", "collateralValue": "2", "principalOut": "3", "tvl": "4",
			"asset": {"id": %q, "symbol": "USDC", "decimals": 0},
			"syrupRouter": null
		}]}}`, poolAddr, usdcAddr))
	}})

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error for zero asset decimals, got nil")
	}
	if !strings.Contains(err.Error(), "asset.decimals") || !strings.Contains(err.Error(), "zero") {
		t.Errorf("error %q should report asset.decimals is zero", err.Error())
	}
}

func TestGetActiveLoans_NullCollateralDecimalsRejected(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"openTermLoans": [{
			"id": %q, "borrower": {"id": %q}, "state": "Active",
			"principalOwed": "1", "acmRatio": "1",
			"collateral": {
				"asset": "BTC", "assetAmount": "1", "assetValueUsd": "2",
				"decimals": null, "state": "Deposited", "custodian": "ANCHORAGE"
			},
			"loanMeta": null, "fundingPool": {"id": %q}
		}]}}`, loanAddr, borrowerAddr, poolAddr))
	}})

	_, err := client.GetActiveLoans(context.Background())
	if err == nil {
		t.Fatal("expected error for null collateral decimals, got nil")
	}
	if !strings.Contains(err.Error(), loanAddr) || !strings.Contains(err.Error(), "collateral.decimals") {
		t.Errorf("error %q should name the loan id and collateral.decimals", err.Error())
	}
}

func TestGetSkyStrategies_NullVersionRejected(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"skyStrategies": [{
			"id": %q, "state": "Active",
			"currentlyDeployed": "0", "depositedAssets": "1", "withdrawnAssets": "0",
			"strategyFeeRate": null, "totalFeesCollected": null, "version": null,
			"pool": {"id": %q, "name": "Pool"}
		}]}}`, strategyAddr, poolAddr))
	}})

	_, err := client.GetSkyStrategies(context.Background())
	if err == nil {
		t.Fatal("expected error for null version, got nil")
	}
	if !strings.Contains(err.Error(), strategyAddr) || !strings.Contains(err.Error(), "version") {
		t.Errorf("error %q should name the strategy id and version", err.Error())
	}
}

func TestGetSyrupGlobals_MalformedValue(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, `{"data": {"syrupGlobals": {
			"apy": "not-a-number", "collateralApy": "1", "poolApy": "2",
			"dripsYieldBoost": null, "tvl": "3"
		}}}`)
	}})

	_, err := client.GetSyrupGlobals(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "syrupGlobals") || !strings.Contains(err.Error(), "apy") {
		t.Errorf("error %q should name syrupGlobals and the field", err.Error())
	}
}

func TestGetSkyStrategies_HappyPath(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, query string, _ map[string]any) {
		if !strings.Contains(query, "skyStrategies") {
			t.Errorf("unexpected query: %s", query)
		}
		writeJSON(w, fmt.Sprintf(`{"data": {"skyStrategies": [{
			"id": %q, "state": "Active",
			"currentlyDeployed": "0", "depositedAssets": "9464548714891221",
			"withdrawnAssets": "9474661204598509", "strategyFeeRate": "100000",
			"totalFeesCollected": "1121557832133", "version": 100,
			"pool": {"id": %q, "name": "Syrup USDC"}
		}]}}`, strategyAddr, poolAddr))
	}})

	strategies, err := client.GetSkyStrategies(context.Background())
	if err != nil {
		t.Fatalf("GetSkyStrategies: %v", err)
	}
	if len(strategies) != 1 {
		t.Fatalf("len(strategies) = %d, want 1", len(strategies))
	}
	s := strategies[0]
	if strings.ToLower(s.Address.Hex()) != strategyAddr || strings.ToLower(s.PoolAddress.Hex()) != poolAddr {
		t.Errorf("addresses = %s/%s", s.Address.Hex(), s.PoolAddress.Hex())
	}
	if s.Version != 100 || s.State != "Active" {
		t.Errorf("version/state = %d/%s", s.Version, s.State)
	}
	if s.DepositedAssets.Cmp(big.NewInt(9464548714891221)) != 0 {
		t.Errorf("DepositedAssets = %s", s.DepositedAssets)
	}
	if s.StrategyFeeRate.Cmp(big.NewInt(100000)) != 0 {
		t.Errorf("StrategyFeeRate = %s", s.StrategyFeeRate)
	}
}

func TestGetSkyStrategies_NullFeeFields(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, fmt.Sprintf(`{"data": {"skyStrategies": [{
			"id": %q, "state": "Active",
			"currentlyDeployed": "0", "depositedAssets": "0", "withdrawnAssets": "0",
			"strategyFeeRate": null, "totalFeesCollected": null, "version": 100,
			"pool": {"id": %q, "name": "Pool"}
		}]}}`, strategyAddr, poolAddr))
	}})

	strategies, err := client.GetSkyStrategies(context.Background())
	if err != nil {
		t.Fatalf("GetSkyStrategies: %v", err)
	}
	if strategies[0].StrategyFeeRate != nil || strategies[0].TotalFeesCollected != nil {
		t.Errorf("fee fields = %v/%v, want nil/nil", strategies[0].StrategyFeeRate, strategies[0].TotalFeesCollected)
	}
}

func TestGetSyrupGlobals_HappyPath(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, query string, _ map[string]any) {
		if !strings.Contains(query, "syrupGlobals") {
			t.Errorf("unexpected query: %s", query)
		}
		writeJSON(w, `{"data": {"syrupGlobals": {
			"apy": "46314950526928033107296807949",
			"collateralApy": "11044228807145689488478201423",
			"poolApy": "35270730693993489373296467374",
			"dripsYieldBoost": "0",
			"tvl": "3563135115920200"
		}}}`)
	}})

	globals, err := client.GetSyrupGlobals(context.Background())
	if err != nil {
		t.Fatalf("GetSyrupGlobals: %v", err)
	}
	wantAPY, _ := new(big.Int).SetString("46314950526928033107296807949", 10)
	if globals.APY.Cmp(wantAPY) != 0 {
		t.Errorf("APY = %s, want %s", globals.APY, wantAPY)
	}
	if globals.TVL.Cmp(big.NewInt(3563135115920200)) != 0 {
		t.Errorf("TVL = %s", globals.TVL)
	}
	if globals.DripsYieldBoost.Sign() != 0 {
		t.Errorf("DripsYieldBoost = %s, want 0", globals.DripsYieldBoost)
	}
}

func TestGetSyrupGlobals_Null(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, `{"data": {"syrupGlobals": null}}`)
	}})

	_, err := client.GetSyrupGlobals(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "null") {
		t.Errorf("error %q should mention null", err.Error())
	}
}

func TestExecute_GraphQLErrorsEnvelope(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		calls.Add(1)
		writeJSON(w, `{"errors": [{"message": "Cannot query field \"fixedTermLoans\""}, {"message": "second error"}]}`)
	}})

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "graphql error") || !strings.Contains(err.Error(), "fixedTermLoans") {
		t.Errorf("error %q should surface the GraphQL messages", err.Error())
	}
	if !strings.Contains(err.Error(), "second error") {
		t.Errorf("error %q should join all messages", err.Error())
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1 (GraphQL errors must not be retried)", calls.Load())
	}
}

func TestExecute_HTTP500RetryThenFail(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("upstream exploded"))
	}))

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "server error (HTTP 500)") {
		t.Errorf("error = %q", err.Error())
	}
	if !strings.Contains(err.Error(), "upstream exploded") {
		t.Errorf("error %q should include the response body snippet", err.Error())
	}
	// MaxRetries=2 -> initial attempt + 2 retries.
	if calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", calls.Load())
	}
}

func TestExecute_HTTP429RetryThenSucceed(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
			writeJSON(w, fmt.Sprintf(`{"data": {"poolV2S": [%s]}}`, poolJSON(poolAddr)))
		}}.ServeHTTP(w, r)
	}))

	pools, err := client.GetPools(context.Background())
	if err != nil {
		t.Fatalf("GetPools: %v", err)
	}
	if len(pools) != 1 {
		t.Errorf("len(pools) = %d, want 1", len(pools))
	}
	if calls.Load() != 2 {
		t.Errorf("calls = %d, want 2 (one 429 then success)", calls.Load())
	}
}

func TestExecute_HTTP400NotRetried(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad request"))
	}))

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unexpected status 400") {
		t.Errorf("error = %q", err.Error())
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1 (4xx must not be retried)", calls.Load())
	}
}

func TestExecute_MalformedJSONNotRetried(t *testing.T) {
	var calls atomic.Int32
	client := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeJSON(w, `{not json`)
	}))

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "decoding GraphQL response") {
		t.Errorf("error = %q", err.Error())
	}
	if calls.Load() != 1 {
		t.Errorf("calls = %d, want 1", calls.Load())
	}
}

func TestNewClient_Defaults(t *testing.T) {
	client, err := NewClient(Config{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if client.endpoint != DefaultEndpoint {
		t.Errorf("endpoint = %q, want %q", client.endpoint, DefaultEndpoint)
	}
	if client.httpClient.Timeout != 15*time.Second {
		t.Errorf("timeout = %v, want 15s", client.httpClient.Timeout)
	}
	if client.retryConfig.MaxRetries != 3 {
		t.Errorf("maxRetries = %d, want 3", client.retryConfig.MaxRetries)
	}
}

func TestGetPools_SkipCapFailsHard(t *testing.T) {
	// A server that always returns full pages (e.g. ignoring skip) must hit
	// the skip cap and fail, not return a truncated set or loop forever.
	fullPage := make([]string, poolBatchSize)
	for i := range fullPage {
		fullPage[i] = poolJSON(fmt.Sprintf("0x%040x", i+1))
	}
	body := fmt.Sprintf(`{"data": {"poolV2S": [%s]}}`, strings.Join(fullPage, ","))

	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, body)
	}})

	_, err := client.GetPools(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "truncated") {
		t.Errorf("error = %q, want skip-cap refusal", err.Error())
	}
}

func TestGetPools_ContextCancelled(t *testing.T) {
	client := newTestClient(t, graphqlHandler{t: t, handleFunc: func(w http.ResponseWriter, _ string, _ map[string]any) {
		writeJSON(w, `{"data": {"poolV2S": []}}`)
	}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.GetPools(ctx)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
