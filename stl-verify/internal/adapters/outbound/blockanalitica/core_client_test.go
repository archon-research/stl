package blockanalitica

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func marketRow(overrides map[string]any) map[string]any {
	row := map[string]any{
		"network":                "ethereum",
		"protocol":               "sparklend",
		"market_uid":             "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359",
		"market_symbol":          "spUSDS",
		"loan_token_symbol":      "USDS",
		"loan_token_address":     "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
		"date":                   "2026-09-17",
		"n_scenarios":            10000,
		"horizon_days":           15,
		"effective_horizon_days": 1,
		"tot_supply_usd":         "1309071023.791548000000000000",
		"prob_no_bad_debt":       "0.694299999999999900",
		"crr_el":                 "0.001214738329317449",
		"crr_var":                "0.010683027397348378",
		"crr_es":                 "0.030818438149105926",
		"crr_el_se":              "0.000077018244182130",
		"crr_var_se":             "0.000694722112615200",
		"crr_es_se":              "0.001220932121971280",
		"crr_floor":              "0.020000000000000000",
		"external_flow_enabled":  true,
	}
	applyOverrides(row, overrides)
	return row
}

func vaultRow(overrides map[string]any) map[string]any {
	row := map[string]any{
		"network":            "base",
		"protocol":           "morpho",
		"vault_address":      "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9",
		"vault_symbol":       "steakUSDC",
		"vault_name":         "Steakhouse Prime USDC",
		"version":            "v2",
		"loan_token_symbol":  "USDC",
		"loan_token_address": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
		"method":             "model",
		"date":               "2026-09-17",
		"n_markets":          5,
		"total_assets_usd":   "434521303.920751150000000000",
		"idle_usd":           "3201.013246119022400000",
		"crr_el":             "0.000288698101040592",
		"crr_el_se":          "0.000028834308921715",
		"crr_es":             "0.009880342305281255",
	}
	applyOverrides(row, overrides)
	return row
}

// applyOverrides sets each key, deleting one whose override is nil so a test
// can simulate an omitted field.
func applyOverrides(row, overrides map[string]any) {
	for key, value := range overrides {
		if value == nil {
			delete(row, key)
			continue
		}
		row[key] = value
	}
}

func overviewPayload(markets, vaults []map[string]any) map[string]any {
	return map[string]any{
		"data":    map[string]any{"markets": markets, "vaults": vaults},
		"status":  200,
		"success": true,
	}
}

// newTestClient serves `routes` keyed by exact request path, recording every
// request target so a test can assert what was asked for.
func newTestClient(t *testing.T, routes map[string]any) (*Client, *[]string) {
	t.Helper()
	var requested []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.String())
		payload, ok := routes[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{BaseURL: server.URL, MaxRetries: 1})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	return client, &requested
}

func fetch(t *testing.T, routes map[string]any) (markets int, vaults int, err error) {
	t.Helper()
	client, _ := newTestClient(t, routes)
	overview, err := client.FetchOverview(context.Background())
	return len(overview.Markets), len(overview.Vaults), err
}

// fetchOne serves one market and one vault and returns the parsed pair.
func fetchOne(t *testing.T, market, vault map[string]any) (outbound.ReferenceCoreMarketRow, outbound.ReferenceCoreVaultRow) {
	t.Helper()
	client, _ := newTestClient(t, map[string]any{
		"/overview/": overviewPayload([]map[string]any{market}, []map[string]any{vault}),
	})
	overview, err := client.FetchOverview(context.Background())
	if err != nil {
		t.Fatalf("FetchOverview() = %v", err)
	}
	if len(overview.Markets) != 1 || len(overview.Vaults) != 1 {
		t.Fatalf("markets/vaults = %d/%d, want 1/1", len(overview.Markets), len(overview.Vaults))
	}
	return overview.Markets[0], overview.Vaults[0]
}

func TestFetchOverviewSendsNoDateParameter(t *testing.T) {
	client, requested := newTestClient(t, map[string]any{
		"/overview/": overviewPayload([]map[string]any{marketRow(nil)}, []map[string]any{vaultRow(nil)}),
	})

	if _, err := client.FetchOverview(context.Background()); err != nil {
		t.Fatalf("FetchOverview() = %v", err)
	}
	if len(*requested) != 1 || strings.Contains((*requested)[0], "date=") {
		t.Errorf("requested %v; the feed ignores ?date=, so none must be sent", *requested)
	}
}

func TestFetchOverviewCarriesTheMarketIdentityFields(t *testing.T) {
	m, _ := fetchOne(t, marketRow(nil), vaultRow(nil))

	if m.Network != "ethereum" || m.Protocol != "sparklend" || m.MarketUID != "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359" {
		t.Errorf("identity = %s/%s/%s, want ethereum/sparklend/0xc02ab…", m.Network, m.Protocol, m.MarketUID)
	}
	if m.ChainID == nil || *m.ChainID != 1 {
		t.Errorf("ChainID = %v, want 1", m.ChainID)
	}
	if m.NScenarios != 10000 || m.HorizonDays != 15 || m.EffectiveHorizonDays != 1 {
		t.Errorf("scalars = %d/%d/%d, want 10000/15/1", m.NScenarios, m.HorizonDays, m.EffectiveHorizonDays)
	}
	if !m.ExternalFlowEnabled || m.Date != "2026-09-17" {
		t.Errorf("ExternalFlowEnabled/Date = %v/%s, want true/2026-09-17", m.ExternalFlowEnabled, m.Date)
	}
}

func TestFetchOverviewCarriesEveryMarketFigureUnrounded(t *testing.T) {
	m, _ := fetchOne(t, marketRow(nil), vaultRow(nil))

	for name, got := range map[string]string{
		"tot_supply_usd":   m.TotalSupply,
		"prob_no_bad_debt": m.ProbNoBadDebt,
		"crr_el":           m.CRREL,
		"crr_var":          m.CRRVaR,
		"crr_es":           m.CRRES,
		"crr_el_se":        m.CRRELSE,
		"crr_var_se":       m.CRRVaRSE,
		"crr_es_se":        m.CRRESSE,
		"crr_floor":        m.CRRFloor,
	} {
		if want := marketRow(nil)[name].(string); got != want {
			t.Errorf("%s = %s, want the 18-decimal literal %s unrounded", name, got, want)
		}
	}
}

func TestFetchOverviewCarriesEveryVaultFieldUnrounded(t *testing.T) {
	_, v := fetchOne(t, marketRow(nil), vaultRow(nil))

	if v.Network != "base" || v.Protocol != "morpho" || v.VaultAddress != "0xbeef0e0834849acc03f0089f01f4f1eeb06873c9" {
		t.Errorf("identity = %s/%s/%s, want base/morpho/0xbeef0e…", v.Network, v.Protocol, v.VaultAddress)
	}
	if v.ChainID == nil || *v.ChainID != 8453 {
		t.Errorf("ChainID = %v, want 8453", v.ChainID)
	}
	if v.TotalAssets != "434521303.920751150000000000" || v.IdleAssets != "3201.013246119022400000" {
		t.Errorf("TotalAssets/IdleAssets = %s/%s, want the literals unrounded", v.TotalAssets, v.IdleAssets)
	}
	if v.CRRELSE == nil || *v.CRRELSE != "0.000028834308921715" || v.CRRES == nil || *v.CRRES != "0.009880342305281255" {
		t.Errorf("CRRELSE/CRRES = %v/%v, want the literals unrounded", v.CRRELSE, v.CRRES)
	}
	if v.Version != "v2" || v.Method != "model" || v.NMarkets != 5 {
		t.Errorf("Version/Method/NMarkets = %s/%s/%d, want v2/model/5", v.Version, v.Method, v.NMarkets)
	}
}

func TestFetchOverviewKeepsAnOverrideVaultsAbsentFiguresNil(t *testing.T) {
	_, v := fetchOne(t, marketRow(nil), vaultRow(map[string]any{"method": "override", "crr_el_se": nil, "crr_es": nil}))

	if v.Method != "override" || v.CRRELSE != nil || v.CRRES != nil {
		t.Errorf("override vault = method %q, se %v, es %v; want override with both nil", v.Method, v.CRRELSE, v.CRRES)
	}
}

func TestFetchOverviewRejectsAModelVaultWithoutASimulationFigure(t *testing.T) {
	for _, field := range []string{"crr_el_se", "crr_es"} {
		t.Run(field, func(t *testing.T) {
			_, _, err := fetch(t, map[string]any{
				"/overview/": overviewPayload(
					[]map[string]any{marketRow(nil)},
					[]map[string]any{vaultRow(map[string]any{field: nil})}),
			})
			if err == nil || !strings.Contains(err.Error(), `"`+field+`"`) {
				t.Fatalf("FetchOverview() = %v, want an error naming %q on a method=model vault", err, field)
			}
		})
	}
}

func TestFetchOverviewAcceptsAnUnquotedNumber(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/overview/": overviewPayload(
			[]map[string]any{marketRow(map[string]any{"crr_el": 0.0012})},
			[]map[string]any{vaultRow(nil)}),
	})

	overview, err := client.FetchOverview(context.Background())
	if err != nil {
		t.Fatalf("FetchOverview() = %v", err)
	}
	if overview.Markets[0].CRREL != "0.0012" {
		t.Errorf("CRREL = %s, want 0.0012", overview.Markets[0].CRREL)
	}
}

func TestFetchOverviewKeepsAnUnmappableNetworkWithANilChainID(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/overview/": overviewPayload(
			[]map[string]any{marketRow(map[string]any{"network": "plasma"})},
			[]map[string]any{vaultRow(nil)}),
	})

	overview, err := client.FetchOverview(context.Background())
	if err != nil {
		t.Fatalf("FetchOverview() = %v", err)
	}
	if overview.Markets[0].Network != "plasma" || overview.Markets[0].ChainID != nil {
		t.Errorf("row = %s/%v, want plasma with a nil chain id", overview.Markets[0].Network, overview.Markets[0].ChainID)
	}
}

func TestFetchOverviewMapsChainIDCaseInsensitively(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/overview/": overviewPayload(
			[]map[string]any{marketRow(map[string]any{"network": "Robinhood"})},
			[]map[string]any{vaultRow(nil)}),
	})

	overview, err := client.FetchOverview(context.Background())
	if err != nil {
		t.Fatalf("FetchOverview() = %v", err)
	}
	if overview.Markets[0].ChainID == nil || *overview.Markets[0].ChainID != 4663 {
		t.Errorf("ChainID = %v, want 4663", overview.Markets[0].ChainID)
	}
}

func TestFetchOverviewRejectsAnyAbsentMarketField(t *testing.T) {
	for _, field := range []string{
		"network", "protocol", "market_uid", "market_symbol", "loan_token_symbol", "loan_token_address",
		"date", "n_scenarios", "horizon_days", "effective_horizon_days", "tot_supply_usd", "prob_no_bad_debt",
		"crr_el", "crr_var", "crr_es", "crr_el_se", "crr_var_se", "crr_es_se", "crr_floor", "external_flow_enabled",
	} {
		t.Run(field, func(t *testing.T) {
			_, _, err := fetch(t, map[string]any{
				"/overview/": overviewPayload(
					[]map[string]any{marketRow(map[string]any{field: nil})},
					[]map[string]any{vaultRow(nil)}),
			})
			if err == nil || !strings.Contains(err.Error(), `"`+field+`"`) {
				t.Fatalf("FetchOverview() = %v, want an error naming %q", err, field)
			}
		})
	}
}

func TestFetchOverviewRejectsAnyAbsentVaultField(t *testing.T) {
	for _, field := range []string{
		"network", "protocol", "vault_address", "vault_symbol", "vault_name", "version", "loan_token_symbol",
		"loan_token_address", "method", "date", "n_markets", "total_assets_usd", "idle_usd", "crr_el",
	} {
		t.Run(field, func(t *testing.T) {
			_, _, err := fetch(t, map[string]any{
				"/overview/": overviewPayload(
					[]map[string]any{marketRow(nil)},
					[]map[string]any{vaultRow(map[string]any{field: nil})}),
			})
			if err == nil || !strings.Contains(err.Error(), `"`+field+`"`) {
				t.Fatalf("FetchOverview() = %v, want an error naming %q", err, field)
			}
		})
	}
}

func TestFetchOverviewBlamesTheFirstAbsentFieldInOrder(t *testing.T) {
	_, _, err := fetch(t, map[string]any{
		"/overview/": overviewPayload(
			[]map[string]any{marketRow(map[string]any{"crr_es": nil, "market_symbol": nil})},
			[]map[string]any{vaultRow(nil)}),
	})
	if err == nil || !strings.Contains(err.Error(), `"market_symbol"`) {
		t.Fatalf("FetchOverview() = %v, want the earlier field market_symbol blamed", err)
	}
}

func TestFetchOverviewRejectsADuplicateMarketIdentityAcrossCasing(t *testing.T) {
	_, _, err := fetch(t, map[string]any{
		"/overview/": overviewPayload(
			[]map[string]any{
				marketRow(nil),
				marketRow(map[string]any{"market_uid": strings.ToUpper("0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359"), "market_symbol": "other"}),
			},
			[]map[string]any{vaultRow(nil)}),
	})
	if err == nil || !strings.Contains(err.Error(), "repeats market identity") {
		t.Fatalf("FetchOverview() = %v, want a duplicate-identity error", err)
	}
}

func TestFetchOverviewAllowsOneMarketUIDUnderTwoProtocols(t *testing.T) {
	markets, _, err := fetch(t, map[string]any{
		"/overview/": overviewPayload(
			[]map[string]any{
				marketRow(map[string]any{"protocol": "anchorage", "market_uid": "0x0000000000000000000000000000000000000000"}),
				marketRow(map[string]any{"protocol": "galaxy", "market_uid": "0x0000000000000000000000000000000000000000"}),
			},
			[]map[string]any{vaultRow(nil)}),
	})
	if err != nil {
		t.Fatalf("FetchOverview() = %v, want nil: protocol is part of the identity", err)
	}
	if markets != 2 {
		t.Errorf("markets = %d, want 2", markets)
	}
}

func TestFetchOverviewRejectsADuplicateVaultIdentity(t *testing.T) {
	_, _, err := fetch(t, map[string]any{
		"/overview/": overviewPayload(
			[]map[string]any{marketRow(nil)},
			[]map[string]any{vaultRow(nil), vaultRow(map[string]any{"vault_symbol": "other"})}),
	})
	if err == nil || !strings.Contains(err.Error(), "repeats vault identity") {
		t.Fatalf("FetchOverview() = %v, want a duplicate-identity error", err)
	}
}

func TestFetchOverviewRejectsAnUnsuccessfulEnvelope(t *testing.T) {
	_, _, err := fetch(t, map[string]any{
		"/overview/": map[string]any{"data": map[string]any{}, "status": 200, "success": false},
	})
	if err == nil || !strings.Contains(err.Error(), "success=false") {
		t.Fatalf("FetchOverview() = %v, want an envelope error", err)
	}
}

func TestFetchOverviewAllowsAnEmptyOverview(t *testing.T) {
	markets, vaults, err := fetch(t, map[string]any{"/overview/": overviewPayload(nil, nil)})
	if err != nil {
		t.Fatalf("FetchOverview() = %v; emptiness is the service's call, not the transport's", err)
	}
	if markets != 0 || vaults != 0 {
		t.Errorf("markets/vaults = %d/%d, want 0/0", markets, vaults)
	}
}

func TestFetchOverviewPropagatesATransportFailure(t *testing.T) {
	if _, _, err := fetch(t, map[string]any{}); err == nil {
		t.Fatal("FetchOverview() = nil, want an error for a 404")
	}
}

func TestNewClientRejectsABaseURLThatAlreadyNamesTheOverviewRoute(t *testing.T) {
	if _, err := NewClient(ClientConfig{BaseURL: "https://core.data.blockanalitica.com/core/overview"}); err == nil {
		t.Fatal("NewClient() = nil, want an error")
	}
}

func TestNewClientRejectsARelativeBaseURL(t *testing.T) {
	if _, err := NewClient(ClientConfig{BaseURL: "core.data.blockanalitica.com/core"}); err == nil {
		t.Fatal("NewClient() = nil, want an error")
	}
}

func TestNewClientDefaultsToTheUpstreamFeed(t *testing.T) {
	client, err := NewClient(ClientConfig{})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	if client.baseURL != defaultBaseURL {
		t.Errorf("baseURL = %s, want %s", client.baseURL, defaultBaseURL)
	}
}
