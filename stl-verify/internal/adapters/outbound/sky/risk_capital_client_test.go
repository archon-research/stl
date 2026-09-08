package sky

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func detailPayload(overrides map[string]any) map[string]any {
	data := map[string]any{
		"total_exposure":       "2098090654.811942249063867795",
		"total_rrc":            "17837860.43",
		"total_rc":             "48142491.08",
		"total_jrc":            "48142491.08",
		"total_src":            "0",
		"internal_jrc":         "48142491.08",
		"external_jrc":         "0",
		"tokenized_jrc":        "0",
		"internal_src":         "0",
		"external_src":         "0",
		"encumbrance_ratio":    "0.3705",
		"total_exposure_share": "0.0084",
		"epi_utilization":      "0",
		"spj_utilization":      "0",
	}
	for key, value := range overrides {
		if value == nil {
			delete(data, key)
			continue
		}
		data[key] = value
	}
	return map[string]any{"data": data, "status": 200, "success": true}
}

func listPayload(stars ...string) map[string]any {
	results := make([]map[string]any, 0, len(stars))
	for _, star := range stars {
		results = append(results, map[string]any{"star": star})
	}
	return map[string]any{"data": map[string]any{"results": results}, "status": 200, "success": true}
}

// newTestClient serves `routes` keyed by exact request path, recording the order
// full request targets (path+query) were requested so a test can assert a route
// was never reached, or assert the query string a route was reached with.
func newTestClient(t *testing.T, routes map[string]any) (*Client, *[]string) {
	t.Helper()
	var requested []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.String())
		payload, ok := routes[r.URL.Path]
		if !ok {
			w.WriteHeader(http.StatusInternalServerError)
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

func TestFetchPrimeSnapshotsSkipsAStarTheMonitorDoesNotCover(t *testing.T) {
	client, requested := newTestClient(t, map[string]any{
		"/primes/":       listPayload("spark"),
		"/primes/spark/": detailPayload(nil),
	})

	snapshots, err := client.FetchPrimeSnapshots(context.Background(), []string{"spark", "grove"})
	if err != nil {
		t.Fatalf("FetchPrimeSnapshots() = %v, want nil", err)
	}

	if len(snapshots) != 1 || snapshots[0].Star != "spark" {
		t.Fatalf("snapshots = %+v, want one for spark", snapshots)
	}
	for _, path := range *requested {
		if strings.Contains(path, "grove") {
			t.Errorf("requested %q; an uncovered star must never reach the detail route", path)
		}
	}
}

func TestFetchPrimeSnapshotsFailsWhenAListedStarHasNoName(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{"/primes/": listPayload("")})

	if _, err := client.FetchPrimeSnapshots(context.Background(), []string{"spark"}); err == nil {
		t.Fatal("FetchPrimeSnapshots() = nil, want an error")
	}
}

func TestFetchPrimeSnapshotsCarriesEighteenDecimalPrecision(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/":       listPayload("spark"),
		"/primes/spark/": detailPayload(nil),
	})

	snapshots, err := client.FetchPrimeSnapshots(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeSnapshots() = %v", err)
	}

	if got := snapshots[0].Exposure; got != "2098090654.811942249063867795" {
		t.Errorf("Exposure = %q, want the 18-decimal value unrounded", got)
	}
}

// Upstream quotes its numbers, but json.Number accepts a bare JSON number too;
// neither encoding may silently become zero.
func TestFetchPrimeSnapshotsAcceptsAnUnquotedNumber(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/":       listPayload("spark"),
		"/primes/spark/": detailPayload(map[string]any{"total_src": json.Number("12.5")}),
	})

	snapshots, err := client.FetchPrimeSnapshots(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeSnapshots() = %v", err)
	}

	if got := snapshots[0].SeniorRiskCapital; got != "12.5" {
		t.Errorf("SeniorRiskCapital = %q, want 12.5", got)
	}
}

func TestFetchPrimeSnapshotsRejectsAnyAbsentFigure(t *testing.T) {
	for _, field := range []string{
		"total_exposure", "total_rrc", "total_rc", "total_jrc", "total_src",
		"internal_jrc", "external_jrc", "tokenized_jrc", "internal_src", "external_src",
		"total_exposure_share", "epi_utilization", "spj_utilization",
	} {
		t.Run(field, func(t *testing.T) {
			client, _ := newTestClient(t, map[string]any{
				"/primes/":       listPayload("spark"),
				"/primes/spark/": detailPayload(map[string]any{field: nil}),
			})

			_, err := client.FetchPrimeSnapshots(context.Background(), []string{"spark"})

			if err == nil {
				t.Fatalf("FetchPrimeSnapshots() = nil, want an error naming %q", field)
			}
			if !strings.Contains(err.Error(), field) {
				t.Errorf("error = %v, want it to name %q", err, field)
			}
		})
	}
}

// A payload missing several figures must always blame the same one, or the same
// fault reads as a different bug on each run.
func TestFetchPrimeSnapshotsBlamesTheFirstAbsentFigureInOrder(t *testing.T) {
	for range 5 {
		client, _ := newTestClient(t, map[string]any{
			"/primes/": listPayload("spark"),
			"/primes/spark/": detailPayload(map[string]any{
				"total_rrc": nil, "total_rc": nil, "total_jrc": nil,
			}),
		})

		_, err := client.FetchPrimeSnapshots(context.Background(), []string{"spark"})

		if err == nil || !strings.Contains(err.Error(), "total_rrc") {
			t.Fatalf("error = %v, want it to name total_rrc every time", err)
		}
	}
}

// The one figure the monitor may omit: nil must survive as nil, because a zero
// would read as a prime with no encumbrance.
func TestFetchPrimeSnapshotsKeepsAnAbsentEncumbranceRatioNil(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/":       listPayload("spark"),
		"/primes/spark/": detailPayload(map[string]any{"encumbrance_ratio": nil}),
	})

	snapshots, err := client.FetchPrimeSnapshots(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeSnapshots() = %v", err)
	}

	if snapshots[0].EncumbranceRatio != nil {
		t.Errorf("EncumbranceRatio = %q, want nil", *snapshots[0].EncumbranceRatio)
	}
}

func TestNewClientRejectsABaseURLThatAlreadyNamesThePrimesRoute(t *testing.T) {
	// The dev env once supplied this shape; it requests /primes/primes/, which
	// upstream answers with a 500 that reads as an outage.
	_, err := NewClient(ClientConfig{BaseURL: "https://monitor.test/star-monitoring/risk-capital/primes/"})

	if err == nil {
		t.Fatal("NewClient() = nil, want an error")
	}
}

func TestNewClientRejectsARelativeBaseURL(t *testing.T) {
	if _, err := NewClient(ClientConfig{BaseURL: "star-monitoring/risk-capital"}); err == nil {
		t.Fatal("NewClient() = nil, want an error")
	}
}

func allocationsPayload(rows ...map[string]any) map[string]any {
	results := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		merged := map[string]any{
			"protocol":           "sparklend",
			"network":            "ethereum",
			"star":               "spark",
			"token_address":      "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359",
			"symbol":             "spUSDS",
			"name":               "Spark USDS",
			"loan_token_address": "0xdc035d45d973e3ec169d2276ddab16f1e407384f",
			"loan_token_symbol":  "USDS",
			"exposure":           "782710914.129541047405509005",
			"rrc":                "23308466.81",
			"crr":                "0.0447",
		}
		for key, value := range row {
			if value == nil {
				delete(merged, key)
				continue
			}
			merged[key] = value
		}
		results = append(results, merged)
	}
	return map[string]any{
		"data":    map[string]any{"results": results, "pagination": map[string]any{"total": len(results)}},
		"status":  200,
		"success": true,
	}
}

func TestFetchPrimeAllocationsCarriesEveryFieldUnrounded(t *testing.T) {
	client, requested := newTestClient(t, map[string]any{
		"/primes/spark/allocations/": allocationsPayload(map[string]any{}),
	})

	rows, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeAllocations() = %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if len(*requested) != 1 || !strings.Contains((*requested)[0], "limit=500") {
		t.Errorf("requested = %v, want the page fetched with limit=500", *requested)
	}
	got := rows[0]
	if got.Exposure != "782710914.129541047405509005" {
		t.Errorf("Exposure = %q, want the 18-decimal value unrounded", got.Exposure)
	}
	if got.CRR != "0.0447" {
		t.Errorf("CRR = %q, want the raw 0-1 fraction", got.CRR)
	}
	if got.ChainID == nil || *got.ChainID != 1 {
		t.Errorf("ChainID = %v, want 1 for network ethereum", got.ChainID)
	}
	if got.Star != "spark" {
		t.Errorf("Star = %q, want spark", got.Star)
	}
}

func TestFetchPrimeAllocationsKeepsAnUnmappableNetworkWithANilChainID(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/spark/allocations/": allocationsPayload(map[string]any{"network": "solana"}),
	})

	rows, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeAllocations() = %v — an unmapped network is a fact, not a fault", err)
	}

	if rows[0].ChainID != nil {
		t.Errorf("ChainID = %v, want nil", *rows[0].ChainID)
	}
	if rows[0].Network != "solana" {
		t.Errorf("Network = %q, want the vendor label kept verbatim", rows[0].Network)
	}
}

func TestFetchPrimeAllocationsKeepsOmittedOptionalFieldsNil(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/spark/allocations/": allocationsPayload(map[string]any{
			"name": nil, "loan_token_address": nil, "loan_token_symbol": nil,
		}),
	})

	rows, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeAllocations() = %v", err)
	}

	got := rows[0]
	if got.Name != nil || got.LoanTokenAddress != nil || got.LoanTokenSymbol != nil {
		t.Errorf("optional fields = (%v, %v, %v), want all nil", got.Name, got.LoanTokenAddress, got.LoanTokenSymbol)
	}
}

func TestFetchPrimeAllocationsRejectsAnyAbsentRequiredField(t *testing.T) {
	for _, field := range []string{
		"protocol", "network", "symbol", "token_address", "exposure", "rrc", "crr",
	} {
		t.Run(field, func(t *testing.T) {
			client, _ := newTestClient(t, map[string]any{
				"/primes/spark/allocations/": allocationsPayload(map[string]any{field: nil}),
			})

			_, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})

			if err == nil {
				t.Fatalf("FetchPrimeAllocations() = nil, want an error naming %q", field)
			}
			if !strings.Contains(err.Error(), field) {
				t.Errorf("error = %v, want it to name %q", err, field)
			}
		})
	}
}

func TestFetchPrimeAllocationsRejectsADuplicateRowIdentity(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/spark/allocations/": allocationsPayload(map[string]any{}, map[string]any{}),
	})

	_, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})

	if err == nil {
		t.Fatal("FetchPrimeAllocations() = nil, want an error — a duplicate identity would silently conflict away at insert")
	}
}

// A casing difference must not hide a duplicate identity: (network,
// token_address) is the table's key, and upstream's casing is not
// trustworthy (see chainIDFor).
func TestFetchPrimeAllocationsRejectsADuplicateRowIdentityAcrossCasing(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/spark/allocations/": allocationsPayload(
			map[string]any{"network": "ethereum"},
			map[string]any{"network": "Ethereum"},
		),
	})

	_, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})

	if err == nil {
		t.Fatal("FetchPrimeAllocations() = nil, want an error — a casing-only difference is still the same identity")
	}
}

// The monitor's own vocabulary is lowercase; a casing difference upstream
// must still resolve rather than silently reading as an unmapped network.
func TestFetchPrimeAllocationsMapsChainIDCaseInsensitively(t *testing.T) {
	client, _ := newTestClient(t, map[string]any{
		"/primes/spark/allocations/": allocationsPayload(map[string]any{"network": "Ethereum"}),
	})

	rows, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeAllocations() = %v", err)
	}

	if rows[0].ChainID == nil || *rows[0].ChainID != 1 {
		t.Errorf("ChainID = %v, want 1 for network \"Ethereum\"", rows[0].ChainID)
	}
}

func TestFetchPrimeAllocationsRejectsATruncatedPage(t *testing.T) {
	payload := allocationsPayload(map[string]any{})
	payload["data"].(map[string]any)["pagination"] = map[string]any{"total": 40}
	client, _ := newTestClient(t, map[string]any{"/primes/spark/allocations/": payload})

	_, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})

	if err == nil {
		t.Fatal("FetchPrimeAllocations() = nil, want an error — a short page reads as rows that do not exist")
	}
}

func TestFetchPrimeAllocationsAllowsAnEmptyBreakdown(t *testing.T) {
	// A zero-exposure star legitimately has nothing to break down; the exposure
	// cross-check lives with the caller, which holds the snapshot.
	client, _ := newTestClient(t, map[string]any{
		"/primes/spark/allocations/": allocationsPayload(),
	})

	rows, err := client.FetchPrimeAllocations(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPrimeAllocations() = %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("rows = %d, want 0", len(rows))
	}
}
