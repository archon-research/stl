package skydata

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func row(star, date string, overrides map[string]any) map[string]any {
	r := map[string]any{
		"star":             star,
		"date":             date,
		"treasury_balance": "48142491.085806286854722044",
		"assets":           "3224022323.40",
		"allocated_assets": "2718840719.96",
		"idle_assets":      "505181603.43",
		"debt":             "2642147590.40",
		"backstop_capital": "25000000",
	}
	for k, v := range overrides {
		if v == nil {
			delete(r, k)
			continue
		}
		r[k] = v
	}
	return r
}

// Pinned so the in-progress-day cutoff lands on the same date every run.
var testNow = time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)

// The returned accessor reads the recorded query under the same lock the
// handler writes it with: the handler runs on the server's goroutine, so
// handing back a bare pointer would race the assertion.
func newTestClient(t *testing.T, rows []map[string]any) (*Client, func() string) {
	t.Helper()
	var (
		mu        sync.Mutex
		lastQuery string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastQuery = r.URL.RawQuery
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"data": rows}); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{
		BaseURL: server.URL,
		Now:     func() time.Time { return testNow },
	})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	return client, func() string {
		mu.Lock()
		defer mu.Unlock()
		return lastQuery
	}
}

// The route returns every prime it holds, so the star list filters the response
// rather than the request — a prime STL does not track must not be recorded.
func TestFetchHistoryKeepsOnlyTheRequestedPrimes(t *testing.T) {
	client, _ := newTestClient(t, []map[string]any{
		row("spark", "2025-08-19", nil),
		row("obex", "2025-08-19", nil),
		row("grove", "2025-08-19", nil),
	})

	days, err := client.FetchHistory(context.Background(), []string{"spark", "grove"}, 365)
	if err != nil {
		t.Fatalf("FetchHistory() = %v", err)
	}

	for _, d := range days {
		if d.Star == "obex" {
			t.Errorf("kept %q, which was not requested", d.Star)
		}
	}
	if len(days) != 2 {
		t.Errorf("kept %d days, want 2", len(days))
	}
}

func TestFetchHistoryRequestsTheGivenWindow(t *testing.T) {
	client, query := newTestClient(t, []map[string]any{row("spark", "2025-08-19", nil)})

	if _, err := client.FetchHistory(context.Background(), []string{"spark"}, 90); err != nil {
		t.Fatalf("FetchHistory() = %v", err)
	}

	if got := query(); !strings.Contains(got, "days_ago=90") {
		t.Errorf("query = %q, want it to request days_ago=90", got)
	}
}

func TestFetchHistoryCarriesTreasuryBalanceAtFullPrecision(t *testing.T) {
	client, _ := newTestClient(t, []map[string]any{row("spark", "2025-08-19", nil)})

	days, err := client.FetchHistory(context.Background(), []string{"spark"}, 365)
	if err != nil {
		t.Fatalf("FetchHistory() = %v", err)
	}

	if got := days[0].TreasuryBalance; got != "48142491.085806286854722044" {
		t.Errorf("TreasuryBalance = %q, want the 18-decimal value unrounded", got)
	}
}

func TestFetchHistoryRejectsAnyAbsentFigure(t *testing.T) {
	for _, field := range []string{
		"treasury_balance", "assets", "allocated_assets", "idle_assets", "debt", "backstop_capital",
	} {
		t.Run(field, func(t *testing.T) {
			client, _ := newTestClient(t, []map[string]any{row("spark", "2025-08-19", map[string]any{field: nil})})

			_, err := client.FetchHistory(context.Background(), []string{"spark"}, 365)

			if err == nil || !strings.Contains(err.Error(), field) {
				t.Fatalf("error = %v, want it to name %q", err, field)
			}
		})
	}
}

func TestFetchHistoryRejectsARowWithNoDate(t *testing.T) {
	client, _ := newTestClient(t, []map[string]any{row("spark", "", nil)})

	if _, err := client.FetchHistory(context.Background(), []string{"spark"}, 365); err == nil {
		t.Fatal("FetchHistory() = nil, want an error")
	}
}

// The feed holds a year for every prime, so emptiness is a broken feed rather
// than an absence of history — recording nothing would leave a silent hole.
func TestFetchHistoryRejectsAnEmptyBody(t *testing.T) {
	client, _ := newTestClient(t, nil)

	if _, err := client.FetchHistory(context.Background(), []string{"spark"}, 365); err == nil {
		t.Fatal("FetchHistory() = nil, want an error")
	}
}

func TestFetchHistoryRejectsAResponseCoveringNoRequestedPrime(t *testing.T) {
	client, _ := newTestClient(t, []map[string]any{row("obex", "2025-08-19", nil)})

	if _, err := client.FetchHistory(context.Background(), []string{"spark"}, 365); err == nil {
		t.Fatal("FetchHistory() = nil, want an error")
	}
}

func TestFetchHistoryRejectsANonPositiveWindow(t *testing.T) {
	client, _ := newTestClient(t, []map[string]any{row("spark", "2025-08-19", nil)})

	if _, err := client.FetchHistory(context.Background(), []string{"spark"}, 0); err == nil {
		t.Fatal("FetchHistory() = nil, want an error")
	}
}

func TestNewClientRejectsARelativeBaseURL(t *testing.T) {
	if _, err := NewClient(ClientConfig{BaseURL: "internal"}); err == nil {
		t.Fatal("NewClient() = nil, want an error")
	}
}

// The current day's row moves as the day runs, and the store cannot revise it:
// the processing-version trigger is build-aware, so a second write for the same
// day under one deployment is discarded and the first reading is frozen.
func TestFetchHistoryWithholdsTheDayStillInProgress(t *testing.T) {
	client, _ := newTestClient(t, []map[string]any{
		row("spark", "2026-08-20", nil),
		row("spark", "2026-08-19", nil),
	})

	days, err := client.FetchHistory(context.Background(), []string{"spark"}, 3)
	if err != nil {
		t.Fatalf("FetchHistory() = %v", err)
	}

	if len(days) != 1 || days[0].Date != "2026-08-19" {
		t.Fatalf("kept %v, want only the completed day 2026-08-19", days)
	}
}

func TestFetchHistoryRejectsAWindowHoldingOnlyTheDayInProgress(t *testing.T) {
	// Distinct from "nothing published": the caller asked for a window and the
	// feed answered, so silently returning an empty set would read as no history.
	client, _ := newTestClient(t, []map[string]any{row("spark", "2026-08-20", nil)})

	if _, err := client.FetchHistory(context.Background(), []string{"spark"}, 1); err == nil {
		t.Fatal("FetchHistory() = nil, want an error")
	}
}

func positionRow(overrides map[string]any) map[string]any {
	r := map[string]any{
		"star":             "spark",
		"protocol":         "sparklend",
		"network":          "ethereum",
		"token_symbol":     "spUSDS",
		"token_name":       "Spark USDS",
		"address":          "0xc02ab1a5eaa8d1b114ef786d9bde108cd4364359",
		"assets":           "782710914.129541047405509005",
		"allocated_assets": "700000000.10",
		"idle_assets":      "82710914.02",
	}
	for k, v := range overrides {
		if v == nil {
			delete(r, k)
			continue
		}
		r[k] = v
	}
	return r
}

// newPositionsTestClient serves the positions envelope at /allocations/,
// which wraps results and pagination unlike the historic route's bare list.
// The returned accessor reads the recorded request target under the same lock
// the handler writes it with, mirroring newTestClient above.
func newPositionsTestClient(t *testing.T, rows []map[string]any, total int) (*Client, func() string) {
	t.Helper()
	var (
		mu       sync.Mutex
		lastPath string
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastPath = r.URL.RequestURI()
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		payload := map[string]any{
			"data": map[string]any{"results": rows, "pagination": map[string]any{"total": total}},
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Errorf("encoding response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(ClientConfig{BaseURL: server.URL, Now: func() time.Time { return testNow }})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}
	return client, func() string {
		mu.Lock()
		defer mu.Unlock()
		return lastPath
	}
}

func TestFetchPositionsCarriesEveryFieldUnrounded(t *testing.T) {
	client, requestURI := newPositionsTestClient(t, []map[string]any{positionRow(nil)}, 1)

	rows, err := client.FetchPositions(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPositions() = %v", err)
	}

	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	if got := requestURI(); got != "/allocations/?prime=spark&limit=1000" {
		t.Errorf("request = %q, want GET /allocations/ with prime=spark and limit=1000", got)
	}
	got := rows[0]
	if got.Assets != "782710914.129541047405509005" {
		t.Errorf("Assets = %q, want the 18-decimal value unrounded", got.Assets)
	}
	if got.ChainID == nil || *got.ChainID != 1 {
		t.Errorf("ChainID = %v, want 1 for network ethereum", got.ChainID)
	}
	if got.AllocatedAssets == nil || *got.AllocatedAssets != "700000000.10" {
		t.Errorf("AllocatedAssets = %v, want the upstream figure", got.AllocatedAssets)
	}
	if got.Star != "spark" {
		t.Errorf("Star = %q, want spark", got.Star)
	}
}

func TestFetchPositionsKeepsOmittedOptionalFieldsNil(t *testing.T) {
	client, _ := newPositionsTestClient(t, []map[string]any{positionRow(map[string]any{
		"token_name": nil, "allocated_assets": nil, "idle_assets": nil,
	})}, 1)

	rows, err := client.FetchPositions(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPositions() = %v", err)
	}

	got := rows[0]
	if got.TokenName != nil || got.AllocatedAssets != nil || got.IdleAssets != nil {
		t.Errorf("optional fields = (%v, %v, %v), want all nil — an omitted figure must not become zero",
			got.TokenName, got.AllocatedAssets, got.IdleAssets)
	}
}

func TestFetchPositionsRejectsAnyAbsentRequiredField(t *testing.T) {
	for _, field := range []string{"protocol", "network", "token_symbol", "address", "assets"} {
		t.Run(field, func(t *testing.T) {
			client, _ := newPositionsTestClient(t, []map[string]any{positionRow(map[string]any{field: nil})}, 1)

			_, err := client.FetchPositions(context.Background(), []string{"spark"})

			if err == nil {
				t.Fatalf("FetchPositions() = nil, want an error naming %q", field)
			}
			if !strings.Contains(err.Error(), field) {
				t.Errorf("error = %v, want it to name %q", err, field)
			}
		})
	}
}

func TestFetchPositionsRejectsAnEmptyResult(t *testing.T) {
	// The route answers an unknown star with 200 and an empty list, so an empty
	// result for a covered star cannot be told from a broken feed.
	client, _ := newPositionsTestClient(t, []map[string]any{}, 0)

	if _, err := client.FetchPositions(context.Background(), []string{"spark"}); err == nil {
		t.Fatal("FetchPositions() = nil, want an error")
	}
}

func TestFetchPositionsRejectsADuplicateRowIdentity(t *testing.T) {
	client, _ := newPositionsTestClient(t, []map[string]any{positionRow(nil), positionRow(nil)}, 2)

	if _, err := client.FetchPositions(context.Background(), []string{"spark"}); err == nil {
		t.Fatal("FetchPositions() = nil, want an error — a duplicate identity would silently conflict away at insert")
	}
}

// A casing difference must not hide a duplicate identity: (network,
// token_address) is the table's key, and upstream's casing is not
// trustworthy (see chainIDFor).
func TestFetchPositionsRejectsADuplicateRowIdentityAcrossCasing(t *testing.T) {
	client, _ := newPositionsTestClient(t, []map[string]any{
		positionRow(map[string]any{"network": "ethereum"}),
		positionRow(map[string]any{"network": "Ethereum"}),
	}, 2)

	if _, err := client.FetchPositions(context.Background(), []string{"spark"}); err == nil {
		t.Fatal("FetchPositions() = nil, want an error — a casing-only difference is still the same identity")
	}
}

// The feed's own vocabulary is lowercase; a casing difference upstream must
// still resolve rather than silently reading as an unmapped network.
func TestFetchPositionsMapsChainIDCaseInsensitively(t *testing.T) {
	client, _ := newPositionsTestClient(t, []map[string]any{positionRow(map[string]any{"network": "Ethereum"})}, 1)

	rows, err := client.FetchPositions(context.Background(), []string{"spark"})
	if err != nil {
		t.Fatalf("FetchPositions() = %v", err)
	}

	if rows[0].ChainID == nil || *rows[0].ChainID != 1 {
		t.Errorf("ChainID = %v, want 1 for network \"Ethereum\"", rows[0].ChainID)
	}
}

func TestFetchPositionsRejectsATruncatedPage(t *testing.T) {
	client, _ := newPositionsTestClient(t, []map[string]any{positionRow(nil)}, 59)

	if _, err := client.FetchPositions(context.Background(), []string{"spark"}); err == nil {
		t.Fatal("FetchPositions() = nil, want an error — a short page reads as rows that do not exist")
	}
}
