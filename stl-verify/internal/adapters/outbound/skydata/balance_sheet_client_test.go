package skydata

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
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

	client, err := NewClient(ClientConfig{BaseURL: server.URL})
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
