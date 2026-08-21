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
// paths were requested so a test can assert a route was never reached.
func newTestClient(t *testing.T, routes map[string]any) (*Client, *[]string) {
	t.Helper()
	var requested []string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.Path)
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
