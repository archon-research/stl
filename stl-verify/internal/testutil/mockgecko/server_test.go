package mockgecko

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func get(t *testing.T, srv *httptest.Server, path string, withKey bool) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if err != nil {
		t.Fatalf("building request: %v", err)
	}
	if withKey {
		req.Header.Set("x-cg-pro-api-key", "any-key")
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	var buf [1 << 16]byte
	n, _ := resp.Body.Read(buf[:])
	return resp, buf[:n]
}

type rangeResponse struct {
	Prices       [][2]float64 `json:"prices"`
	MarketCaps   [][2]float64 `json:"market_caps"`
	TotalVolumes [][2]float64 `json:"total_volumes"`
}

func TestMarketChartRange_InclusiveAtBothEnds(t *testing.T) {
	srv := httptest.NewServer(NewServer(nil))
	t.Cleanup(srv.Close)

	// 24 hours must return 25 hourly points, matching the live API.
	_, body := get(t, srv, "/coins/ripple/market_chart/range?from=1756684800&to=1756771200", true)
	var r rangeResponse
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(r.Prices) != 25 || len(r.MarketCaps) != 25 || len(r.TotalVolumes) != 25 {
		t.Errorf("points = %d/%d/%d, want 25 each (inclusive at both ends)",
			len(r.Prices), len(r.MarketCaps), len(r.TotalVolumes))
	}
}

func TestMarketChartRange_DeterministicAcrossRequests(t *testing.T) {
	srv := httptest.NewServer(NewServer(nil))
	t.Cleanup(srv.Close)

	// Two windows sharing the seam hour must agree on its value — the seam is
	// exactly what the backfiller's chunking guards against double-counting.
	_, a := get(t, srv, "/coins/ripple/market_chart/range?from=1756684800&to=1756688400", true)
	_, b := get(t, srv, "/coins/ripple/market_chart/range?from=1756688400&to=1756692000", true)
	var ra, rb rangeResponse
	if err := json.Unmarshal(a, &ra); err != nil {
		t.Fatalf("decoding a: %v", err)
	}
	if err := json.Unmarshal(b, &rb); err != nil {
		t.Fatalf("decoding b: %v", err)
	}
	seamA, seamB := ra.Prices[len(ra.Prices)-1], rb.Prices[0]
	if seamA != seamB {
		t.Errorf("seam hour disagrees between windows: %v vs %v", seamA, seamB)
	}
}

func TestMarketChartRange_UnknownAssetIsEmptyNotAnError(t *testing.T) {
	srv := httptest.NewServer(NewServer(nil))
	t.Cleanup(srv.Close)

	resp, body := get(t, srv, "/coins/not-a-coin/market_chart/range?from=1756684800&to=1756771200", true)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 — the live API answers unknown ids with empty arrays", resp.StatusCode)
	}
	var r rangeResponse
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(r.Prices) != 0 {
		t.Errorf("expected empty prices for an unknown id, got %d points", len(r.Prices))
	}
}

func TestMarketChartRange_EntitlementHorizonTruncatesSilently(t *testing.T) {
	s := NewServer(nil)
	s.SetEntitlementStart(time.Unix(1756728000, 0).UTC()) // mid-window
	srv := httptest.NewServer(s)
	t.Cleanup(srv.Close)

	_, body := get(t, srv, "/coins/ripple/market_chart/range?from=1756684800&to=1756771200", true)
	var r rangeResponse
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(r.Prices) == 0 || len(r.Prices) >= 25 {
		t.Fatalf("expected a leading truncation, got %d points", len(r.Prices))
	}
	if got := int64(r.Prices[0][0] / 1000); got != 1756728000 {
		t.Errorf("first served point = %d, want the entitlement start 1756728000", got)
	}
}

func TestMissingKeyIsRefusedWith401(t *testing.T) {
	srv := httptest.NewServer(NewServer(nil))
	t.Cleanup(srv.Close)

	resp, _ := get(t, srv, "/simple/price?ids=ripple&vs_currencies=usd", false)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 without an API key", resp.StatusCode)
	}
}

func TestSimplePrice_ServesKnownIdsAndSkipsUnknown(t *testing.T) {
	srv := httptest.NewServer(NewServer(nil))
	t.Cleanup(srv.Close)

	_, body := get(t, srv, "/simple/price?ids=ripple,hyperliquid,not-a-coin&vs_currencies=usd", true)
	var r map[string]map[string]float64
	if err := json.Unmarshal(body, &r); err != nil {
		t.Fatalf("decoding: %v", err)
	}
	if len(r) != 2 {
		t.Fatalf("expected 2 known assets, got %d: %v", len(r), r)
	}
	xrp, hype := r["ripple"]["usd"], r["hyperliquid"]["usd"]
	if xrp < 1.0 || xrp > 2.0 {
		t.Errorf("ripple price %v outside its realistic band", xrp)
	}
	if hype < 60 || hype > 110 {
		t.Errorf("hyperliquid price %v outside its realistic band", hype)
	}
	if r["ripple"]["usd_market_cap"] < 5e10 {
		t.Errorf("ripple market cap %v not at realistic scale", r["ripple"]["usd_market_cap"])
	}
}
