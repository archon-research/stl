package anchorage_tracker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClient_FetchPackages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify auth header.
		if got := r.Header.Get("Api-Access-Key"); got != "test-key" {
			t.Errorf("expected Api-Key=test-key, got %s", got)
		}

		if r.URL.Path != "/v2/collateral_management/packages" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{
			"data": [{
				"active": true,
				"packageId": "pkg-1",
				"pledgorId": "p1",
				"securedPartyId": "sp1",
				"state": "HEALTHY",
				"currentLtv": "0.68",
				"exposureValue": "50000000",
				"packageValue": "73000000",
				"ltvTimestamp": "2026-03-16T20:34:11Z",
				"collateralAssets": [{
					"asset": {"assetType": "BTC", "type": "AnchorageCustody"},
					"price": "74073.59",
					"quantity": "988.33",
					"weight": "1",
					"weightedValue": "73000000"
				}],
				"marginCall": {"action": "PARTIAL", "ltv": "0.8", "returnToLtv": "0.7", "warningLtv": "0.75"},
				"critical": {"action": "PARTIAL", "ltv": "0.9", "returnToLtv": "0.7", "warningLtv": "0.85", "defaultNotice": false},
				"marginReturn": {"action": "NONE", "ltv": "0.6", "returnToLtv": "0.7"}
			}],
			"page": {"next": null}
		}`)
	}))
	defer server.Close()

	client := NewClient(server.URL, "test-key")
	packages, err := client.FetchPackages(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(packages) != 1 {
		t.Fatalf("expected 1 package, got %d", len(packages))
	}

	pkg := packages[0]
	if pkg.PackageID != "pkg-1" {
		t.Errorf("expected package_id=pkg-1, got %s", pkg.PackageID)
	}
	if pkg.State != "HEALTHY" {
		t.Errorf("expected state=HEALTHY, got %s", pkg.State)
	}
	if len(pkg.CollateralAssets) != 1 {
		t.Fatalf("expected 1 collateral asset, got %d", len(pkg.CollateralAssets))
	}
	if pkg.CollateralAssets[0].Asset.AssetType != "BTC" {
		t.Errorf("expected BTC, got %s", pkg.CollateralAssets[0].Asset.AssetType)
	}
}

func TestClient_Pagination(t *testing.T) {
	callCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		w.Header().Set("Content-Type", "application/json")

		if callCount == 1 {
			// First page with a next cursor.
			fmt.Fprintf(w, `{
				"data": [{"packageId": "pkg-1", "active": true, "pledgorId": "p1", "securedPartyId": "sp1",
				  "state": "HEALTHY", "currentLtv": "0.5", "exposureValue": "100", "packageValue": "200",
				  "ltvTimestamp": "2026-01-01T00:00:00Z", "collateralAssets": [],
				  "marginCall": {"ltv": "0.8"}, "critical": {"ltv": "0.9"}, "marginReturn": {"ltv": "0.6"}}],
				"page": {"next": "http://%s/v2/collateral_management/packages?cursor=abc"}
			}`, r.Host)
			return
		}

		// Second page, no more pages.
		fmt.Fprint(w, `{
			"data": [{"packageId": "pkg-2", "active": true, "pledgorId": "p1", "securedPartyId": "sp1",
			  "state": "HEALTHY", "currentLtv": "0.5", "exposureValue": "100", "packageValue": "200",
			  "ltvTimestamp": "2026-01-01T00:00:00Z", "collateralAssets": [],
			  "marginCall": {"ltv": "0.8"}, "critical": {"ltv": "0.9"}, "marginReturn": {"ltv": "0.6"}}],
			"page": {"next": null}
		}`)
	}))
	defer server.Close()

	client := NewClient(server.URL, "key")
	packages, err := client.FetchPackages(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(packages) != 2 {
		t.Fatalf("expected 2 packages across 2 pages, got %d", len(packages))
	}
	if callCount != 2 {
		t.Errorf("expected 2 HTTP calls, got %d", callCount)
	}
}

func TestClient_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"error": "invalid api key"}`)
	}))
	defer server.Close()

	client := NewClient(server.URL, "bad-key")
	_, err := client.FetchPackages(context.Background())
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
}
