package star

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestClient(t *testing.T, srv *httptest.Server) *Client {
	t.Helper()
	c, err := NewClient(ClientConfig{BaseURL: srv.URL, HTTPClient: srv.Client()})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

func TestClient_FetchRiskCapital_ParsesRows(t *testing.T) {
	const body = `{
		"data": {"results": [
			{"star": "spark", "exposure": "100", "total_rc": "200", "financial_rrc": "50", "exposure_share": "0.5", "risk_tolerance_ratio": "0.85"},
			{"star": "grove", "exposure": "10", "total_rc": "20", "financial_rrc": "5", "exposure_share": "0.1", "risk_tolerance_ratio": "0.4"}
		]},
		"status": 200,
		"success": true
	}`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	rows, err := newTestClient(t, srv).FetchRiskCapital(context.Background())
	if err != nil {
		t.Fatalf("FetchRiskCapital: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("got %d rows, want 2", len(rows))
	}
	if rows[0].Star != "spark" || rows[0].Exposure != "100" || rows[0].TotalRC != "200" ||
		rows[0].FinancialRRC != "50" || rows[0].RiskToleranceRatio != "0.85" {
		t.Errorf("unexpected first row: %+v", rows[0])
	}
}

func TestClient_FetchRiskCapital_Errors(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		wantErr bool
		wantLen int
	}{
		{
			name:    "non-2xx is an error",
			status:  http.StatusBadGateway,
			body:    "upstream exploded",
			wantErr: true,
		},
		{
			name:    "success false is an error",
			status:  http.StatusOK,
			body:    `{"data": {"results": []}, "status": 500, "success": false}`,
			wantErr: true,
		},
		{
			name:    "status >= 400 in body is an error",
			status:  http.StatusOK,
			body:    `{"data": {"results": []}, "status": 503, "success": true}`,
			wantErr: true,
		},
		{
			name:    "invalid json is an error",
			status:  http.StatusOK,
			body:    `not json`,
			wantErr: true,
		},
		{
			name:    "null data yields no rows",
			status:  http.StatusOK,
			body:    `{"data": null, "status": 200, "success": true}`,
			wantErr: false,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			rows, err := newTestClient(t, srv).FetchRiskCapital(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("FetchRiskCapital() err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && len(rows) != tt.wantLen {
				t.Fatalf("got %d rows, want %d", len(rows), tt.wantLen)
			}
		})
	}
}

func TestNewClient_DefaultsBaseURL(t *testing.T) {
	c, err := NewClient(ClientConfig{})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	if c.baseURL != DefaultBaseURL {
		t.Errorf("baseURL = %q, want default %q", c.baseURL, DefaultBaseURL)
	}
}
