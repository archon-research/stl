package coingecko

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	tests := []struct {
		name    string
		config  ClientConfig
		wantErr bool
	}{
		{
			name: "valid config",
			config: ClientConfig{
				APIKey: "test-api-key",
			},
			wantErr: false,
		},
		{
			name:    "missing API key",
			config:  ClientConfig{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := NewClient(tt.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewClient() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && client == nil {
				t.Error("NewClient() returned nil client")
			}
		})
	}
}

func TestClient_Name(t *testing.T) {
	client, _ := NewClient(ClientConfig{APIKey: "test"})
	if got := client.Name(); got != "coingecko" {
		t.Errorf("Name() = %v, want coingecko", got)
	}
}

func TestClient_SupportsHistorical(t *testing.T) {
	client, _ := NewClient(ClientConfig{APIKey: "test"})
	if !client.SupportsHistorical() {
		t.Error("SupportsHistorical() = false, want true")
	}
}

func TestClient_GetCurrentPrices(t *testing.T) {
	tests := []struct {
		name           string
		assetIDs       []string
		serverResponse string
		serverStatus   int
		wantCount      int
		wantErr        bool
	}{
		{
			name:     "empty asset IDs",
			assetIDs: []string{},
			wantErr:  false,
		},
		{
			name:     "single asset",
			assetIDs: []string{"ethereum"},
			serverResponse: `{
				"ethereum": {
					"usd": 3456.78,
					"usd_market_cap": 415123456789,
					"last_updated_at": 1704067200
				}
			}`,
			serverStatus: http.StatusOK,
			wantCount:    1,
			wantErr:      false,
		},
		{
			name:     "multiple assets",
			assetIDs: []string{"ethereum", "bitcoin"},
			serverResponse: `{
				"ethereum": {
					"usd": 3456.78,
					"usd_market_cap": 415123456789,
					"last_updated_at": 1704067200
				},
				"bitcoin": {
					"usd": 45678.90,
					"usd_market_cap": 895123456789,
					"last_updated_at": 1704067200
				}
			}`,
			serverStatus: http.StatusOK,
			wantCount:    2,
			wantErr:      false,
		},
		{
			name:           "server error",
			assetIDs:       []string{"ethereum"},
			serverResponse: `{"error": "internal error"}`,
			serverStatus:   http.StatusInternalServerError,
			wantErr:        true,
		},
		{
			name:           "rate limited",
			assetIDs:       []string{"ethereum"},
			serverResponse: `{"error": "rate limited"}`,
			serverStatus:   http.StatusTooManyRequests,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("x-cg-pro-api-key") != "test-api-key" {
					t.Error("API key header not set correctly")
				}
				w.WriteHeader(tt.serverStatus)
				_, _ = w.Write([]byte(tt.serverResponse))
			}))
			defer server.Close()

			client, err := NewClient(ClientConfig{
				APIKey:          "test-api-key",
				BaseURL:         server.URL,
				MaxRetries:      0, // Disable retries for testing
				RateLimitPerMin: 6000,
			})
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			prices, err := client.GetCurrentPrices(ctx, tt.assetIDs)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetCurrentPrices() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && len(prices) != tt.wantCount {
				t.Errorf("GetCurrentPrices() got %d prices, want %d", len(prices), tt.wantCount)
			}
		})
	}
}

func TestClient_GetHistoricalData(t *testing.T) {
	tests := []struct {
		name           string
		assetID        string
		serverResponse string
		serverStatus   int
		wantPrices     int
		wantVolumes    int
		wantErr        bool
	}{
		{
			name:    "valid response",
			assetID: "ethereum",
			serverResponse: `{
				"prices": [[1704067200000, 3456.78], [1704070800000, 3460.12]],
				"market_caps": [[1704067200000, 415123456789], [1704070800000, 415234567890]],
				"total_volumes": [[1704067200000, 12345678901], [1704070800000, 12456789012]]
			}`,
			serverStatus: http.StatusOK,
			wantPrices:   2,
			wantVolumes:  2,
			wantErr:      false,
		},
		{
			name:    "empty response",
			assetID: "ethereum",
			serverResponse: `{
				"prices": [],
				"market_caps": [],
				"total_volumes": []
			}`,
			serverStatus: http.StatusOK,
			wantPrices:   0,
			wantVolumes:  0,
			wantErr:      false,
		},
		{
			name:           "not found",
			assetID:        "invalid-coin",
			serverResponse: `{"error": "coin not found"}`,
			serverStatus:   http.StatusNotFound,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.serverStatus)
				_, _ = w.Write([]byte(tt.serverResponse))
			}))
			defer server.Close()

			client, err := NewClient(ClientConfig{
				APIKey:          "test-api-key",
				BaseURL:         server.URL,
				MaxRetries:      0,
				RateLimitPerMin: 6000,
			})
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			to := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

			data, err := client.GetHistoricalData(ctx, tt.assetID, from, to)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetHistoricalData() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(data.Prices) != tt.wantPrices {
					t.Errorf("got %d prices, want %d", len(data.Prices), tt.wantPrices)
				}
				if len(data.Volumes) != tt.wantVolumes {
					t.Errorf("got %d volumes, want %d", len(data.Volumes), tt.wantVolumes)
				}
			}
		})
	}
}

func TestClient_RetryLogic(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(simplePriceResponse{
			"ethereum": {USD: 3456.78, LastUpdated: 1704067200},
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         server.URL,
		MaxRetries:      3,
		InitialBackoff:  10 * time.Millisecond,
		BackoffFactor:   2.0,
		RateLimitPerMin: 6000,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	prices, err := client.GetCurrentPrices(ctx, []string{"ethereum"})
	if err != nil {
		t.Errorf("expected success after retries, got error: %v", err)
	}
	if len(prices) != 1 {
		t.Errorf("expected 1 price, got %d", len(prices))
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestClient_NonRetryableError(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": "invalid request"}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         server.URL,
		MaxRetries:      3,
		RateLimitPerMin: 6000,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.GetCurrentPrices(ctx, []string{"ethereum"})
	if err == nil {
		t.Error("expected error for bad request")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt for non-retryable error, got %d", attempts)
	}
}
