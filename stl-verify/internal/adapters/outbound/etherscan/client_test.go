package etherscan

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
	if got := client.Name(); got != "etherscan" {
		t.Errorf("Name() = %v, want etherscan", got)
	}
}

func TestClient_GetBlockByNumber(t *testing.T) {
	tests := []struct {
		name           string
		blockNumber    int64
		serverResponse string
		serverStatus   int
		wantHash       string
		wantErr        bool
	}{
		{
			name:        "valid block",
			blockNumber: 21000000,
			serverResponse: `{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"number": "0x1406f40",
					"hash": "0xabc123def456",
					"timestamp": "0x65a12345"
				}
			}`,
			serverStatus: http.StatusOK,
			wantHash:     "0xabc123def456",
			wantErr:      false,
		},
		{
			name:        "block not found",
			blockNumber: 999999999,
			serverResponse: `{
				"jsonrpc": "2.0",
				"id": 1,
				"result": null
			}`,
			serverStatus: http.StatusOK,
			wantHash:     "",
			wantErr:      false,
		},
		{
			name:        "server error",
			blockNumber: 21000000,
			serverResponse: `{
				"status": "0",
				"message": "NOTOK",
				"result": "Internal server error"
			}`,
			serverStatus: http.StatusInternalServerError,
			wantErr:      true,
		},
		{
			name:        "rate limited",
			blockNumber: 21000000,
			serverResponse: `{
				"status": "0",
				"message": "NOTOK",
				"result": "Max rate limit reached"
			}`,
			serverStatus: http.StatusOK,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Verify the API key is in the query params
				if r.URL.Query().Get("apikey") != "test-api-key" {
					t.Error("API key not set correctly")
				}
				// Verify the chainid (V2 API requirement)
				if r.URL.Query().Get("chainid") != "1" {
					t.Errorf("chainid not set correctly, got %q", r.URL.Query().Get("chainid"))
				}
				// Verify the action
				if r.URL.Query().Get("action") != "eth_getBlockByNumber" {
					t.Error("Incorrect action")
				}
				w.WriteHeader(tt.serverStatus)
				_, _ = w.Write([]byte(tt.serverResponse))
			}))
			defer server.Close()

			client, err := NewClient(ClientConfig{
				APIKey:          "test-api-key",
				BaseURL:         server.URL,
				MaxRetries:      -1, // Disable retries for testing
				RateLimitPerSec: 1000,
			})
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			block, err := client.GetBlockByNumber(ctx, tt.blockNumber)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBlockByNumber() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.wantHash != "" {
				if block == nil {
					t.Error("expected block, got nil")
					return
				}
				if block.Hash != tt.wantHash {
					t.Errorf("got hash %s, want %s", block.Hash, tt.wantHash)
				}
			}
		})
	}
}

func TestClient_GetBlockByHash(t *testing.T) {
	tests := []struct {
		name           string
		blockHash      string
		serverResponse string
		serverStatus   int
		wantNumber     int64
		wantErr        bool
	}{
		{
			name:      "valid block",
			blockHash: "0xabc123def456",
			serverResponse: `{
				"jsonrpc": "2.0",
				"id": 1,
				"result": {
					"number": "0x1406f40",
					"hash": "0xabc123def456",
					"timestamp": "0x65a12345"
				}
			}`,
			serverStatus: http.StatusOK,
			wantNumber:   21000000,
			wantErr:      false,
		},
		{
			name:      "block not found",
			blockHash: "0xinvalidhash",
			serverResponse: `{
				"jsonrpc": "2.0",
				"id": 1,
				"result": null
			}`,
			serverStatus: http.StatusOK,
			wantNumber:   0,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("action") != "eth_getBlockByHash" {
					t.Error("Incorrect action")
				}
				w.WriteHeader(tt.serverStatus)
				_, _ = w.Write([]byte(tt.serverResponse))
			}))
			defer server.Close()

			client, err := NewClient(ClientConfig{
				APIKey:          "test-api-key",
				BaseURL:         server.URL,
				MaxRetries:      -1,
				RateLimitPerSec: 1000,
			})
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			block, err := client.GetBlockByHash(ctx, tt.blockHash)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBlockByHash() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && tt.wantNumber != 0 {
				if block == nil {
					t.Error("expected block, got nil")
					return
				}
				if block.Number != tt.wantNumber {
					t.Errorf("got number %d, want %d", block.Number, tt.wantNumber)
				}
			}
		})
	}
}

func TestClient_GetLatestBlockNumber(t *testing.T) {
	tests := []struct {
		name           string
		serverResponse string
		serverStatus   int
		wantNumber     int64
		wantErr        bool
	}{
		{
			name: "valid response",
			serverResponse: `{
				"jsonrpc": "2.0",
				"id": 1,
				"result": "0x1406f40"
			}`,
			serverStatus: http.StatusOK,
			wantNumber:   21000000,
			wantErr:      false,
		},
		{
			name: "API error",
			serverResponse: `{
				"jsonrpc": "2.0",
				"id": 1,
				"error": {
					"code": -32000,
					"message": "Internal error"
				}
			}`,
			serverStatus: http.StatusOK,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("action") != "eth_blockNumber" {
					t.Error("Incorrect action")
				}
				w.WriteHeader(tt.serverStatus)
				_, _ = w.Write([]byte(tt.serverResponse))
			}))
			defer server.Close()

			client, err := NewClient(ClientConfig{
				APIKey:          "test-api-key",
				BaseURL:         server.URL,
				MaxRetries:      -1,
				RateLimitPerSec: 1000,
			})
			if err != nil {
				t.Fatalf("failed to create client: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			number, err := client.GetLatestBlockNumber(ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetLatestBlockNumber() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && number != tt.wantNumber {
				t.Errorf("got %d, want %d", number, tt.wantNumber)
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
		_ = json.NewEncoder(w).Encode(proxyResponse{
			JSONRPC: "2.0",
			ID:      1,
			Result:  "0x1406f40",
		})
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         server.URL,
		MaxRetries:      3,
		InitialBackoff:  10 * time.Millisecond,
		BackoffFactor:   2.0,
		RateLimitPerSec: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	number, err := client.GetLatestBlockNumber(ctx)
	if err != nil {
		t.Errorf("expected success after retries, got error: %v", err)
	}
	if number != 21000000 {
		t.Errorf("expected 21000000, got %d", number)
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
		_, _ = w.Write([]byte(`{"status": "0", "message": "NOTOK", "result": "Invalid API Key"}`))
	}))
	defer server.Close()

	client, err := NewClient(ClientConfig{
		APIKey:          "test-api-key",
		BaseURL:         server.URL,
		MaxRetries:      3,
		RateLimitPerSec: 1000,
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.GetLatestBlockNumber(ctx)
	if err == nil {
		t.Error("expected error for bad request")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt for non-retryable error, got %d", attempts)
	}
}

func TestParseHexInt64(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{"0x1406f40", 21000000, false},
		{"1406f40", 21000000, false},
		{"0x0", 0, false},
		{"0x", 0, false},
		{"", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseHexInt64(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseHexInt64(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseHexInt64(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
