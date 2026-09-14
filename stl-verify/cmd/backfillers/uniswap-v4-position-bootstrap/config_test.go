package main

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
)

const mainnetRPCURL = "https://eth-mainnet.g.alchemy.com/v2"

func TestLoadConfig(t *testing.T) {
	// The complete valid environment; each case overrides one key, and an empty
	// value stands for "unset" (env.Require and env.GetInt64 both read empty as unset).
	valid := map[string]string{
		"CHAIN_ID":         "1",
		"ALCHEMY_API_KEY":  "test-key",
		"ALCHEMY_HTTP_URL": "",
		"FINALITY_DEPTH":   "",
		"INITIAL_WINDOW":   "",
		"MIN_WINDOW":       "",
		"MAX_WINDOW":       "",
		"POSITION_BATCH":   "",
	}

	tests := []struct {
		name            string
		override        map[string]string
		want            config
		wantErrContains string
	}{
		{
			name: "defaults fill every knob and the mainnet endpoint",
			want: config{
				rpcURL:    mainnetRPCURL + "/test-key",
				bootstrap: uniswapv4bootstrap.Config{ChainID: 1},
			},
		},
		{
			name: "the knobs come from the environment",
			override: map[string]string{
				"FINALITY_DEPTH": "128",
				"INITIAL_WINDOW": "1000",
				"MIN_WINDOW":     "10",
				"MAX_WINDOW":     "5000",
				"POSITION_BATCH": "50",
			},
			want: config{
				rpcURL: mainnetRPCURL + "/test-key",
				bootstrap: uniswapv4bootstrap.Config{
					ChainID: 1, FinalityDepth: 128, InitialWindow: 1000, MinWindow: 10, MaxWindow: 5000, PositionBatch: 50,
				},
			},
		},
		{
			name:     "an explicit endpoint composes without a double slash",
			override: map[string]string{"ALCHEMY_HTTP_URL": mainnetRPCURL + "/"},
			want: config{
				rpcURL:    mainnetRPCURL + "/test-key",
				bootstrap: uniswapv4bootstrap.Config{ChainID: 1},
			},
		},
		{
			name:            "the chain is required",
			override:        map[string]string{"CHAIN_ID": ""},
			wantErrContains: "CHAIN_ID",
		},
		{
			name:            "the API key is required",
			override:        map[string]string{"ALCHEMY_API_KEY": ""},
			wantErrContains: "ALCHEMY_API_KEY",
		},
		{
			name:            "another chain needs its own endpoint",
			override:        map[string]string{"CHAIN_ID": "8453"},
			wantErrContains: "ALCHEMY_HTTP_URL",
		},
		{
			name:            "an unparseable knob is refused rather than defaulted",
			override:        map[string]string{"MAX_WINDOW": "lots"},
			wantErrContains: "MAX_WINDOW",
		},
		{
			name:            "the knobs are validated together",
			override:        map[string]string{"MIN_WINDOW": "10", "MAX_WINDOW": "5"},
			wantErrContains: "maxWindow",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, valid, tt.override)

			got, err := loadConfig()

			if tt.wantErrContains != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
					t.Fatalf("loadConfig() error = %v, want it to name %q", err, tt.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadConfig() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("loadConfig() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func setEnv(t *testing.T, base, override map[string]string) {
	t.Helper()
	for key, value := range base {
		t.Setenv(key, value)
	}
	for key, value := range override {
		t.Setenv(key, value)
	}
}
