package main

import (
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// The complete valid environment; each case overrides one key, and an empty
	// value stands for "unset" (env.Require and env.Get both read empty as unset).
	valid := map[string]string{
		"CHAIN_ID":             "1",
		"S3_BUCKET":            "stl-sentinelstaging-ethereum-raw-89d540d0",
		"ALCHEMY_API_KEY":      "test-key",
		"ALCHEMY_HTTP_URL":     "",
		"BACKFILL_GOROUTINES":  "",
		"BACKFILL_PROBE_BATCH": "",
	}

	tests := []struct {
		name            string
		override        map[string]string
		want            config
		wantErrContains string
	}{
		{
			name: "defaults fill the optional knobs",
			want: config{
				bucket:     "stl-sentinelstaging-ethereum-raw-89d540d0",
				rpcURL:     defaultAlchemyHTTPURL + "/test-key",
				chainID:    1,
				goroutines: defaultGoroutines,
				probeBatch: defaultProbeBatch,
			},
		},
		{
			name:     "ALCHEMY_HTTP_URL overrides the mainnet default",
			override: map[string]string{"ALCHEMY_HTTP_URL": "https://eth-sepolia.g.alchemy.com/v2"},
			want: config{
				bucket:     "stl-sentinelstaging-ethereum-raw-89d540d0",
				rpcURL:     "https://eth-sepolia.g.alchemy.com/v2/test-key",
				chainID:    1,
				goroutines: defaultGoroutines,
				probeBatch: defaultProbeBatch,
			},
		},
		{
			name:     "tuning knobs are overridable",
			override: map[string]string{"BACKFILL_GOROUTINES": "64", "BACKFILL_PROBE_BATCH": "25"},
			want: config{
				bucket:     "stl-sentinelstaging-ethereum-raw-89d540d0",
				rpcURL:     defaultAlchemyHTTPURL + "/test-key",
				chainID:    1,
				goroutines: 64,
				probeBatch: 25,
			},
		},
		{
			name:            "missing CHAIN_ID",
			override:        map[string]string{"CHAIN_ID": ""},
			wantErrContains: "CHAIN_ID",
		},
		{
			name:            "missing S3_BUCKET",
			override:        map[string]string{"S3_BUCKET": ""},
			wantErrContains: "S3_BUCKET",
		},
		{
			name:            "missing ALCHEMY_API_KEY",
			override:        map[string]string{"ALCHEMY_API_KEY": ""},
			wantErrContains: "ALCHEMY_API_KEY",
		},
		{
			name:            "unparseable goroutine count",
			override:        map[string]string{"BACKFILL_GOROUTINES": "lots"},
			wantErrContains: "BACKFILL_GOROUTINES",
		},
		{
			name:            "zero goroutines",
			override:        map[string]string{"BACKFILL_GOROUTINES": "0"},
			wantErrContains: "must be positive",
		},
		{
			name:            "negative probe batch",
			override:        map[string]string{"BACKFILL_PROBE_BATCH": "-1"},
			wantErrContains: "must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for key, value := range valid {
				if override, ok := tc.override[key]; ok {
					value = override
				}
				t.Setenv(key, value)
			}

			got, err := loadConfig()

			if tc.wantErrContains != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q, got %+v", tc.wantErrContains, got)
				}
				if !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Errorf("error = %q, want it to contain %q", err, tc.wantErrContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("config = %+v, want %+v", got, tc.want)
			}
		})
	}
}
