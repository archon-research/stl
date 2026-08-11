package main

import (
	"maps"
	"strings"
	"testing"
)

const testQueueURL = "https://sqs.eu-west-1.amazonaws.com/123/q.fifo"

// parseEnv returns the env vars parseConfig reads, with overrides applied. Every
// key parseConfig touches is listed so setEnv can blank the ones a case omits;
// otherwise a value from the developer's shell decides the result.
func parseEnv(overrides map[string]string) map[string]string {
	env := map[string]string{"AWS_SQS_QUEUE_URL": testQueueURL}
	maps.Copy(env, overrides)
	return env
}

func setEnv(t *testing.T, set map[string]string) {
	t.Helper()
	for _, key := range []string{
		"DATABASE_URL", "VAT_ADDRESS", "ETH_RPC_URL", "AWS_SQS_QUEUE_URL",
		"ALCHEMY_HTTP_URL", "ALCHEMY_API_KEY", "CHAIN_ID",
		"SQS_WAIT_TIME", "SQS_VISIBILITY_TIMEOUT",
	} {
		if _, has := set[key]; !has {
			t.Setenv(key, "")
		}
	}
	for k, v := range set {
		t.Setenv(k, v)
	}
}

func TestParseConfigResolvesRPCEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		envVars map[string]string
		wantRPC string
	}{
		{
			name:    "explicit -rpc flag",
			args:    []string{"-rpc", "http://localhost:8545"},
			envVars: parseEnv(map[string]string{"ETH_RPC_URL": "http://from-env:8545"}),
			wantRPC: "http://localhost:8545",
		},
		{
			name:    "ETH_RPC_URL when no flag",
			envVars: parseEnv(map[string]string{"ETH_RPC_URL": "http://from-env:8545"}),
			wantRPC: "http://from-env:8545",
		},
		{
			name: "composed from ALCHEMY_HTTP_URL and key",
			envVars: parseEnv(map[string]string{
				"ALCHEMY_HTTP_URL": "https://eth-mainnet.g.alchemy.com/v2",
				"ALCHEMY_API_KEY":  "abc123",
			}),
			wantRPC: "https://eth-mainnet.g.alchemy.com/v2/abc123",
		},
		{
			// A configured base URL ending in "/" must not yield a "//" before the key.
			name: "trailing slash trimmed from ALCHEMY_HTTP_URL",
			envVars: parseEnv(map[string]string{
				"ALCHEMY_HTTP_URL": "https://eth-mainnet.g.alchemy.com/v2/",
				"ALCHEMY_API_KEY":  "abc123",
			}),
			wantRPC: "https://eth-mainnet.g.alchemy.com/v2/abc123",
		},
		{
			name:    "default base URL when only the key is set",
			envVars: parseEnv(map[string]string{"ALCHEMY_API_KEY": "abc123"}),
			wantRPC: "https://eth-mainnet.g.alchemy.com/v2/abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, tt.envVars)

			cfg, err := parseConfig(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.rpcURL != tt.wantRPC {
				t.Errorf("rpcURL: got %q, want %q", cfg.rpcURL, tt.wantRPC)
			}
		})
	}
}

func TestParseConfigRejectsUnusableConfig(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		envVars   map[string]string
		wantError string
	}{
		{
			// The bug this guards: an empty key composed a URL ending in "/" that
			// dialled fine and then 401'd on every call.
			name:      "no RPC endpoint and no ALCHEMY_API_KEY",
			envVars:   parseEnv(nil),
			wantError: "no RPC endpoint",
		},
		{
			name:      "no queue URL",
			envVars:   map[string]string{"ETH_RPC_URL": "http://localhost:8545"},
			wantError: "queue URL not provided",
		},
		{
			name:      "malformed vat address",
			args:      []string{"-vat", "not-an-address"},
			envVars:   parseEnv(map[string]string{"ETH_RPC_URL": "http://localhost:8545"}),
			wantError: "invalid vat address",
		},
		{
			name: "non-numeric CHAIN_ID",
			envVars: parseEnv(map[string]string{
				"ETH_RPC_URL": "http://localhost:8545",
				"CHAIN_ID":    "mainnet",
			}),
			wantError: "parsing CHAIN_ID",
		},
		{
			name: "non-numeric SQS_WAIT_TIME",
			envVars: parseEnv(map[string]string{
				"ETH_RPC_URL":   "http://localhost:8545",
				"SQS_WAIT_TIME": "abc",
			}),
			wantError: "parsing SQS_WAIT_TIME",
		},
		{
			name: "non-numeric SQS_VISIBILITY_TIMEOUT",
			envVars: parseEnv(map[string]string{
				"ETH_RPC_URL":            "http://localhost:8545",
				"SQS_VISIBILITY_TIMEOUT": "abc",
			}),
			wantError: "parsing SQS_VISIBILITY_TIMEOUT",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, tt.envVars)

			_, err := parseConfig(tt.args)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantError)
			}
			if !strings.Contains(err.Error(), tt.wantError) {
				t.Errorf("expected error containing %q, got %q", tt.wantError, err.Error())
			}
		})
	}
}

func TestParseConfigSQSTimingPrecedence(t *testing.T) {
	tests := []struct {
		name           string
		args           []string
		envVars        map[string]string
		wantWait       int
		wantVisibility int
	}{
		{
			name:           "defaults when unset",
			envVars:        parseEnv(map[string]string{"ETH_RPC_URL": "http://localhost:8545"}),
			wantWait:       20,
			wantVisibility: 300,
		},
		{
			name: "env applies when no flag given",
			envVars: parseEnv(map[string]string{
				"ETH_RPC_URL":            "http://localhost:8545",
				"SQS_WAIT_TIME":          "5",
				"SQS_VISIBILITY_TIMEOUT": "60",
			}),
			wantWait:       5,
			wantVisibility: 60,
		},
		{
			name: "explicit flags beat their env vars",
			args: []string{"-wait", "7", "-visibility-timeout", "90"},
			envVars: parseEnv(map[string]string{
				"ETH_RPC_URL":            "http://localhost:8545",
				"SQS_WAIT_TIME":          "5",
				"SQS_VISIBILITY_TIMEOUT": "60",
			}),
			wantWait:       7,
			wantVisibility: 90,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setEnv(t, tt.envVars)

			cfg, err := parseConfig(tt.args)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.waitTime != tt.wantWait {
				t.Errorf("waitTime: got %d, want %d", cfg.waitTime, tt.wantWait)
			}
			if cfg.visibilityTimeout != tt.wantVisibility {
				t.Errorf("visibilityTimeout: got %d, want %d", cfg.visibilityTimeout, tt.wantVisibility)
			}
		})
	}
}
