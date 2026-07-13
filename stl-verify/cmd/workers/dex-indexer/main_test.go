package main

import (
	"context"
	"strings"
	"testing"
)

// envSet clears every relevant env var and then sets only what the test
// cares about, so a var leaking from a prior test or the caller's shell
// cannot produce non-deterministic results.
func envSet(t *testing.T, vars map[string]string) {
	t.Helper()
	all := []string{
		"AWS_SQS_QUEUE_URL", "DATABASE_URL", "ALCHEMY_API_KEY", "ALCHEMY_HTTP_URL",
		"REDIS_ADDR", "REDIS_PASSWORD", "SQS_WAIT_TIME", "SQS_VISIBILITY_TIMEOUT",
		"CHAIN_ID", "S3_BUCKET", "DEPLOY_ENV", "DEX",
	}
	for _, k := range all {
		t.Setenv(k, "")
	}
	for k, v := range vars {
		t.Setenv(k, v)
	}
}

// happyEnv is every required var set to a valid value matching mainnet+staging.
func happyEnv() map[string]string {
	return map[string]string{
		"AWS_SQS_QUEUE_URL": "https://sqs.eu-west-1.amazonaws.com/123/test-queue",
		"DATABASE_URL":      "postgres://localhost/test",
		"ALCHEMY_API_KEY":   "testkey",
		"REDIS_ADDR":        "localhost:6379",
		"CHAIN_ID":          "1",
		"S3_BUCKET":         "stl-sentinelstaging-ethereum-raw",
		"DEPLOY_ENV":        "staging",
		"DEX":               "curve",
	}
}

// TestRun_MissingRequiredEnv verifies run fails fast with a clear, var-naming
// error when any single required env var is absent. No infra is touched:
// ParseConfig fails before any connection is attempted. One row per required
// var; the error must name the offending var so an operator can fix the deploy
// config from the message alone.
func TestRun_MissingRequiredEnv(t *testing.T) {
	tests := []struct {
		name    string
		omit    string
		wantSub string
	}{
		{name: "queue URL", omit: "AWS_SQS_QUEUE_URL", wantSub: "queue"},
		{name: "database URL", omit: "DATABASE_URL", wantSub: "database"},
		{name: "alchemy API key", omit: "ALCHEMY_API_KEY", wantSub: "ALCHEMY_API_KEY"},
		{name: "DEX selector", omit: "DEX", wantSub: "DEX"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vars := happyEnv()
			delete(vars, tt.omit)
			envSet(t, vars)

			err := run(context.Background(), nil)
			if err == nil {
				t.Fatalf("expected error when %s is absent, got nil", tt.omit)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("expected error to mention %q, got: %v", tt.wantSub, err)
			}
		})
	}
}

// TestRun_UnknownDex verifies that an unrecognized DEX value fails fast
// (before Bootstrap touches any infra) and names the valid registry keys so
// an operator can fix the deploy config directly from the error.
func TestRun_UnknownDex(t *testing.T) {
	vars := happyEnv()
	vars["DEX"] = "sushiswap"
	envSet(t, vars)

	err := run(context.Background(), nil)
	if err == nil {
		t.Fatal("expected error for unknown DEX, got nil")
	}
	if !strings.Contains(err.Error(), "sushiswap") {
		t.Errorf("expected error to mention the unknown value 'sushiswap', got: %v", err)
	}
	for _, want := range []string{"curve", "uniswap-v3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("expected error to list valid DEX %q, got: %v", want, err)
		}
	}
}

// TestRegistry_SelectsFactoryByKind asserts the registry maps cfg.Dex to a
// factory with matching Kind/ServiceName/MetricPrefix, without touching infra
// (Bootstrap is never called here).
func TestRegistry_SelectsFactoryByKind(t *testing.T) {
	tests := []struct {
		dex         string
		wantService string
		wantMetric  string
	}{
		{dex: "curve", wantService: "curve-indexer", wantMetric: "curve"},
		{dex: "uniswap-v3", wantService: "uniswap-v3-indexer", wantMetric: "uniswap_v3"},
	}
	registry := newRegistry()
	for _, tt := range tests {
		t.Run(tt.dex, func(t *testing.T) {
			f, ok := registry[tt.dex]
			if !ok {
				t.Fatalf("registry missing factory for Dex=%q", tt.dex)
			}
			if f.Kind() != tt.dex {
				t.Errorf("Kind() = %q, want %q", f.Kind(), tt.dex)
			}
			if f.ServiceName() != tt.wantService {
				t.Errorf("ServiceName() = %q, want %q", f.ServiceName(), tt.wantService)
			}
			if f.MetricPrefix() != tt.wantMetric {
				t.Errorf("MetricPrefix() = %q, want %q", f.MetricPrefix(), tt.wantMetric)
			}
		})
	}
}
