package main

import (
	"strings"
	"testing"
)

func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/stl")
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", "")
	t.Setenv("CHAIN_ID", "")
	t.Setenv("FROM_BLOCK", "")
	t.Setenv("PIN_BLOCK", "")
}

func TestParseConfig_DefaultsToMainnetAlchemyAndChainOne(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.rpcURL != "https://eth-mainnet.g.alchemy.com/v2/test-key" {
		t.Errorf("rpcURL = %q", cfg.rpcURL)
	}
	if cfg.bootstrap.ChainID != 1 {
		t.Errorf("chainID = %d, want 1", cfg.bootstrap.ChainID)
	}
	if cfg.dbURL != "postgres://user:pass@localhost:5432/stl" {
		t.Errorf("dbURL = %q", cfg.dbURL)
	}
}

func TestParseConfig_ComposesTheAlchemyURLWithoutADoubleSlash(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ALCHEMY_HTTP_URL", "https://base-mainnet.g.alchemy.com/v2/")

	cfg, err := parseConfig(nil)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if want := "https://base-mainnet.g.alchemy.com/v2/test-key"; cfg.rpcURL != want {
		t.Errorf("rpcURL = %q, want %q", cfg.rpcURL, want)
	}
}

func TestParseConfig_ExplicitRPCURLNeedsNoAPIKey(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("ALCHEMY_API_KEY", "")

	cfg, err := parseConfig([]string{"-rpc-url", "http://localhost:8545"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.rpcURL != "http://localhost:8545" {
		t.Errorf("rpcURL = %q, want the explicit endpoint", cfg.rpcURL)
	}
}

func TestParseConfig_FlagsOverrideTheEnvironment(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("FROM_BLOCK", "100")
	t.Setenv("PIN_BLOCK", "200")

	cfg, err := parseConfig([]string{
		"-db", "postgres://flag/db",
		"-chain-id", "8453",
		"-from", "300",
		"-pin", "400",
		"-finality-depth", "128",
		"-initial-window", "1000",
		"-min-window", "10",
		"-max-window", "5000",
		"-position-batch", "42",
	})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.dbURL != "postgres://flag/db" {
		t.Errorf("dbURL = %q", cfg.dbURL)
	}
	got := cfg.bootstrap
	if got.ChainID != 8453 || got.FromBlock != 300 || got.PinBlock != 400 ||
		got.FinalityDepth != 128 || got.InitialWindow != 1000 || got.MinWindow != 10 ||
		got.MaxWindow != 5000 || got.PositionBatch != 42 {
		t.Errorf("bootstrap config = %+v, want every flag to win over the env", got)
	}
}

func TestParseConfig_ReadsTheBlockOverridesFromTheEnvironment(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("CHAIN_ID", "8453")
	t.Setenv("FROM_BLOCK", "21688329")
	t.Setenv("PIN_BLOCK", "23000000")

	// Chain 8453 has no default finality depth and the depth has no env form.
	cfg, err := parseConfig([]string{"-finality-depth", "200"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.bootstrap.ChainID != 8453 {
		t.Errorf("chainID = %d, want 8453", cfg.bootstrap.ChainID)
	}
	if cfg.bootstrap.FromBlock != 21688329 || cfg.bootstrap.PinBlock != 23000000 {
		t.Errorf("from/pin = %d/%d, want 21688329/23000000", cfg.bootstrap.FromBlock, cfg.bootstrap.PinBlock)
	}
}

func TestParseConfig_RejectsAnIncompleteOrUnparseableEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		args    []string
		wantErr string
	}{
		{"no database url", map[string]string{"DATABASE_URL": ""}, nil, "database URL"},
		{"no alchemy key", map[string]string{"ALCHEMY_API_KEY": ""}, nil, "ALCHEMY_API_KEY"},
		{"unparseable chain id", map[string]string{"CHAIN_ID": "abc"}, nil, "CHAIN_ID"},
		{"unparseable from block", map[string]string{"FROM_BLOCK": "abc"}, nil, "FROM_BLOCK"},
		{"unparseable pin block", map[string]string{"PIN_BLOCK": "abc"}, nil, "PIN_BLOCK"},
		{"invalid bootstrap config", nil, []string{"-position-batch", "-1"}, "positionBatch"},
		{"off-mainnet chain without a finality depth", map[string]string{"CHAIN_ID": "8453"}, []string{"-rpc-url", "http://base.invalid"}, "finality depth"},
		{"unknown flag", nil, []string{"-nope"}, "nope"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			_, err := parseConfig(tt.args)
			if err == nil {
				t.Fatalf("expected an error for %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want it to name %q", err, tt.wantErr)
			}
		})
	}
}

func TestParseConfig_OffMainnetAcceptsAnExplicitFinalityDepth(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := parseConfig([]string{"-chain-id", "8453", "-finality-depth", "200", "-rpc-url", "http://base.invalid"})
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.bootstrap.FinalityDepth != 200 {
		t.Errorf("FinalityDepth = %d, want 200", cfg.bootstrap.FinalityDepth)
	}
}
