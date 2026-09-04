package uniswapv4bootstrap

import (
	"strings"
	"testing"
)

func TestConfigWithDefaults_FillsEveryUnsetKnob(t *testing.T) {
	got := Config{ChainID: 1}.withDefaults()

	if got.FinalityDepth != DefaultFinalityDepth {
		t.Errorf("FinalityDepth = %d, want %d", got.FinalityDepth, DefaultFinalityDepth)
	}
	if got.InitialWindow != DefaultInitialWindow {
		t.Errorf("InitialWindow = %d, want %d", got.InitialWindow, DefaultInitialWindow)
	}
	if got.MinWindow != DefaultMinWindow {
		t.Errorf("MinWindow = %d, want %d", got.MinWindow, DefaultMinWindow)
	}
	if got.MaxWindow != DefaultMaxWindow {
		t.Errorf("MaxWindow = %d, want %d", got.MaxWindow, DefaultMaxWindow)
	}
	if got.PositionBatch != DefaultPositionBatch {
		t.Errorf("PositionBatch = %d, want %d", got.PositionBatch, DefaultPositionBatch)
	}
}

func TestConfigWithDefaults_KeepsExplicitValues(t *testing.T) {
	cfg := Config{
		ChainID:       1,
		FinalityDepth: 128,
		InitialWindow: 10,
		MinWindow:     2,
		MaxWindow:     20,
		PositionBatch: 3,
	}
	if got := cfg.withDefaults(); got != cfg {
		t.Errorf("withDefaults() = %+v, want it unchanged", got)
	}
}

func TestConfigValidate_RejectsUnusableSettings(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"non-positive chain", func(c *Config) { c.ChainID = 0 }, "chainID"},
		{"negative finality depth", func(c *Config) { c.FinalityDepth = -1 }, "finality depth"},
		{"negative from block", func(c *Config) { c.FromBlock = -5 }, "fromBlock"},
		{"negative pin block", func(c *Config) { c.PinBlock = -5 }, "pinBlock"},
		{"pin below from", func(c *Config) { c.FromBlock = 100; c.PinBlock = 99 }, "fromBlock"},
		{"zero min window", func(c *Config) { c.MinWindow = 0 }, "minWindow"},
		{"max below min", func(c *Config) { c.MinWindow = 10; c.MaxWindow = 9 }, "maxWindow"},
		{"initial below min", func(c *Config) { c.InitialWindow = 1; c.MinWindow = 2 }, "initialWindow"},
		{"initial above max", func(c *Config) { c.InitialWindow = 100; c.MaxWindow = 50 }, "initialWindow"},
		{"zero position batch", func(c *Config) { c.PositionBatch = 0 }, "positionBatch"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{ChainID: 1}.withDefaults()
			tt.mutate(&cfg)

			err := cfg.validate()
			if err == nil {
				t.Fatalf("expected an error for %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %v, want it to name %q", err, tt.wantErr)
			}
		})
	}
}

func TestConfigValidate_AcceptsTheDefaults(t *testing.T) {
	if err := (Config{ChainID: 1}.withDefaults()).validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestConfigValidate_ExportedFormAppliesTheDefaultsFirst(t *testing.T) {
	if err := (Config{ChainID: 1}).Validate(); err != nil {
		t.Fatalf("Validate on an otherwise-zero config: %v", err)
	}
	if err := (Config{ChainID: 1, PositionBatch: -1}).Validate(); err == nil {
		t.Fatal("expected an error: a negative batch is not filled by the defaults")
	}
}
