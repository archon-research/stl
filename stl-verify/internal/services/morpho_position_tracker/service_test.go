package morpho_position_tracker

import (
	"testing"
)

func TestConfigDefaults(t *testing.T) {
	defaults := ConfigDefaults()

	if defaults.MaxMessages != 10 {
		t.Errorf("MaxMessages = %d, want 10", defaults.MaxMessages)
	}
	if defaults.PollInterval == 0 {
		t.Error("PollInterval should not be zero")
	}
	if defaults.ChainID != 1 {
		t.Errorf("ChainID = %d, want 1", defaults.ChainID)
	}
	if defaults.Logger == nil {
		t.Error("Logger should not be nil")
	}
}

func TestValidateDependencies(t *testing.T) {
	tests := []struct {
		name        string
		errContains string
	}{
		{"all nil", "consumer is required"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDependencies(nil, nil, nil, nil, nil, nil, nil, nil, nil)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if tt.errContains != "" {
				found := false
				for i := 0; i <= len(err.Error())-len(tt.errContains); i++ {
					if err.Error()[i:i+len(tt.errContains)] == tt.errContains {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func TestMorphoBlueAddress(t *testing.T) {
	expected := "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"
	if MorphoBlueAddress.Hex() != expected {
		t.Errorf("MorphoBlueAddress = %s, want %s", MorphoBlueAddress.Hex(), expected)
	}
}
