package allocation_tracker

import (
	"context"
	"log/slog"
	"testing"
)

func TestSkipSource_Supports(t *testing.T) {
	logger := slog.Default()
	src := NewSkipSource("test-skip", "atoken", []string{"sparklend", "aave"}, logger)

	tests := []struct {
		tokenType string
		protocol  string
		want      bool
	}{
		{"atoken", "sparklend", true},
		{"atoken", "aave", true},
		{"atoken", "morpho", false},
		{"erc20", "sparklend", false},
	}
	for _, tt := range tests {
		got := src.Supports(tt.tokenType, tt.protocol)
		if got != tt.want {
			t.Errorf("Supports(%q, %q) = %v, want %v", tt.tokenType, tt.protocol, got, tt.want)
		}
	}
}

func TestSkipSource_Supports_NoProtocolFilter(t *testing.T) {
	logger := slog.Default()
	src := NewSkipSource("all-match", "anchorage", nil, logger)

	if !src.Supports("anchorage", "any-protocol") {
		t.Error("nil protocols should match any protocol")
	}
	if !src.Supports("anchorage", "") {
		t.Error("nil protocols should match empty protocol")
	}
	if src.Supports("erc20", "") {
		t.Error("should not match wrong tokenType")
	}
}

func TestSkipSource_FetchBalances_ReturnsEmpty(t *testing.T) {
	logger := slog.Default()
	src := NewSkipSource("test-skip", "atoken", nil, logger)

	entries := []*TokenEntry{{TokenType: "atoken"}}
	result, err := src.FetchBalances(context.Background(), entries, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

func TestStubSource_Supports(t *testing.T) {
	logger := slog.Default()
	src := NewStubSource("test-stub", "centrifuge", logger)

	if !src.Supports("centrifuge", "anything") {
		t.Error("stub should match its tokenType regardless of protocol")
	}
	if src.Supports("erc20", "") {
		t.Error("stub should not match different tokenType")
	}
}

func TestStubSource_FetchBalances_ReturnsEmpty(t *testing.T) {
	logger := slog.Default()
	src := NewStubSource("test-stub", "centrifuge", logger)

	result, err := src.FetchBalances(context.Background(), nil, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 0 {
		t.Errorf("expected empty map, got %d entries", len(result))
	}
}

func TestDefaultSkipSources(t *testing.T) {
	logger := slog.Default()
	sources := DefaultSkipSources(logger)

	if len(sources) == 0 {
		t.Fatal("expected at least 1 skip source")
	}

	// Verify sparklend-skip exists
	found := false
	for _, s := range sources {
		if s.Name() == "sparklend-skip" {
			found = true
			if !s.Supports("atoken", "sparklend") {
				t.Error("sparklend-skip should support atoken/sparklend")
			}
		}
	}
	if !found {
		t.Error("sparklend-skip not found in DefaultSkipSources")
	}
}

func TestDefaultStubSources(t *testing.T) {
	logger := slog.Default()
	sources := DefaultStubSources(logger)

	if len(sources) == 0 {
		t.Fatal("expected at least 1 stub source")
	}

	names := make(map[string]bool)
	for _, s := range sources {
		names[s.Name()] = true
	}
	if !names["centrifuge"] {
		t.Error("centrifuge stub not found")
	}
}
