package main

import (
	"testing"
	"time"
)

func TestParseAssetIDs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "single asset",
			input:    "ethereum",
			expected: []string{"ethereum"},
		},
		{
			name:     "multiple assets",
			input:    "ethereum,bitcoin,usd-coin",
			expected: []string{"ethereum", "bitcoin", "usd-coin"},
		},
		{
			name:     "with spaces",
			input:    " ethereum , bitcoin , usd-coin ",
			expected: []string{"ethereum", "bitcoin", "usd-coin"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseAssetIDs(tt.input)

			if len(result) != len(tt.expected) {
				t.Errorf("expected %d assets, got %d", len(tt.expected), len(result))
				return
			}

			for i, expected := range tt.expected {
				if result[i] != expected {
					t.Errorf("asset %d: expected %q, got %q", i, expected, result[i])
				}
			}
		})
	}
}

func TestGetChainID(t *testing.T) {
	// Test default value
	t.Setenv("CHAIN_ID", "")
	chainID, err := getChainID()
	if err != nil {
		t.Fatalf("getChainID failed: %v", err)
	}
	if chainID != 1 {
		t.Errorf("expected default chainID 1, got %d", chainID)
	}

	// Test custom value
	t.Setenv("CHAIN_ID", "42")
	chainID, err = getChainID()
	if err != nil {
		t.Fatalf("getChainID failed: %v", err)
	}
	if chainID != 42 {
		t.Errorf("expected chainID 42, got %d", chainID)
	}

	// Test invalid value
	t.Setenv("CHAIN_ID", "not-a-number")
	_, err = getChainID()
	if err == nil {
		t.Error("expected error for invalid CHAIN_ID")
	}
}

func TestParseDateRange(t *testing.T) {
	tests := []struct {
		name        string
		fromDate    string
		toDate      string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid date range",
			fromDate:    "2024-01-01",
			toDate:      "2024-01-15",
			expectError: false,
		},
		{
			name:        "same day",
			fromDate:    "2024-01-01",
			toDate:      "2024-01-01",
			expectError: false,
		},
		{
			name:        "invalid from date",
			fromDate:    "not-a-date",
			toDate:      "2024-01-15",
			expectError: true,
			errorMsg:    "invalid --from date",
		},
		{
			name:        "invalid to date",
			fromDate:    "2024-01-01",
			toDate:      "not-a-date",
			expectError: true,
			errorMsg:    "invalid --to date",
		},
		{
			name:        "from after to",
			fromDate:    "2024-01-15",
			toDate:      "2024-01-01",
			expectError: true,
			errorMsg:    "--from date must be before --to date",
		},
		{
			name:        "invalid from format - wrong separator",
			fromDate:    "2024/01/01",
			toDate:      "2024-01-15",
			expectError: true,
			errorMsg:    "invalid --from date",
		},
		{
			name:        "invalid to format - wrong separator",
			fromDate:    "2024-01-01",
			toDate:      "2024/01/15",
			expectError: true,
			errorMsg:    "invalid --to date",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, to, err := parseDateRange(tt.fromDate, tt.toDate)

			if tt.expectError {
				if err == nil {
					t.Error("expected error but got nil")
					return
				}
				if tt.errorMsg != "" && !contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			// Verify from date was parsed correctly
			expectedFrom, _ := time.Parse(time.DateOnly, tt.fromDate)
			if !from.Equal(expectedFrom) {
				t.Errorf("from date: expected %v, got %v", expectedFrom, from)
			}

			// Verify to date is end of day (23:59:59)
			expectedToBase, _ := time.Parse(time.DateOnly, tt.toDate)
			expectedTo := expectedToBase.Add(24*time.Hour - time.Second)
			if !to.Equal(expectedTo) {
				t.Errorf("to date: expected %v, got %v", expectedTo, to)
			}
		})
	}
}

func TestParseDateRange_DefaultToYesterday(t *testing.T) {
	from, to, err := parseDateRange("2024-01-01", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expectedFrom, _ := time.Parse(time.DateOnly, "2024-01-01")
	if !from.Equal(expectedFrom) {
		t.Errorf("from date: expected %v, got %v", expectedFrom, from)
	}

	// to should be yesterday at end of day
	yesterday := time.Now().UTC().Truncate(24 * time.Hour).Add(-24 * time.Hour)
	yesterdayEndOfDay := yesterday.Add(24*time.Hour - time.Second)

	if !to.Equal(yesterdayEndOfDay) {
		t.Errorf("to date: expected yesterday end of day %v, got %v", yesterdayEndOfDay, to)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && searchSubstring(s, substr)))
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
