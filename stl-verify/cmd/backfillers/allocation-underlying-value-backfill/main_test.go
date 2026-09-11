package main

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantCfg   cliConfig
		wantError string
	}{
		{
			name: "defaults",
			args: []string{"-db", "postgres://localhost/db"},
			wantCfg: cliConfig{
				dbURL:       "postgres://localhost/db",
				before:      time.Date(2026, 7, 6, 14, 0, 0, 0, time.UTC),
				limit:       100,
				dryRun:      true,
				maxPriceLag: 7200,
			},
		},
		{
			name: "overrides everything",
			args: []string{
				"-db", "postgres://localhost/db",
				"-before", "2026-08-01T00:00:00Z",
				"-after", "2026-01-01T00:00:00Z",
				"-prime-id", "3",
				"-limit", "500",
				"-dry-run=false",
				"-max-price-block-lag", "100",
			},
			wantCfg: cliConfig{
				dbURL:       "postgres://localhost/db",
				before:      time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
				after:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
				primeID:     3,
				limit:       500,
				dryRun:      false,
				maxPriceLag: 100,
			},
		},
		{
			name:      "missing db",
			args:      []string{},
			wantError: "--db is required",
		},
		{
			name:      "bad before",
			args:      []string{"-db", "x", "-before", "not-a-time"},
			wantError: "--before",
		},
		{
			name:      "bad after",
			args:      []string{"-db", "x", "-after", "not-a-time"},
			wantError: "--after",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFlags(tt.args)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("parseFlags(%v) error = %v, want containing %q", tt.args, err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseFlags(%v) unexpected error: %v", tt.args, err)
			}
			if got != tt.wantCfg {
				t.Fatalf("parseFlags(%v) = %+v, want %+v", tt.args, got, tt.wantCfg)
			}
		})
	}
}

func TestIsAaveFamily(t *testing.T) {
	tests := []struct {
		protocol string
		want     bool
	}{
		{"SparkLend", true},
		{"Aave V3", true},
		{"Aave V3 Lido", true},
		{"Aave V3 Base", true},
		{"Aave V3 RWA", true},
		{"Aave V2", true},
		// A receipt token can be registered under more than one protocol when
		// two distinct on-chain contracts share a display symbol (e.g. spDAI:
		// a SparkLend aToken at one address, a Morpho Blue vault at another).
		// Classification keys off the resolved protocol name for THIS row's
		// own address, never the symbol, so a non-Aave registration for a
		// same-symbol token must not be swept in by a loose match.
		{"Morpho Blue", false},
		{"Sky Savings Rate", false},
		{"", false},
		{"aave v3", false}, // case-sensitive: on-chain protocol names are exact
	}
	for _, tt := range tests {
		t.Run(tt.protocol, func(t *testing.T) {
			if got := isAaveFamily(tt.protocol); got != tt.want {
				t.Errorf("isAaveFamily(%q) = %v, want %v", tt.protocol, got, tt.want)
			}
		})
	}
}

func TestHumanToRaw(t *testing.T) {
	tests := []struct {
		name     string
		human    string
		decimals int32
		want     *big.Int
		wantErr  bool
	}{
		{"whole number 18 decimals", "1", 18, big.NewInt(1_000_000_000_000_000_000), false},
		{"fractional 6 decimals", "20102052.000000", 6, big.NewInt(20_102_052_000_000), false},
		{"zero", "0", 18, big.NewInt(0), false},
		{"not a number", "abc", 18, nil, true},
		// Regression for the 10^18 scaling bug: a human value whose fractional
		// part does not divide evenly at the given decimals is not a valid raw
		// on-chain amount, and must be rejected rather than truncated.
		{"non-exact raw amount", "1.0000000000000000001", 18, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := humanToRaw(tt.human, tt.decimals)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("humanToRaw(%q, %d) expected error, got %v", tt.human, tt.decimals, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("humanToRaw(%q, %d) unexpected error: %v", tt.human, tt.decimals, err)
			}
			if got.Cmp(tt.want) != 0 {
				t.Fatalf("humanToRaw(%q, %d) = %s, want %s", tt.human, tt.decimals, got, tt.want)
			}
		})
	}
}
