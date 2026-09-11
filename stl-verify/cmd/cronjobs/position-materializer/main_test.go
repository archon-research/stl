package main

import (
	"slices"
	"testing"
)

func TestParseProjections(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		want    []string
		wantErr bool
	}{
		{"single", "materialize_morpho_market", []string{"materialize_morpho_market"}, false},
		{"several with spaces", " materialize_a , materialize_b ,materialize_c", []string{"materialize_a", "materialize_b", "materialize_c"}, false},
		{"trailing comma is not a blank entry", "materialize_a,materialize_b,", []string{"materialize_a", "materialize_b"}, false},
		{"empty segments dropped", ",,materialize_a,,", []string{"materialize_a"}, false},
		{"all empty", " , ,", nil, false},
		{"a view name is not a materializer", "position_morpho_market", nil, true},
		{"the shared function is not a projection", "materialize_position_projection", nil, true},
		{"quoting or SQL is rejected", `materialize_a"; drop table x; --`, nil, true},
		{"upper case is rejected", "Materialize_A", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseProjections(tc.raw)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseProjections(%q) error = %v; wantErr %v", tc.raw, err, tc.wantErr)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("parseProjections(%q) = %v; want %v", tc.raw, got, tc.want)
			}
		})
	}
}
