package main

import "testing"

func TestSplitProjections(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []string
	}{
		{"single", "position_morpho_market", []string{"position_morpho_market"}},
		{"several with spaces", " a , b ,c", []string{"a", "b", "c"}},
		{"trailing comma is not a blank view", "a,b,", []string{"a", "b"}},
		{"empty segments dropped", ",,a,,", []string{"a"}},
		{"all empty", " , ,", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitProjections(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("splitProjections(%q) = %v; want %v", tc.raw, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("splitProjections(%q) = %v; want %v", tc.raw, got, tc.want)
				}
			}
		})
	}
}
