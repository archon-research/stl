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

func TestChangeReason(t *testing.T) {
	if got := changeReason("position-materializer", ""); got != "position-materializer@dev" {
		t.Errorf("empty commit: %q", got)
	}
	if got := changeReason("pm", "abcdef0123456789"); got != "pm@abcdef012345" {
		t.Errorf("long commit not truncated to 12: %q", got)
	}
	if got := changeReason("pm", "abc"); got != "pm@abc" {
		t.Errorf("short commit: %q", got)
	}
}
