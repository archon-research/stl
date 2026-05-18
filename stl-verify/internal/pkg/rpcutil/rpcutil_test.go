package rpcutil

import (
	"encoding/json"
	"testing"
)

func TestIsNullOrEmpty(t *testing.T) {
	cases := []struct {
		name string
		in   json.RawMessage
		want bool
	}{
		{"nil", nil, true},
		{"empty", json.RawMessage{}, true},
		{"whitespace_only", json.RawMessage("\n  \t"), true},
		{"literal_null", json.RawMessage("null"), true},
		{"literal_null_with_whitespace", json.RawMessage("  null\n"), true},
		{"empty_object", json.RawMessage("{}"), false},
		{"empty_array", json.RawMessage("[]"), false},
		{"valid_block", json.RawMessage(`{"hash":"0xabc"}`), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsNullOrEmpty(tc.in); got != tc.want {
				t.Errorf("IsNullOrEmpty(%q) = %v, want %v", string(tc.in), got, tc.want)
			}
		})
	}
}
