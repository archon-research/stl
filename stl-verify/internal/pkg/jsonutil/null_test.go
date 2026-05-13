package jsonutil

import (
	"encoding/json"
	"testing"
)

func TestIsNullOrEmpty(t *testing.T) {
	tests := []struct {
		name string
		in   json.RawMessage
		want bool
	}{
		{"nil RawMessage", nil, true},
		{"empty bytes", json.RawMessage{}, true},
		{"literal null", json.RawMessage("null"), true},
		{"null with surrounding spaces", json.RawMessage("  null  "), true},
		{"null with surrounding newlines", json.RawMessage("\nnull\n"), true},
		{"empty object", json.RawMessage("{}"), false},
		{"zero number", json.RawMessage("0"), false},
		{"valid block payload", json.RawMessage(`{"hash":"0xabc"}`), false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := IsNullOrEmpty(tc.in)
			if got != tc.want {
				t.Errorf("IsNullOrEmpty(%q) = %v, want %v", string(tc.in), got, tc.want)
			}
		})
	}
}
