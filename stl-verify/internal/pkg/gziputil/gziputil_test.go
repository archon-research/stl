package gziputil_test

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/gziputil"
)

func TestIsGzipped(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"gzip magic bytes", []byte{0x1f, 0x8b, 0x08, 0x00}, true},
		{"json data", []byte(`{"key": "value"}`), false},
		{"empty data", []byte{}, false},
		{"single byte", []byte{0x1f}, false},
		{"wrong second byte", []byte{0x1f, 0x00}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := gziputil.IsGzipped(tt.data)
			if got != tt.expected {
				t.Errorf("IsGzipped(%v) = %v, want %v", tt.data, got, tt.expected)
			}
		})
	}
}

func TestDecompress_UncompressedPassthrough(t *testing.T) {
	data := []byte(`{"number":"0x1"}`)
	got, err := gziputil.Decompress(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != string(data) {
		t.Errorf("got %q, want %q", got, data)
	}
}

func TestDecompress_NilInput(t *testing.T) {
	got, err := gziputil.Decompress(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestDecompress_InvalidGzip(t *testing.T) {
	// Starts with gzip magic bytes but is not valid gzip.
	data := []byte{0x1f, 0x8b, 0x00, 0x00}
	_, err := gziputil.Decompress(data)
	if err == nil {
		t.Fatal("expected error for invalid gzip data, got nil")
	}
}
