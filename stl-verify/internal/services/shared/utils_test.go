package shared

import (
	"bytes"
	"compress/gzip"
	"testing"
)

// ============================================================================
// CacheKey tests
// ============================================================================

func TestCacheKey(t *testing.T) {
	tests := []struct {
		name        string
		chainID     int64
		blockNumber int64
		version     int
		dataType    string
		expected    string
	}{
		{
			name:        "block data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "block",
			expected:    "stl:1:12345:0:block",
		},
		{
			name:        "receipts data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "receipts",
			expected:    "stl:1:12345:0:receipts",
		},
		{
			name:        "traces data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "traces",
			expected:    "stl:1:12345:0:traces",
		},
		{
			name:        "blobs data",
			chainID:     1,
			blockNumber: 12345,
			version:     0,
			dataType:    "blobs",
			expected:    "stl:1:12345:0:blobs",
		},
		{
			name:        "reorged block version 1",
			chainID:     1,
			blockNumber: 12345,
			version:     1,
			dataType:    "block",
			expected:    "stl:1:12345:1:block",
		},
		{
			name:        "different chain",
			chainID:     137,
			blockNumber: 50000000,
			version:     0,
			dataType:    "block",
			expected:    "stl:137:50000000:0:block",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := CacheKey(tc.chainID, tc.blockNumber, tc.version, tc.dataType)
			if result != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, result)
			}
		})
	}
}

// ============================================================================
// ParseCompressedJSON tests
// ============================================================================

type testStruct struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

func TestParseCompressedJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    testStruct
		wantErr bool
	}{
		{
			name:    "uncompressed JSON",
			input:   []byte(`{"name":"test","value":42}`),
			want:    testStruct{Name: "test", Value: 42},
			wantErr: false,
		},
		{
			name:    "gzipped JSON",
			input:   gzipData(t, []byte(`{"name":"compressed","value":100}`)),
			want:    testStruct{Name: "compressed", Value: 100},
			wantErr: false,
		},
		{
			name:    "empty JSON object",
			input:   []byte(`{}`),
			want:    testStruct{},
			wantErr: false,
		},
		{
			name:    "gzipped empty JSON",
			input:   gzipData(t, []byte(`{}`)),
			want:    testStruct{},
			wantErr: false,
		},
		{
			name:    "invalid JSON",
			input:   []byte(`{invalid`),
			want:    testStruct{},
			wantErr: true,
		},
		{
			name:    "gzipped invalid JSON",
			input:   gzipData(t, []byte(`{invalid`)),
			want:    testStruct{},
			wantErr: true,
		},
		{
			name:    "corrupted gzip data",
			input:   []byte{0x1f, 0x8b, 0x00, 0x00, 0xff, 0xff}, // gzip magic bytes but corrupted
			want:    testStruct{},
			wantErr: true,
		},
		{
			name:    "empty data",
			input:   []byte{},
			want:    testStruct{},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got testStruct
			err := ParseCompressedJSON(tc.input, &got)

			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if got != tc.want {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseCompressedJSON_Array(t *testing.T) {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}

	tests := []struct {
		name    string
		input   []byte
		want    []item
		wantErr bool
	}{
		{
			name:    "uncompressed array",
			input:   []byte(`[{"id":1,"name":"first"},{"id":2,"name":"second"}]`),
			want:    []item{{ID: 1, Name: "first"}, {ID: 2, Name: "second"}},
			wantErr: false,
		},
		{
			name:    "gzipped array",
			input:   gzipData(t, []byte(`[{"id":3,"name":"third"}]`)),
			want:    []item{{ID: 3, Name: "third"}},
			wantErr: false,
		},
		{
			name:    "empty array",
			input:   []byte(`[]`),
			want:    []item{},
			wantErr: false,
		},
		{
			name:    "gzipped empty array",
			input:   gzipData(t, []byte(`[]`)),
			want:    []item{},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got []item
			err := ParseCompressedJSON(tc.input, &got)

			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if len(got) != len(tc.want) {
				t.Errorf("got length %d, want %d", len(got), len(tc.want))
				return
			}

			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("item %d: got %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// gzipData is a test helper that compresses data with gzip
func gzipData(t *testing.T, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)

	if _, err := gw.Write(data); err != nil {
		t.Fatalf("failed to write to gzip writer: %v", err)
	}

	if err := gw.Close(); err != nil {
		t.Fatalf("failed to close gzip writer: %v", err)
	}

	return buf.Bytes()
}
