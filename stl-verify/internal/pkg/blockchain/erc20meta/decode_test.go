package erc20meta

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

func TestDecodeStringOrBytes32(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}

	// Modern string-returning ERC20: ABI-encode "WBTC" as a `string`.
	stringEncoded, err := erc20ABI.Methods["symbol"].Outputs.Pack("WBTC")
	if err != nil {
		t.Fatalf("packing string: %v", err)
	}

	// Legacy bytes32-returning ERC20 (MKR-style): the raw return is exactly
	// 32 bytes, ASCII bytes left-aligned, null-padded.
	mkrBytes32 := append([]byte("MKR"), make([]byte, 29)...)

	tests := []struct {
		name    string
		data    []byte
		want    string
		wantErr bool
	}{
		{
			name: "modern string-encoded symbol decodes",
			data: stringEncoded,
			want: "WBTC",
		},
		{
			name: "MKR-style bytes32 symbol decodes via fallback",
			data: mkrBytes32,
			want: "MKR",
		},
		{
			name: "trailing nulls trimmed from bytes32 decode",
			data: append([]byte("LONGER_NAME_15B"), make([]byte, 17)...),
			want: "LONGER_NAME_15B",
		},
		{
			name:    "empty return data is an error",
			data:    nil,
			wantErr: true,
		},
		{
			name:    "non-32-byte non-string data is an error",
			data:    []byte{0x01, 0x02, 0x03},
			wantErr: true,
		},
		{
			// 0xFF / 0xFE / 0xFD are invalid as a UTF-8 leading byte.
			// Without sanitization the cast `string(data)` produces a Go
			// string with invalid UTF-8 — pgx rejects that on downstream
			// `text` column upserts. Helper must guarantee valid UTF-8.
			name: "bytes32 with invalid UTF-8 bytes is sanitized to empty",
			data: append([]byte{0xFF, 0xFE, 0xFD}, make([]byte, 29)...),
			want: "",
		},
		{
			name: "bytes32 with valid prefix + invalid bytes keeps the prefix",
			data: append([]byte("MKR"), append([]byte{0xFF, 0xFE}, make([]byte, 27)...)...),
			want: "MKR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeStringOrBytes32(erc20ABI, "symbol", tt.data)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DecodeStringOrBytes32 err = %v; wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("DecodeStringOrBytes32 = %q; want %q", got, tt.want)
			}
		})
	}
}

// TestDecodeStringOrBytes32_NotInABIError ensures the helper surfaces the
// underlying ABI error when the method name is wrong (defensive — every
// caller will have already validated this, but the wrap is part of the
// contract).
func TestDecodeStringOrBytes32_NotInABIError(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}
	_, err = DecodeStringOrBytes32(erc20ABI, "nonExistentMethod", []byte{0x01})
	if err == nil {
		t.Fatal("expected error for unknown method")
	}
	if !strings.Contains(err.Error(), "nonExistentMethod") {
		t.Errorf("error should reference the method name; got %q", err)
	}
}
