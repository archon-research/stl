package transform

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// HexToBytes converts a hex string (with or without 0x prefix) to bytes.
// Returns an error if the hex string is invalid.
func HexToBytes(hexStr string) ([]byte, error) {
	hexStr = strings.TrimPrefix(hexStr, "0x")
	hexStr = strings.TrimPrefix(hexStr, "0X")
	return hex.DecodeString(hexStr)
}

// HexToAddress converts a hex string to a 20-byte Ethereum address.
// Returns an error if the hex string is invalid or not 20 bytes.
func HexToAddress(hexStr string) ([]byte, error) {
	bytes, err := HexToBytes(hexStr)
	if err != nil {
		return nil, fmt.Errorf("invalid hex: %w", err)
	}
	if len(bytes) != 20 {
		return nil, fmt.Errorf("invalid address length: expected 20, got %d", len(bytes))
	}
	return bytes, nil
}

// AddressToHex converts a 20-byte address to a hex string with 0x prefix.
func AddressToHex(addr []byte) string {
	return "0x" + hex.EncodeToString(addr)
}

// NormalizeAddress normalizes an address string to lowercase without 0x prefix.
// This is useful for consistent key generation.
func NormalizeAddress(addr string) string {
	addr = strings.TrimPrefix(addr, "0x")
	addr = strings.TrimPrefix(addr, "0X")
	return strings.ToLower(addr)
}
