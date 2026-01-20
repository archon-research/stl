// Package hexutil provides utilities for parsing Ethereum hex-encoded values.
//
// This package is intentionally placed in internal/pkg to allow imports from
// both adapters and services without violating hexagonal architecture principles.
package hexutil

import (
	"strconv"
	"strings"
)

// ParseInt64 parses a hex-encoded string to int64.
// Handles both "0x" prefixed and non-prefixed hex strings.
func ParseInt64(hexNum string) (int64, error) {
	hexNum = strings.TrimPrefix(hexNum, "0x")
	return strconv.ParseInt(hexNum, 16, 64)
}