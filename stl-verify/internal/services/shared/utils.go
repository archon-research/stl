// Package shared provides shared utilities for application services.
package shared

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ParseBlockHeader unmarshals JSON data into a BlockHeader.
func ParseBlockHeader(data []byte, header *outbound.BlockHeader) error {
	return json.Unmarshal(data, header)
}

// CacheKey generates the cache key for a given data type.
// Format: stl:{chainID}:{blockNumber}:{version}:{dataType}
// The version is incremented each time a block at the same height is reorged.
func CacheKey(chainID, blockNumber int64, version int, dataType string) string {
	return fmt.Sprintf("stl:%d:%d:%d:%s", chainID, blockNumber, version, dataType)
}

// FormatAmount converts a raw big.Int token amount to a human-readable
// decimal-adjusted string. For example, 1500000 with 6 decimals becomes "1.5".
func FormatAmount(rawAmount *big.Int, decimals int) string {
	if rawAmount == nil {
		return "0"
	}
	if decimals == 0 {
		return rawAmount.String()
	}

	negative := rawAmount.Sign() < 0
	absAmount := new(big.Int).Abs(rawAmount)

	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	integerPart := new(big.Int).Div(absAmount, divisor)
	remainder := new(big.Int).Mod(absAmount, divisor)

	prefix := ""
	if negative {
		prefix = "-"
	}

	if remainder.Cmp(big.NewInt(0)) == 0 {
		return prefix + integerPart.String()
	}

	remainderStr := remainder.String()
	padded := strings.Repeat("0", decimals-len(remainderStr)) + remainderStr
	padded = strings.TrimRight(padded, "0")

	return fmt.Sprintf("%s%s.%s", prefix, integerPart.String(), padded)
}
