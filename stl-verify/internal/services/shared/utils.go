// Package shared provides shared utilities for application services.
package shared

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

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

// ParseCompressedJSON decompresses gzipped data (if needed) and unmarshals it
func ParseCompressedJSON(data []byte, v any) error {
	// Check for gzip magic bytes (0x1f 0x8b)
	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}

		decompressed, err := io.ReadAll(gr)
		closeErr := gr.Close()

		if err != nil {
			return fmt.Errorf("failed to decompress: %w", err)
		}

		if closeErr != nil {
			return fmt.Errorf("failed to close gzip reader: %w", closeErr)
		}

		data = decompressed
	}

	return json.Unmarshal(data, v)
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

// UnpackUint decodes a single uint256-returning view method's result from a
// multicall sub-call. It is the shared implementation used by the per-DEX
// multicall readers (Curve, Uniswap V3, Balancer). A reverted sub-call, an
// undecodable payload, an empty tuple, or a non-*big.Int first value are all
// errors. The returned value is a defensive copy, so callers may retain it
// without aliasing the decoder's internal buffers.
func UnpackUint(a *abi.ABI, method string, r outbound.Result) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("%s reverted", method)
	}
	unpacked, err := a.Unpack(method, r.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking %s: %w", method, err)
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("%s returned no values", method)
	}
	v, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("%s returned %T, want *big.Int", method, unpacked[0])
	}
	return new(big.Int).Set(v), nil
}

// Negate returns -b, treating a nil input as zero.
func Negate(b *big.Int) *big.Int {
	if b == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Neg(b)
}

// BigIntCopy returns a defensive copy of b, preserving nil.
func BigIntCopy(b *big.Int) *big.Int {
	if b == nil {
		return nil
	}
	return new(big.Int).Set(b)
}

// BigIntToTimePtr converts a Unix-seconds big.Int to a *time.Time in UTC,
// returning nil for a nil or zero value (the on-chain "unset" sentinel), or for
// a value outside int64 range — that is not a real timestamp (it would be year
// >2.9e11), and returning nil avoids Int64() silently truncating it to a
// plausible-looking wrong date.
func BigIntToTimePtr(b *big.Int) *time.Time {
	if b == nil || b.Sign() == 0 || !b.IsInt64() {
		return nil
	}
	t := time.Unix(b.Int64(), 0).UTC()
	return &t
}
