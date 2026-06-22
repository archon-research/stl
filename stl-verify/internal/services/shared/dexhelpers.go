package shared

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// This file holds the pure helpers hoisted out of the per-DEX worker packages
// (review S7/S8): single-uint multicall decoding and big.Int/time conversions
// shared by the Curve / Uniswap V3 / Balancer workers.

// UnpackUint decodes a single uint256-returning view method's result from a
// multicall sub-call. It is the shared implementation used by the per-DEX
// multicall readers (Curve, Uniswap V3, Balancer). A reverted sub-call, an
// undecodable payload, an empty tuple, or a non-*big.Int first value are all
// errors. The returned value is a defensive copy, so callers may retain it
// without aliasing the decoder's internal buffers.
func UnpackUint(a *abi.ABI, method string, r outbound.Result) (*big.Int, error) {
	if a == nil {
		return nil, fmt.Errorf("unpacking %s: nil ABI", method)
	}
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
