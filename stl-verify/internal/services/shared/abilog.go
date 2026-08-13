package shared

import (
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Pure helpers for ABI-log decoding and multicall-result unpacking shared by
// the per-DEX worker packages (Curve, Uniswap V3, Balancer).

// LogBelongsTo reports whether a log emitted by addr should be routed to a
// pool/pair watched under any of addrs. A pool may be watched under more than
// one address (e.g. a pre-NG Curve pool's separate LP-token contract), so
// callers pass every address that should route to it.
func LogBelongsTo(addr common.Address, addrs ...common.Address) bool {
	return slices.Contains(addrs, addr)
}

// DecodeLog extracts both indexed (from topics) and non-indexed (from data)
// fields of an ABI event log into a flat map, following the morpho_indexer
// parseTopics/parseData pattern.
func DecodeLog(ev abi.Event, log Log) (map[string]any, error) {
	out := make(map[string]any)

	var indexed abi.Arguments
	for _, arg := range ev.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}
	if len(indexed) > 0 {
		hashes := make([]common.Hash, 0, len(log.Topics)-1)
		for i := 1; i < len(log.Topics); i++ {
			hashes = append(hashes, common.HexToHash(log.Topics[i]))
		}
		if err := abi.ParseTopicsIntoMap(out, indexed, hashes); err != nil {
			return nil, fmt.Errorf("parsing indexed params: %w", err)
		}
	}

	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	if len(nonIndexed) > 0 && len(log.Data) > 2 {
		raw := common.FromHex(log.Data)
		if err := nonIndexed.UnpackIntoMap(out, raw); err != nil {
			return nil, fmt.Errorf("parsing non-indexed params: %w", err)
		}
	}

	return out, nil
}

// GetAddrField reads key from a DecodeLog result map as a common.Address.
func GetAddrField(data map[string]any, key string) (common.Address, error) {
	v, ok := data[key]
	if !ok {
		return common.Address{}, fmt.Errorf("missing field: %s", key)
	}
	addr, ok := v.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return addr, nil
}

// GetBigIntField reads key from a DecodeLog result map as a *big.Int.
func GetBigIntField(data map[string]any, key string) (*big.Int, error) {
	v, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return b, nil
}

// GetBigIntSliceField reads key from a DecodeLog result map as a []*big.Int.
func GetBigIntSliceField(data map[string]any, key string) ([]*big.Int, error) {
	v, ok := data[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	slice, ok := v.([]*big.Int)
	if !ok {
		return nil, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return slice, nil
}

// ParseHexUint parses a 0x-prefixed hex string into a uint, as used in Log.LogIndex.
func ParseHexUint(s string) (uint, error) {
	if !strings.HasPrefix(s, "0x") || len(s) == 2 {
		return 0, fmt.Errorf("invalid hex uint %q", s)
	}
	n, err := strconv.ParseUint(s[2:], 16, strconv.IntSize)
	if err != nil {
		return 0, fmt.Errorf("parsing hex uint %q: %w", s, err)
	}
	return uint(n), nil
}

// UnpackSingleUint decodes a single uint256-returning multicall result whose
// method is not in an ABI (e.g. a manually-selector-packed call). A reverted
// call or an undecodable payload is an error.
func UnpackSingleUint(r outbound.Result) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("uint256 call reverted")
	}
	u256T, err := abi.NewType("uint256", "", nil)
	if err != nil {
		return nil, fmt.Errorf("uint256 type: %w", err)
	}
	args := abi.Arguments{{Type: u256T}}
	vals, err := args.Unpack(r.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking uint256: %w", err)
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("uint256 returned no values")
	}
	bi, ok := vals[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("uint256 value is %T, not *big.Int", vals[0])
	}
	return bi, nil
}

// UnpackUintArray decodes a uint256[n]-returning multicall result into a
// []*big.Int of length n. A reverted sub-call or an undecodable payload is an error.
func UnpackUintArray(r outbound.Result, n int) ([]*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("uint256[%d] call reverted", n)
	}
	arrT, err := abi.NewType(fmt.Sprintf("uint256[%d]", n), "", nil)
	if err != nil {
		return nil, fmt.Errorf("uint256[%d] type: %w", n, err)
	}
	args := abi.Arguments{{Type: arrT}}
	vals, err := args.Unpack(r.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking uint256[%d]: %w", n, err)
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("uint256[%d] returned no values", n)
	}
	return toBigIntSlice(vals[0])
}

// toBigIntSlice converts a Go fixed-size array or slice of *big.Int (as
// returned by abi.Arguments.Unpack for uint256[N]) into []*big.Int.
func toBigIntSlice(v any) ([]*big.Int, error) {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Array && rv.Kind() != reflect.Slice {
		return nil, fmt.Errorf("expected array/slice of *big.Int, got %T", v)
	}
	out := make([]*big.Int, rv.Len())
	for i := range out {
		bi, ok := rv.Index(i).Interface().(*big.Int)
		if !ok {
			return nil, fmt.Errorf("element %d is not *big.Int (got %T)", i, rv.Index(i).Interface())
		}
		out[i] = bi
	}
	return out, nil
}

// OptionalUintResult decodes an AllowFailure=true scalar snapshot result. A
// revert is an error, not a nil field: per the no-swallowed-errors rule a
// best-effort read that reverted must stop the block rather than be silently
// turned into a NULL column. The only legitimately-absent reads are gated
// structurally (not issued) rather than swallowed here.
func OptionalUintResult(abiDef *abi.ABI, method string, r outbound.Result, target common.Address, blockNumber int64) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("snapshot call %s reverted for %s at block %d", method, target, blockNumber)
	}
	return UnpackUint(abiDef, method, r)
}
