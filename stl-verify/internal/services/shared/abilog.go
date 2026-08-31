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
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// LogBelongsTo reports whether a log emitted by addr should be routed to a
// pool/pair watched under any of addrs. A pool may be watched under more than
// one address (e.g. a pre-NG Curve pool's separate LP-token contract), so
// callers pass every address that should route to it.
func LogBelongsTo(addr common.Address, addrs ...common.Address) bool {
	return slices.Contains(addrs, addr)
}

// IsHexWord reports whether value is a full 32-byte hex word. common.HexToHash
// pads a short one into a plausible-looking wrong hash instead of failing.
func IsHexWord(value string) bool {
	return len(value) == 66 && strings.HasPrefix(value, "0x") && common.IsHexHash(value)
}

// DecodeLog extracts both indexed (from topics) and non-indexed (from data)
// fields of an ABI event log into a flat map, following the morpho_indexer
// parseTopics/parseData pattern.
//
// A log that cannot fill every argument its event declares is an error, never a
// partial map: a params blob missing half its fields is indistinguishable from
// a healthy row once persisted, and repairing it later costs a backfill.
func DecodeLog(ev abi.Event, log Log) (map[string]any, error) {
	out := make(map[string]any)
	if err := parseIndexedArgs(ev, log, out); err != nil {
		return nil, err
	}
	if err := parseNonIndexedArgs(ev, log, out); err != nil {
		return nil, err
	}
	if err := assertEveryArgumentDecoded(ev, out); err != nil {
		return nil, err
	}
	return out, nil
}

func parseIndexedArgs(ev abi.Event, log Log, out map[string]any) error {
	indexed := indexedArgs(ev)
	if len(indexed) == 0 {
		return nil
	}
	if len(log.Topics) == 0 {
		return fmt.Errorf("%s log carries no topics but declares indexed arguments %s", ev.Name, argNames(indexed))
	}
	hashes := make([]common.Hash, 0, len(log.Topics)-1)
	for _, topic := range log.Topics[1:] {
		hashes = append(hashes, common.HexToHash(topic))
	}
	if err := abi.ParseTopicsIntoMap(out, indexed, hashes); err != nil {
		return fmt.Errorf("parsing indexed params: %w", err)
	}
	return nil
}

func parseNonIndexedArgs(ev abi.Event, log Log, out map[string]any) error {
	nonIndexed := ev.Inputs.NonIndexed()
	if len(nonIndexed) == 0 {
		return nil
	}
	// common.FromHex left-pads odd-length input and swallows the error, so a
	// dropped digit silently shifts every argument; hexutil.Decode rejects it.
	raw, err := hexutil.Decode(log.Data)
	if err != nil {
		return fmt.Errorf("%s log data %q is not valid hex: %w", ev.Name, log.Data, err)
	}
	if len(raw) == 0 {
		return fmt.Errorf("%s log carries no data for non-indexed arguments %s", ev.Name, argNames(nonIndexed))
	}
	if err := nonIndexed.UnpackIntoMap(out, raw); err != nil {
		return fmt.Errorf("parsing non-indexed params: %w", err)
	}
	return nil
}

func assertEveryArgumentDecoded(ev abi.Event, out map[string]any) error {
	for _, arg := range ev.Inputs {
		if _, ok := out[arg.Name]; !ok {
			return fmt.Errorf("%s log left argument %s undecoded", ev.Name, arg.Name)
		}
	}
	return nil
}

func indexedArgs(ev abi.Event) abi.Arguments {
	var out abi.Arguments
	for _, arg := range ev.Inputs {
		if arg.Indexed {
			out = append(out, arg)
		}
	}
	return out
}

func argNames(args abi.Arguments) string {
	names := make([]string, len(args))
	for i, arg := range args {
		names[i] = arg.Name
	}
	return strings.Join(names, ", ")
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

// GetHashField reads key as a common.Hash: go-ethereum decodes a bytes32
// argument, indexed or not, into [32]byte rather than any named type.
func GetHashField(data map[string]any, key string) (common.Hash, error) {
	v, ok := data[key]
	if !ok {
		return common.Hash{}, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.([32]byte)
	if !ok {
		return common.Hash{}, fmt.Errorf("field %s: unexpected type %T", key, v)
	}
	return common.Hash(b), nil
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
