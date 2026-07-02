package curveindexer

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gocrypto "github.com/ethereum/go-ethereum/crypto"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// logBelongsToPool reports whether a log emitted by addr should be decoded for
// pool. It matches the pool's own address and, for pre-NG pools whose LP token is
// a separate contract, the LP-token contract address (so its Transfer/Approval
// logs route to the owning pool).
func logBelongsToPool(addr common.Address, pool RegisteredPool) bool {
	if addr == pool.Address {
		return true
	}
	return pool.LpTokenAddress != nil && addr == *pool.LpTokenAddress
}

// decodeLog extracts both indexed (from topics) and non-indexed (from data) fields
// into a flat map, following the morpho_indexer parseTopics/parseData pattern.
func decodeLog(ev *abi.Event, log shared.Log) (map[string]any, error) {
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

func getAddrField(data map[string]any, key string) (common.Address, error) {
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

// coinIndexOrError converts an on-chain coin index (int128, decoded as *big.Int)
// to an int, erroring if it falls outside [0, nCoins). A garbage index must stop
// the block (SQS-retryable) rather than be silently truncated into a plausible
// coin. Shared by the swap and remove-one decoders across both pool classes.
func coinIndexOrError(field string, v *big.Int, nCoins int) (int, error) {
	if !v.IsInt64() || v.Sign() < 0 || v.Int64() >= int64(nCoins) {
		return 0, fmt.Errorf("%s %s out of range [0,%d)", field, v, nCoins)
	}
	return int(v.Int64()), nil
}

func getBigIntField(data map[string]any, key string) (*big.Int, error) {
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

func getBigIntSliceField(data map[string]any, key string) ([]*big.Int, error) {
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

// packCalcTokenAmount packs calc_token_amount(uint256[N], bool). The on-chain
// signature takes a fixed-size array, so the selector depends on N; go-ethereum's
// abi.ABI.Pack of a dynamic uint256[] would compute the wrong selector. We build
// the selector and the uint256[N] argument type by N instead. Arguments.Pack
// accepts the []*big.Int slice directly for a uint256[N] arg (it length-checks
// against N), so no fixed-array conversion is needed.
func packCalcTokenAmount(amounts []*big.Int, isDeposit bool) ([]byte, error) {
	n := len(amounts)
	arrT, err := uint256ArrayType(n)
	if err != nil {
		return nil, fmt.Errorf("calc_token_amount arg type: %w", err)
	}
	boolT, err := abi.NewType("bool", "", nil)
	if err != nil {
		return nil, fmt.Errorf("calc_token_amount bool type: %w", err)
	}
	args := abi.Arguments{{Name: "amounts", Type: arrT}, {Name: "is_deposit", Type: boolT}}
	body, err := args.Pack(amounts, isDeposit)
	if err != nil {
		return nil, fmt.Errorf("packing calc_token_amount args: %w", err)
	}
	return append(calcTokenAmountSelector(n), body...), nil
}

// calcTokenAmountSelector returns the 4-byte selector for calc_token_amount(uint256[N],bool).
func calcTokenAmountSelector(n int) []byte {
	sig := fmt.Sprintf("calc_token_amount(uint256[%d],bool)", n)
	return gocrypto.Keccak256([]byte(sig))[:4]
}

// optionalUintResult decodes an AllowFailure=true scalar snapshot result. A
// revert is an error, not a nil field: per the no-swallowed-errors rule a
// best-effort read that reverted must stop the block rather than be silently
// turned into a NULL column. The only legitimately-absent reads are gated
// structurally (not issued) rather than swallowed here.
func optionalUintResult(abiDef *abi.ABI, method string, r outbound.Result, poolAddr common.Address, blockNumber int64) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("snapshot call %s reverted for pool %s at block %d", method, poolAddr, blockNumber)
	}
	return shared.UnpackUint(abiDef, method, r)
}

// unpackSingleUint decodes a single uint256-returning result whose method is not
// in the ABI (e.g. calc_token_amount, which we pack manually per N). A reverted
// call or a payload shorter than 32 bytes is an error.
func unpackSingleUint(r outbound.Result) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("uint256 call reverted")
	}
	u256T, err := uint256Type()
	if err != nil {
		return nil, fmt.Errorf("uint256 type: %w", err)
	}
	args := abi.Arguments{{Type: u256T}}
	vals, err := args.Unpack(r.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking uint256: %w", err)
	}
	return asBigInt(vals, 0)
}

// unpackUintArray decodes a uint256[N]-returning view method's result into a
// []*big.Int of length n. A reverted sub-call or an undecodable payload is an error.
func unpackUintArray(r outbound.Result, n int) ([]*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("uint256[%d] call reverted", n)
	}
	arrT, err := uint256ArrayType(n)
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

// parseHexUint parses a 0x-prefixed hex string into a uint, as used in
// shared.Log.LogIndex.
func parseHexUint(s string) (uint, error) {
	if !strings.HasPrefix(s, "0x") || len(s) == 2 {
		return 0, fmt.Errorf("invalid hex uint %q", s)
	}
	n, err := strconv.ParseUint(s[2:], 16, strconv.IntSize)
	if err != nil {
		return 0, fmt.Errorf("parsing hex uint %q: %w", s, err)
	}
	return uint(n), nil
}
