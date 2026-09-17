package curveindexer

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gocrypto "github.com/ethereum/go-ethereum/crypto"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// logBelongsToPool reports whether a log emitted by addr should be decoded for
// pool. It matches the pool's own address and, for pre-NG pools whose LP token is
// a separate contract, the LP-token contract address (so its Transfer/Approval
// logs route to the owning pool).
func logBelongsToPool(addr common.Address, pool RegisteredPool) bool {
	if pool.LpTokenAddress != nil {
		return shared.LogBelongsTo(addr, pool.Address, *pool.LpTokenAddress)
	}
	return shared.LogBelongsTo(addr, pool.Address)
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

// packCalcTokenAmount packs calc_token_amount(<array>, bool), where <array> is
// uint256[N] when dynArray is false and uint256[] when it is. Curve splits on
// this by implementation -- the pre-NG pools, the cryptoswap pools and the
// original NG pools take the fixed array, later NG implementations take a
// DynArray and REVERT on the fixed selector -- and the argument shape cannot be
// discovered from a return value, so it is curated per pool
// (curve_pool.calc_token_amount_dyn_array). Both selector and argument type are
// built here because go-ethereum's abi.ABI.Pack would compute only one of them.
// Arguments.Pack accepts the []*big.Int slice for either arg type (it
// length-checks against N for the fixed one), so no array conversion is needed.
func packCalcTokenAmount(amounts []*big.Int, isDeposit, dynArray bool) ([]byte, error) {
	n := len(amounts)
	arrT, err := calcTokenAmountArgType(n, dynArray)
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
	return append(calcTokenAmountSelector(n, dynArray), body...), nil
}

func calcTokenAmountArgType(n int, dynArray bool) (abi.Type, error) {
	if dynArray {
		return abi.NewType("uint256[]", "", nil)
	}
	return uint256ArrayType(n)
}

// calcTokenAmountSelector returns the 4-byte selector for
// calc_token_amount(uint256[N],bool) or calc_token_amount(uint256[],bool).
func calcTokenAmountSelector(n int, dynArray bool) []byte {
	sig := fmt.Sprintf("calc_token_amount(uint256[%d],bool)", n)
	if dynArray {
		sig = "calc_token_amount(uint256[],bool)"
	}
	return gocrypto.Keccak256([]byte(sig))[:4]
}
