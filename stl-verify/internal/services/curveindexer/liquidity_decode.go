package curveindexer

import (
	"fmt"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gocrypto "github.com/ethereum/go-ethereum/crypto"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// eventTopic0 returns the Keccak256 hash of a canonical event signature string.
func eventTopic0(sig string) common.Hash {
	return gocrypto.Keccak256Hash([]byte(sig))
}

// uint256ArrayType returns an abi.Type for uint256[n].
func uint256ArrayType(n int) (abi.Type, error) {
	return abi.NewType(fmt.Sprintf("uint256[%d]", n), "", nil)
}

// uint256Type returns an abi.Type for uint256.
func uint256Type() (abi.Type, error) {
	return abi.NewType("uint256", "", nil)
}

// asBigInt reads the i-th unpacked ABI value as a *big.Int, erroring (not panicking)
// on an unexpected type.
func asBigInt(vals []any, i int) (*big.Int, error) {
	if i >= len(vals) {
		return nil, fmt.Errorf("abi value index %d out of range (len %d)", i, len(vals))
	}
	bi, ok := vals[i].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("abi value %d is %T, not *big.Int", i, vals[i])
	}
	return bi, nil
}

// toBigIntSlice converts a Go fixed-size array or slice of *big.Int (as returned
// by abi.Arguments.Unpack for uint256[N]) into []*big.Int.
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

// classicLiquiditySigs holds the precomputed topic0 hashes for all classic (pre-NG)
// fixed-array liquidity event signatures, keyed by N (number of coins).
type classicLiquiditySigs struct {
	addLiquidity             common.Hash
	removeLiquidity          common.Hash
	removeLiquidityOne       common.Hash
	removeLiquidityImbalance common.Hash
}

func buildClassicSigs(n int) classicLiquiditySigs {
	return classicLiquiditySigs{
		addLiquidity:             eventTopic0(fmt.Sprintf("AddLiquidity(address,uint256[%d],uint256[%d],uint256,uint256)", n, n)),
		removeLiquidity:          eventTopic0(fmt.Sprintf("RemoveLiquidity(address,uint256[%d],uint256[%d],uint256)", n, n)),
		removeLiquidityOne:       eventTopic0("RemoveLiquidityOne(address,uint256,uint256)"),
		removeLiquidityImbalance: eventTopic0(fmt.Sprintf("RemoveLiquidityImbalance(address,uint256[%d],uint256[%d],uint256,uint256)", n, n)),
	}
}

// cryptoswapLiquiditySigs holds the precomputed topic0 hashes for cryptoswap
// fixed-array liquidity event signatures, keyed by N (number of coins).
type cryptoswapLiquiditySigs struct {
	addLiquidity       common.Hash
	removeLiquidity    common.Hash
	removeLiquidityOne common.Hash
}

func buildCryptoswapSigs(n int) cryptoswapLiquiditySigs {
	return cryptoswapLiquiditySigs{
		addLiquidity:       eventTopic0(fmt.Sprintf("AddLiquidity(address,uint256[%d],uint256,uint256,uint256)", n)),
		removeLiquidity:    eventTopic0(fmt.Sprintf("RemoveLiquidity(address,uint256[%d],uint256)", n)),
		removeLiquidityOne: eventTopic0("RemoveLiquidityOne(address,uint256,uint256,uint256,uint256,uint256)"),
	}
}

// decodeClassicLiquidity attempts to decode a log from a pre-NG classic stableswap pool.
// It returns (record, true, nil) on a match, (nil, false, nil) when topic0 does not
// match any classic liquidity signature, and (nil, false, err) when topic0 matches
// but the word layout is invalid.
func decodeClassicLiquidity(log shared.Log, pool RegisteredPool) (*LiquidityRecord, bool, error) {
	if len(log.Topics) == 0 {
		return nil, false, nil
	}

	topic0 := common.HexToHash(log.Topics[0])
	n := pool.NCoins
	sigs := buildClassicSigs(n)

	// provider is indexed (Topics[1]); once topic0 matches a known liquidity
	// signature, its absence is a malformed log and must fail decoding rather
	// than silently fall through to the capture net.
	if len(log.Topics) < 2 {
		switch topic0 {
		case sigs.addLiquidity, sigs.removeLiquidity, sigs.removeLiquidityOne, sigs.removeLiquidityImbalance:
			return nil, false, fmt.Errorf("matched classic liquidity signature %s but log is missing the indexed provider topic", topic0)
		default:
			return nil, false, nil
		}
	}
	provider := common.HexToAddress(log.Topics[1])
	txHash := common.HexToHash(log.TransactionHash)
	logIndex, err := parseHexUint(log.LogIndex)
	if err != nil {
		return nil, false, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}

	raw := common.FromHex(log.Data)

	switch topic0 {
	case sigs.addLiquidity:
		// non-indexed: token_amounts[N], fees[N], invariant, token_supply
		arrT, err := uint256ArrayType(n)
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity unpack: %w", err)
		}
		u256T, err := uint256Type()
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity unpack: %w", err)
		}
		args := abi.Arguments{
			{Name: "token_amounts", Type: arrT},
			{Name: "fees", Type: arrT},
			{Name: "invariant", Type: u256T},
			{Name: "token_supply", Type: u256T},
		}
		vals, err := args.Unpack(raw)
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity unpack: %w", err)
		}
		amounts, err := toBigIntSlice(vals[0])
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity unpack: %w", err)
		}
		fees, err := toBigIntSlice(vals[1])
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity unpack: %w", err)
		}
		invariant, err := asBigInt(vals, 2)
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity invariant: %w", err)
		}
		tokenSupply, err := asBigInt(vals, 3)
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity token_supply: %w", err)
		}
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityAdd,
			TokenAmounts: amounts,
			Fees:         fees,
			Invariant:    invariant,
			TokenSupply:  tokenSupply,
		}
		return rec, true, nil

	case sigs.removeLiquidity:
		// non-indexed: token_amounts[N], fees[N], token_supply
		arrT, err := uint256ArrayType(n)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidity unpack: %w", err)
		}
		u256T, err := uint256Type()
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidity unpack: %w", err)
		}
		args := abi.Arguments{
			{Name: "token_amounts", Type: arrT},
			{Name: "fees", Type: arrT},
			{Name: "token_supply", Type: u256T},
		}
		vals, err := args.Unpack(raw)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidity unpack: %w", err)
		}
		amounts, err := toBigIntSlice(vals[0])
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidity unpack: %w", err)
		}
		fees, err := toBigIntSlice(vals[1])
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidity unpack: %w", err)
		}
		tokenSupply, err := asBigInt(vals, 2)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidity token_supply: %w", err)
		}
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemove,
			TokenAmounts: amounts,
			Fees:         fees,
			TokenSupply:  tokenSupply,
		}
		return rec, true, nil

	case sigs.removeLiquidityOne:
		// non-indexed: token_amount, coin_amount (no coin_index in classic)
		u256T, err := uint256Type()
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityOne unpack: %w", err)
		}
		args := abi.Arguments{
			{Name: "token_amount", Type: u256T},
			{Name: "coin_amount", Type: u256T},
		}
		vals, err := args.Unpack(raw)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityOne unpack: %w", err)
		}
		tokenAmount, err := asBigInt(vals, 0)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityOne token_amount: %w", err)
		}
		coinAmount, err := asBigInt(vals, 1)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityOne coin_amount: %w", err)
		}
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemoveOne,
			TokenAmounts: []*big.Int{tokenAmount, coinAmount},
		}
		return rec, true, nil

	case sigs.removeLiquidityImbalance:
		// non-indexed: token_amounts[N], fees[N], invariant, token_supply
		arrT, err := uint256ArrayType(n)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance unpack: %w", err)
		}
		u256T, err := uint256Type()
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance unpack: %w", err)
		}
		args := abi.Arguments{
			{Name: "token_amounts", Type: arrT},
			{Name: "fees", Type: arrT},
			{Name: "invariant", Type: u256T},
			{Name: "token_supply", Type: u256T},
		}
		vals, err := args.Unpack(raw)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance unpack: %w", err)
		}
		amounts, err := toBigIntSlice(vals[0])
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance unpack: %w", err)
		}
		fees, err := toBigIntSlice(vals[1])
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance unpack: %w", err)
		}
		invariant, err := asBigInt(vals, 2)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance invariant: %w", err)
		}
		tokenSupply, err := asBigInt(vals, 3)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance token_supply: %w", err)
		}
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemoveImbalance,
			TokenAmounts: amounts,
			Fees:         fees,
			Invariant:    invariant,
			TokenSupply:  tokenSupply,
		}
		return rec, true, nil
	}

	return nil, false, nil
}

// decodeCryptoLiquidity attempts to decode a log from a cryptoswap pool.
// It returns (record, true, nil) on a match, (nil, false, nil) when topic0 does not
// match any cryptoswap liquidity signature, and (nil, false, err) when topic0 matches
// but the word layout is invalid.
func decodeCryptoLiquidity(log shared.Log, pool RegisteredPool) (*LiquidityRecord, bool, error) {
	if len(log.Topics) == 0 {
		return nil, false, nil
	}

	topic0 := common.HexToHash(log.Topics[0])
	n := pool.NCoins
	sigs := buildCryptoswapSigs(n)

	// provider is indexed (Topics[1]); once topic0 matches a known cryptoswap liquidity
	// signature, its absence is a malformed log and must fail decoding rather
	// than silently fall through to the capture net.
	if len(log.Topics) < 2 {
		switch topic0 {
		case sigs.addLiquidity, sigs.removeLiquidity, sigs.removeLiquidityOne:
			return nil, false, fmt.Errorf("matched cryptoswap liquidity signature %s but log is missing the indexed provider topic", topic0)
		default:
			return nil, false, nil
		}
	}
	provider := common.HexToAddress(log.Topics[1])
	txHash := common.HexToHash(log.TransactionHash)
	logIndex, err := parseHexUint(log.LogIndex)
	if err != nil {
		return nil, false, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}

	raw := common.FromHex(log.Data)

	switch topic0 {
	case sigs.addLiquidity:
		// non-indexed: token_amounts[N], fee, token_supply, packed_price_scale (ignored)
		arrT, err := uint256ArrayType(n)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity unpack: %w", err)
		}
		u256T, err := uint256Type()
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity unpack: %w", err)
		}
		args := abi.Arguments{
			{Name: "token_amounts", Type: arrT},
			{Name: "fee", Type: u256T},
			{Name: "token_supply", Type: u256T},
			{Name: "packed_price_scale", Type: u256T},
		}
		vals, err := args.Unpack(raw)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity unpack: %w", err)
		}
		amounts, err := toBigIntSlice(vals[0])
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity unpack: %w", err)
		}
		fee, err := asBigInt(vals, 1)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity fee: %w", err)
		}
		tokenSupply, err := asBigInt(vals, 2)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity token_supply: %w", err)
		}
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityAdd,
			TokenAmounts: amounts,
			Fees:         []*big.Int{fee},
			TokenSupply:  tokenSupply,
		}
		return rec, true, nil

	case sigs.removeLiquidity:
		// non-indexed: token_amounts[N], token_supply
		arrT, err := uint256ArrayType(n)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidity unpack: %w", err)
		}
		u256T, err := uint256Type()
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidity unpack: %w", err)
		}
		args := abi.Arguments{
			{Name: "token_amounts", Type: arrT},
			{Name: "token_supply", Type: u256T},
		}
		vals, err := args.Unpack(raw)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidity unpack: %w", err)
		}
		amounts, err := toBigIntSlice(vals[0])
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidity unpack: %w", err)
		}
		tokenSupply, err := asBigInt(vals, 1)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidity token_supply: %w", err)
		}
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemove,
			TokenAmounts: amounts,
			TokenSupply:  tokenSupply,
		}
		return rec, true, nil

	case sigs.removeLiquidityOne:
		// non-indexed: token_amount, coin_index, coin_amount, approx_fee (ignored), packed_price_scale (ignored)
		u256T, err := uint256Type()
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidityOne unpack: %w", err)
		}
		args := abi.Arguments{
			{Name: "token_amount", Type: u256T},
			{Name: "coin_index", Type: u256T},
			{Name: "coin_amount", Type: u256T},
			{Name: "approx_fee", Type: u256T},
			{Name: "packed_price_scale", Type: u256T},
		}
		vals, err := args.Unpack(raw)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidityOne unpack: %w", err)
		}
		tokenAmount, err := asBigInt(vals, 0)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidityOne token_amount: %w", err)
		}
		coinIdxBig, err := asBigInt(vals, 1)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidityOne coin_index: %w", err)
		}
		coinAmount, err := asBigInt(vals, 2)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidityOne coin_amount: %w", err)
		}
		coinIdx := int(coinIdxBig.Int64())
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemoveOne,
			TokenAmounts: []*big.Int{tokenAmount, coinAmount},
			CoinIndex:    &coinIdx,
		}
		return rec, true, nil
	}

	return nil, false, nil
}
