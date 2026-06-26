package curveindexer

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	gocrypto "github.com/ethereum/go-ethereum/crypto"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// eventTopic0 returns the Keccak256 hash of a canonical event signature string.
func eventTopic0(sig string) common.Hash {
	return gocrypto.Keccak256Hash([]byte(sig))
}

// wordsFromData splits a hex-encoded log data field into 32-byte big-endian words.
// Returns an error if the byte length is not a multiple of 32.
func wordsFromData(dataHex string) ([]*big.Int, error) {
	b := common.FromHex(dataHex)
	if len(b)%32 != 0 {
		return nil, fmt.Errorf("data length %d is not a multiple of 32", len(b))
	}
	words := make([]*big.Int, len(b)/32)
	for i := range words {
		words[i] = new(big.Int).SetBytes(b[i*32 : (i+1)*32])
	}
	return words, nil
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

	// provider is indexed (Topics[1]); if absent the log is malformed and can't match.
	if len(log.Topics) < 2 {
		return nil, false, nil
	}
	provider := common.HexToAddress(log.Topics[1])
	txHash := common.HexToHash(log.TransactionHash)
	logIndex, err := parseHexUint(log.LogIndex)
	if err != nil {
		return nil, false, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}

	switch topic0 {
	case sigs.addLiquidity:
		// word layout: token_amounts[0..N-1], fees[N..2N-1], invariant[2N], token_supply[2N+1]
		words, err := wordsFromData(log.Data)
		if err != nil {
			return nil, false, fmt.Errorf("classic AddLiquidity word decode: %w", err)
		}
		if len(words) < 2*n+2 {
			return nil, false, fmt.Errorf("classic AddLiquidity: expected %d words, got %d", 2*n+2, len(words))
		}
		amounts := make([]*big.Int, n)
		copy(amounts, words[0:n])
		fees := make([]*big.Int, n)
		copy(fees, words[n:2*n])
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityAdd,
			TokenAmounts: amounts,
			Fees:         fees,
			Invariant:    words[2*n],
			TokenSupply:  words[2*n+1],
		}
		return rec, true, nil

	case sigs.removeLiquidity:
		// word layout: token_amounts[0..N-1], fees[N..2N-1], token_supply[2N]
		words, err := wordsFromData(log.Data)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidity word decode: %w", err)
		}
		if len(words) < 2*n+1 {
			return nil, false, fmt.Errorf("classic RemoveLiquidity: expected %d words, got %d", 2*n+1, len(words))
		}
		amounts := make([]*big.Int, n)
		copy(amounts, words[0:n])
		fees := make([]*big.Int, n)
		copy(fees, words[n:2*n])
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemove,
			TokenAmounts: amounts,
			Fees:         fees,
			TokenSupply:  words[2*n],
		}
		return rec, true, nil

	case sigs.removeLiquidityOne:
		// word layout: token_amount[0] (LP burned), coin_amount[1]; no coin_index in classic
		words, err := wordsFromData(log.Data)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityOne word decode: %w", err)
		}
		if len(words) < 2 {
			return nil, false, fmt.Errorf("classic RemoveLiquidityOne: expected 2 words, got %d", len(words))
		}
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemoveOne,
			TokenAmounts: []*big.Int{words[0], words[1]},
		}
		return rec, true, nil

	case sigs.removeLiquidityImbalance:
		// word layout: token_amounts[0..N-1], fees[N..2N-1], invariant[2N], token_supply[2N+1]
		words, err := wordsFromData(log.Data)
		if err != nil {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance word decode: %w", err)
		}
		if len(words) < 2*n+2 {
			return nil, false, fmt.Errorf("classic RemoveLiquidityImbalance: expected %d words, got %d", 2*n+2, len(words))
		}
		amounts := make([]*big.Int, n)
		copy(amounts, words[0:n])
		fees := make([]*big.Int, n)
		copy(fees, words[n:2*n])
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemoveImbalance,
			TokenAmounts: amounts,
			Fees:         fees,
			Invariant:    words[2*n],
			TokenSupply:  words[2*n+1],
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

	// provider is indexed (Topics[1]); if absent the log is malformed and can't match.
	if len(log.Topics) < 2 {
		return nil, false, nil
	}
	provider := common.HexToAddress(log.Topics[1])
	txHash := common.HexToHash(log.TransactionHash)
	logIndex, err := parseHexUint(log.LogIndex)
	if err != nil {
		return nil, false, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}

	switch topic0 {
	case sigs.addLiquidity:
		// word layout: token_amounts[0..N-1], fee[N], token_supply[N+1], packed_price_scale[N+2] (ignored)
		words, err := wordsFromData(log.Data)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity word decode: %w", err)
		}
		if len(words) < n+2 {
			return nil, false, fmt.Errorf("cryptoswap AddLiquidity: expected %d words, got %d", n+2, len(words))
		}
		amounts := make([]*big.Int, n)
		copy(amounts, words[0:n])
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityAdd,
			TokenAmounts: amounts,
			Fees:         []*big.Int{words[n]},
			TokenSupply:  words[n+1],
		}
		return rec, true, nil

	case sigs.removeLiquidity:
		// word layout: token_amounts[0..N-1], token_supply[N]
		words, err := wordsFromData(log.Data)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidity word decode: %w", err)
		}
		if len(words) < n+1 {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidity: expected %d words, got %d", n+1, len(words))
		}
		amounts := make([]*big.Int, n)
		copy(amounts, words[0:n])
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemove,
			TokenAmounts: amounts,
			TokenSupply:  words[n],
		}
		return rec, true, nil

	case sigs.removeLiquidityOne:
		// word layout: token_amount[0] (LP burned), coin_index[1], coin_amount[2],
		// approx_fee[3] (ignored), packed_price_scale[4] (ignored)
		words, err := wordsFromData(log.Data)
		if err != nil {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidityOne word decode: %w", err)
		}
		if len(words) < 3 {
			return nil, false, fmt.Errorf("cryptoswap RemoveLiquidityOne: expected 3+ words, got %d", len(words))
		}
		coinIdx := int(words[1].Int64())
		rec := &LiquidityRecord{
			Pool:         pool,
			LogIndex:     logIndex,
			TxHash:       txHash,
			Provider:     provider,
			Kind:         LiquidityRemoveOne,
			TokenAmounts: []*big.Int{words[0], words[2]},
			CoinIndex:    &coinIdx,
		}
		return rec, true, nil
	}

	return nil, false, nil
}
