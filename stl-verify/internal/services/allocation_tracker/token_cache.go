package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// TokenMetadata holds ERC20 token metadata.
type TokenMetadata struct {
	Symbol   string
	Decimals int
	Name     string
}

// BalanceKey uniquely identifies a (token, holder) pair for balance lookups.
type BalanceKey struct {
	Token  common.Address
	Holder common.Address
}

// TokenCache fetches and caches ERC20 token metadata, and provides
// on-chain balance lookups via multicall.
type TokenCache struct {
	mu              sync.RWMutex
	cache           map[common.Address]TokenMetadata
	multicallClient outbound.Multicaller
	erc20ABI        *abi.ABI
	logger          *slog.Logger
}

func NewTokenCache(multicallClient outbound.Multicaller, erc20ABI *abi.ABI, logger *slog.Logger) *TokenCache {
	return &TokenCache{
		cache:           make(map[common.Address]TokenMetadata),
		multicallClient: multicallClient,
		erc20ABI:        erc20ABI,
		logger:          logger.With("component", "token-cache"),
	}
}

// Get returns cached metadata, or ok=false if not cached.
func (c *TokenCache) Get(token common.Address) (TokenMetadata, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.cache[token]
	return m, ok
}

// FetchMissing fetches metadata for any tokens not already in the cache.
func (c *TokenCache) FetchMissing(ctx context.Context, tokens []common.Address) (map[common.Address]TokenMetadata, error) {
	result := make(map[common.Address]TokenMetadata, len(tokens))
	var toFetch []common.Address

	c.mu.RLock()
	for _, token := range tokens {
		if m, ok := c.cache[token]; ok {
			result[token] = m
		} else {
			toFetch = append(toFetch, token)
		}
	}
	c.mu.RUnlock()

	if len(toFetch) == 0 {
		return result, nil
	}

	fetched, err := c.batchFetchMetadata(ctx, toFetch)
	if err != nil {
		return result, fmt.Errorf("batch fetch metadata: %w", err)
	}

	c.mu.Lock()
	for addr, m := range fetched {
		c.cache[addr] = m
		result[addr] = m
	}
	c.mu.Unlock()

	return result, nil
}

// FetchBalances calls balanceOf for each (token, holder) pair at the given block.
func (c *TokenCache) FetchBalances(ctx context.Context, keys []BalanceKey, blockNumber int64) (map[BalanceKey]*big.Int, error) {
	if len(keys) == 0 {
		return make(map[BalanceKey]*big.Int), nil
	}

	calls := make([]outbound.Call, 0, len(keys))
	for _, key := range keys {
		callData, err := c.erc20ABI.Pack("balanceOf", key.Holder)
		if err != nil {
			c.logger.Warn("failed to pack balanceOf", "token", key.Token.Hex(), "holder", key.Holder.Hex(), "error", err)
			continue
		}
		calls = append(calls, outbound.Call{
			Target:       key.Token,
			AllowFailure: true,
			CallData:     callData,
		})
	}

	if len(calls) == 0 {
		return make(map[BalanceKey]*big.Int), nil
	}

	results, err := c.multicallClient.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf failed: %w", err)
	}

	balances := make(map[BalanceKey]*big.Int, len(keys))
	for i, key := range keys {
		if i >= len(results) {
			break
		}
		if !results[i].Success || len(results[i].ReturnData) == 0 {
			c.logger.Debug("balanceOf call failed", "token", key.Token.Hex(), "holder", key.Holder.Hex())
			balances[key] = big.NewInt(0)
			continue
		}

		unpacked, err := c.erc20ABI.Unpack("balanceOf", results[i].ReturnData)
		if err != nil || len(unpacked) == 0 {
			c.logger.Warn("failed to unpack balanceOf", "token", key.Token.Hex(), "error", err)
			balances[key] = big.NewInt(0)
			continue
		}

		bal, ok := unpacked[0].(*big.Int)
		if !ok {
			c.logger.Warn("unexpected balanceOf return type", "token", key.Token.Hex(), "type", fmt.Sprintf("%T", unpacked[0]))
			balances[key] = big.NewInt(0)
			continue
		}

		balances[key] = bal
	}

	return balances, nil
}

func (c *TokenCache) batchFetchMetadata(ctx context.Context, tokens []common.Address) (map[common.Address]TokenMetadata, error) {
	calls := make([]outbound.Call, 0, len(tokens)*3)
	for _, token := range tokens {
		decimalsData, _ := c.erc20ABI.Pack("decimals")
		symbolData, _ := c.erc20ABI.Pack("symbol")
		nameData, _ := c.erc20ABI.Pack("name")

		calls = append(calls,
			outbound.Call{Target: token, AllowFailure: true, CallData: decimalsData},
			outbound.Call{Target: token, AllowFailure: true, CallData: symbolData},
			outbound.Call{Target: token, AllowFailure: true, CallData: nameData},
		)
	}

	results, err := c.multicallClient.Execute(ctx, calls, nil)
	if err != nil {
		return nil, fmt.Errorf("multicall failed: %w", err)
	}

	fetched := make(map[common.Address]TokenMetadata, len(tokens))
	for i, token := range tokens {
		decimalsIdx := i * 3
		symbolIdx := i*3 + 1
		nameIdx := i*3 + 2

		var metadata TokenMetadata

		if results[decimalsIdx].Success && len(results[decimalsIdx].ReturnData) > 0 {
			unpacked, err := c.erc20ABI.Unpack("decimals", results[decimalsIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if d, ok := unpacked[0].(uint8); ok {
					metadata.Decimals = int(d)
				}
			}
		}

		if results[symbolIdx].Success && len(results[symbolIdx].ReturnData) > 0 {
			unpacked, err := c.erc20ABI.Unpack("symbol", results[symbolIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if s, ok := unpacked[0].(string); ok {
					metadata.Symbol = s
				}
			}
		}

		if results[nameIdx].Success && len(results[nameIdx].ReturnData) > 0 {
			unpacked, err := c.erc20ABI.Unpack("name", results[nameIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if n, ok := unpacked[0].(string); ok {
					metadata.Name = n
				}
			}
		}

		if metadata.Symbol == "" {
			c.logger.Warn("could not fetch token symbol", "token", token.Hex())
		}

		fetched[token] = metadata
	}

	return fetched, nil
}
