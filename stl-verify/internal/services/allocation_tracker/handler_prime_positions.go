package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type PrimePositionHandler struct {
	repo        outbound.AllocationRepository
	metadata    *metadataCache
	primeLookup map[string]int64 // star name → prime.id
	logger      *slog.Logger
}

func NewPrimePositionHandler(
	repo outbound.AllocationRepository,
	multicaller outbound.Multicaller,
	erc20ABI *abi.ABI,
	primeLookup map[string]int64,
	logger *slog.Logger,
) *PrimePositionHandler {
	return &PrimePositionHandler{
		repo:        repo,
		metadata:    newMetadataCache(multicaller, erc20ABI, logger),
		primeLookup: primeLookup,
		logger:      logger.With("component", "postgres-handler"),
	}
}

func (h *PrimePositionHandler) HandleSnapshots(
	ctx context.Context,
	snapshots []*PositionSnapshot,
) error {
	if len(snapshots) == 0 {
		return nil
	}

	blockNum := snapshots[0].BlockNumber

	// Token types where the contract itself isn't ERC20-compatible
	// (e.g. Uniswap V3 pool contracts don't have decimals/symbol).
	// For these, we use the AssetAddress metadata instead.
	nonERC20Types := map[string]bool{
		"uni_v3_pool": true,
		"uni_v3_lp":   true,
	}

	var addrs []common.Address
	for _, s := range snapshots {
		if nonERC20Types[s.Entry.TokenType] {
			// Only fetch metadata for the asset, not the pool contract.
			if s.Entry.AssetAddress != nil {
				addrs = append(addrs, *s.Entry.AssetAddress)
			}
			continue
		}
		addrs = append(addrs, s.Entry.ContractAddress)
		if s.Entry.AssetAddress != nil {
			addrs = append(addrs, *s.Entry.AssetAddress)
		}
	}
	if err := h.metadata.fetchMissing(ctx, addrs, blockNum); err != nil {
		return fmt.Errorf("metadata fetch: %w", err)
	}

	positions := make([]*entity.AllocationPosition, 0, len(snapshots))
	for _, s := range snapshots {
		var meta tokenMeta
		var ok bool

		if nonERC20Types[s.Entry.TokenType] {
			// Use asset metadata for non-ERC20 contract types.
			if s.Entry.AssetAddress == nil {
				return fmt.Errorf(
					"uni_v3 entry %s has no asset address",
					s.Entry.ContractAddress.Hex(),
				)
			}
			meta, ok = h.metadata.get(*s.Entry.AssetAddress)
			if !ok {
				return fmt.Errorf(
					"metadata missing for asset %s (pool %s)",
					s.Entry.AssetAddress.Hex(),
					s.Entry.ContractAddress.Hex(),
				)
			}
		} else {
			meta, ok = h.metadata.get(s.Entry.ContractAddress)
			if !ok {
				return fmt.Errorf(
					"metadata missing for token %s",
					s.Entry.ContractAddress.Hex(),
				)
			}
		}

		primeID, ok := h.primeLookup[s.Entry.Star]
		if !ok {
			return fmt.Errorf("unknown star %q: no matching prime_id", s.Entry.Star)
		}

		var createdAtBlock int64
		if s.Entry.CreatedAtBlock != nil {
			createdAtBlock = *s.Entry.CreatedAtBlock
		}

		positions = append(positions, &entity.AllocationPosition{
			ChainID:        s.ChainID,
			TokenAddress:   s.Entry.ContractAddress,
			TokenSymbol:    meta.symbol,
			TokenDecimals:  meta.decimals,
			PrimeID:        primeID,
			ProxyAddress:   s.Entry.WalletAddress,
			Balance:        s.Balance,
			ScaledBalance:  s.ScaledBalance,
			BlockNumber:    s.BlockNumber,
			BlockVersion:   s.BlockVersion,
			TxHash:         s.TxHash,
			LogIndex:       s.LogIndex,
			TxAmount:       s.TxAmount,
			Direction:      string(s.Direction),
			CreatedAtBlock: createdAtBlock,
			CreatedAt:      s.BlockTimestamp,
		})
	}

	return h.repo.SavePositions(ctx, positions)
}

type tokenMeta struct {
	symbol   string
	decimals int
}

type metadataCache struct {
	mu          sync.RWMutex
	cache       map[common.Address]tokenMeta
	multicaller outbound.Multicaller
	erc20ABI    *abi.ABI
	logger      *slog.Logger
}

func newMetadataCache(
	multicaller outbound.Multicaller,
	erc20ABI *abi.ABI,
	logger *slog.Logger,
) *metadataCache {
	return &metadataCache{
		cache:       make(map[common.Address]tokenMeta),
		multicaller: multicaller,
		erc20ABI:    erc20ABI,
		logger:      logger.With("component", "metadata-cache"),
	}
}

func (c *metadataCache) get(addr common.Address) (tokenMeta, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m, ok := c.cache[addr]
	return m, ok
}

func (c *metadataCache) fetchMissing(
	ctx context.Context,
	addrs []common.Address,
	blockNumber int64,
) error {
	uniqueAddresses := make(map[common.Address]bool)
	var missing []common.Address

	c.mu.RLock()
	for _, addr := range addrs {
		if _, ok := c.cache[addr]; ok {
			continue
		}
		if !uniqueAddresses[addr] {
			uniqueAddresses[addr] = true
			missing = append(missing, addr)
		}
	}
	c.mu.RUnlock()

	if len(missing) == 0 {
		return nil
	}

	calls := make([]outbound.Call, 0, len(missing)*2)
	for _, addr := range missing {
		decimalsData, _ := c.erc20ABI.Pack("decimals")
		symbolData, _ := c.erc20ABI.Pack("symbol")
		calls = append(calls,
			outbound.Call{
				Target:       addr,
				AllowFailure: true,
				CallData:     decimalsData,
			},
			outbound.Call{
				Target:       addr,
				AllowFailure: true,
				CallData:     symbolData,
			},
		)
	}

	block := big.NewInt(blockNumber)
	results, err := c.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return fmt.Errorf("multicall metadata: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for i, addr := range missing {
		decIdx := i * 2
		symIdx := i*2 + 1

		decimals := -1
		if decIdx < len(results) &&
			results[decIdx].Success &&
			len(results[decIdx].ReturnData) > 0 {
			unpacked, err := c.erc20ABI.Unpack(
				"decimals", results[decIdx].ReturnData,
			)
			if err == nil && len(unpacked) > 0 {
				if d, ok := unpacked[0].(uint8); ok {
					decimals = int(d)
				}
			}
		}

		if decimals < 0 {
			return fmt.Errorf("decimals fetch failed for %s", addr.Hex())
		}

		symbol := "UNKNOWN"
		if symIdx < len(results) &&
			results[symIdx].Success &&
			len(results[symIdx].ReturnData) > 0 {
			unpacked, err := c.erc20ABI.Unpack(
				"symbol", results[symIdx].ReturnData,
			)
			if err == nil && len(unpacked) > 0 {
				if s, ok := unpacked[0].(string); ok {
					symbol = s
				}
			}
		}

		c.cache[addr] = tokenMeta{symbol: symbol, decimals: decimals}
		c.logger.Debug("cached metadata",
			"address", addr.Hex(),
			"symbol", symbol,
			"decimals", decimals)
	}

	return nil
}
