package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// PostgresHandler converts PositionSnapshots to AllocationPositions and persists them.
type PostgresHandler struct {
	repo     outbound.AllocationRepository
	metadata *metadataCache
	logger   *slog.Logger
}

func NewPostgresHandler(
	repo outbound.AllocationRepository,
	multicaller outbound.Multicaller,
	erc20ABI *abi.ABI,
	logger *slog.Logger,
) *PostgresHandler {
	return &PostgresHandler{
		repo:     repo,
		metadata: newMetadataCache(multicaller, erc20ABI, logger),
		logger:   logger.With("component", "postgres-handler"),
	}
}

func (h *PostgresHandler) HandleSnapshots(ctx context.Context, snapshots []*PositionSnapshot) error {
	if len(snapshots) == 0 {
		return nil
	}

	// Fetch token metadata for contracts AND underlying assets
	var addrs []common.Address
	for _, s := range snapshots {
		addrs = append(addrs, s.Entry.ContractAddress)
		if s.Entry.AssetAddress != nil {
			addrs = append(addrs, *s.Entry.AssetAddress)
		}
	}
	if err := h.metadata.fetchMissing(ctx, addrs); err != nil {
		h.logger.Warn("metadata fetch partial failure", "error", err)
	}

	// Convert to AllocationPositions
	positions := make([]*outbound.AllocationPosition, 0, len(snapshots))
	for _, s := range snapshots {
		meta := h.metadata.getOrDefault(s.Entry.ContractAddress)

		// For erc4626: Balance is in underlying asset units, use asset decimals
		// For everything else: Balance is in token units, use token decimals
		assetDecimals := meta.decimals
		if s.Entry.AssetAddress != nil {
			assetMeta := h.metadata.getOrDefault(*s.Entry.AssetAddress)
			assetDecimals = assetMeta.decimals
		}

		var createdAtBlock int64
		if s.Entry.CreatedAtBlock != nil {
			createdAtBlock = *s.Entry.CreatedAtBlock
		}

		positions = append(positions, &outbound.AllocationPosition{
			ChainID:        s.ChainID,
			TokenAddress:   s.Entry.ContractAddress,
			TokenSymbol:    meta.symbol,
			TokenDecimals:  meta.decimals,
			AssetDecimals:  assetDecimals,
			Star:           s.Entry.Star,
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
		})
	}

	return h.repo.SavePositions(ctx, positions)
}

// ── Minimal metadata cache for symbol/decimals ──

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

func newMetadataCache(multicaller outbound.Multicaller, erc20ABI *abi.ABI, logger *slog.Logger) *metadataCache {
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

func (c *metadataCache) getOrDefault(addr common.Address) tokenMeta {
	if m, ok := c.get(addr); ok {
		return m
	}
	return tokenMeta{symbol: "UNKNOWN", decimals: 18}
}

func (c *metadataCache) fetchMissing(ctx context.Context, addrs []common.Address) error {
	// Deduplicate and filter already cached
	seen := make(map[common.Address]bool)
	var missing []common.Address

	c.mu.RLock()
	for _, addr := range addrs {
		if _, ok := c.cache[addr]; ok {
			continue
		}
		if !seen[addr] {
			seen[addr] = true
			missing = append(missing, addr)
		}
	}
	c.mu.RUnlock()

	if len(missing) == 0 {
		return nil
	}

	// Multicall: decimals + symbol per token
	calls := make([]outbound.Call, 0, len(missing)*2)
	for _, addr := range missing {
		decimalsData, _ := c.erc20ABI.Pack("decimals")
		symbolData, _ := c.erc20ABI.Pack("symbol")
		calls = append(calls,
			outbound.Call{Target: addr, AllowFailure: true, CallData: decimalsData},
			outbound.Call{Target: addr, AllowFailure: true, CallData: symbolData},
		)
	}

	results, err := c.multicaller.Execute(ctx, calls, nil)
	if err != nil {
		return fmt.Errorf("multicall metadata: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for i, addr := range missing {
		meta := tokenMeta{symbol: "UNKNOWN", decimals: 18}
		decIdx := i * 2
		symIdx := i*2 + 1

		if decIdx < len(results) && results[decIdx].Success && len(results[decIdx].ReturnData) > 0 {
			if unpacked, err := c.erc20ABI.Unpack("decimals", results[decIdx].ReturnData); err == nil && len(unpacked) > 0 {
				if d, ok := unpacked[0].(uint8); ok {
					meta.decimals = int(d)
				}
			}
		}

		if symIdx < len(results) && results[symIdx].Success && len(results[symIdx].ReturnData) > 0 {
			if unpacked, err := c.erc20ABI.Unpack("symbol", results[symIdx].ReturnData); err == nil && len(unpacked) > 0 {
				if s, ok := unpacked[0].(string); ok {
					meta.symbol = s
				}
			}
		}

		c.cache[addr] = meta
		c.logger.Debug("cached metadata", "address", addr.Hex(), "symbol", meta.symbol, "decimals", meta.decimals)
	}

	return nil
}
