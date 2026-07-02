package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type PrimePositionHandler struct {
	repo        outbound.AllocationRepository
	supplyRepo  outbound.TokenTotalSupplyRepository
	txm         outbound.TxManager
	metadata    *metadataCache
	primeLookup map[string]int64 // star name → prime.id
	logger      *slog.Logger
	telemetry   *Telemetry

	// Tracks tokens already warned about for an estimated created_at_block, so
	// the warning fires once per token rather than on every snapshot.
	estimatedMu     sync.Mutex
	estimatedTokens map[common.Address]struct{}
}

func NewPrimePositionHandler(
	repo outbound.AllocationRepository,
	supplyRepo outbound.TokenTotalSupplyRepository,
	txm outbound.TxManager,
	multicaller outbound.Multicaller,
	erc20ABI *abi.ABI,
	primeLookup map[string]int64,
	logger *slog.Logger,
	tel *Telemetry,
) *PrimePositionHandler {
	return &PrimePositionHandler{
		repo:        repo,
		supplyRepo:  supplyRepo,
		txm:         txm,
		metadata:    newMetadataCache(multicaller, erc20ABI, logger),
		primeLookup: primeLookup,
		logger:      logger.With("component", "postgres-handler"),
		telemetry:   tel,
	}
}

func (h *PrimePositionHandler) HandleBatch(
	ctx context.Context,
	batch *SnapshotBatch,
) error {
	if batch == nil {
		return nil
	}
	if len(batch.Snapshots) == 0 && len(batch.Supplies) == 0 {
		return nil
	}

	var blockNum int64
	if len(batch.Snapshots) > 0 {
		blockNum = batch.Snapshots[0].BlockNumber
	} else {
		blockNum = batch.Supplies[0].BlockNumber
	}

	// Token types where the contract itself isn't ERC20-compatible
	// (e.g. Uniswap V3 pool contracts don't have decimals/symbol).
	// For these, we use the AssetAddress metadata instead.
	nonERC20Types := map[string]bool{
		"uni_v3_pool": true,
		"uni_v3_lp":   true,
	}

	var addrs []common.Address
	for _, s := range batch.Snapshots {
		if nonERC20Types[s.Entry.TokenType] {
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
	for _, sup := range batch.Supplies {
		addrs = append(addrs, sup.TokenAddress)
	}
	if err := h.metadata.fetchMissing(ctx, addrs, blockNum); err != nil {
		return fmt.Errorf("metadata fetch: %w", err)
	}

	positions, err := h.buildPositions(ctx, batch.Snapshots, nonERC20Types)
	if err != nil {
		return err
	}

	supplies, err := h.buildSupplyEntities(batch.Supplies)
	if err != nil {
		return err
	}

	if len(positions) == 0 && len(supplies) == 0 {
		return nil
	}

	return h.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		if len(positions) > 0 {
			if err := h.repo.SavePositions(ctx, tx, positions); err != nil {
				return fmt.Errorf("save positions: %w", err)
			}
		}
		if len(supplies) > 0 {
			if err := h.supplyRepo.SaveSupplies(ctx, tx, supplies); err != nil {
				return fmt.Errorf("save supplies: %w", err)
			}
		}
		return nil
	})
}

func (h *PrimePositionHandler) buildPositions(
	ctx context.Context,
	snapshots []*PositionSnapshot,
	nonERC20Types map[string]bool,
) ([]*entity.AllocationPosition, error) {
	positions := make([]*entity.AllocationPosition, 0, len(snapshots))
	for _, s := range snapshots {
		var meta tokenMeta
		var ok bool

		if nonERC20Types[s.Entry.TokenType] {
			if s.Entry.AssetAddress == nil {
				return nil, fmt.Errorf(
					"uni_v3 entry %s has no asset address",
					s.Entry.ContractAddress.Hex(),
				)
			}
			meta, ok = h.metadata.get(*s.Entry.AssetAddress)
			if !ok {
				return nil, fmt.Errorf(
					"metadata missing for asset %s (pool %s)",
					s.Entry.AssetAddress.Hex(),
					s.Entry.ContractAddress.Hex(),
				)
			}
		} else {
			meta, ok = h.metadata.get(s.Entry.ContractAddress)
			if !ok {
				return nil, fmt.Errorf(
					"metadata missing for token %s",
					s.Entry.ContractAddress.Hex(),
				)
			}
		}

		primeID, ok := h.primeLookup[s.Entry.Star]
		if !ok {
			return nil, fmt.Errorf("unknown star %q: no matching prime_id", s.Entry.Star)
		}

		// created_at_block comes from the tracker-owned knownCreatedAtBlocks
		// registry (it is chain-observed, not carried by the axis-synome
		// contract). For positions absent from that registry, fall back to the
		// observation block as a non-zero floor. The token upsert applies
		// LEAST(existing, new), so a later write with the actual deploy block
		// self-corrects downward. Passing 0 would permanently pin
		// token.created_at_block to 0; see buildSupplyEntities.
		createdAtBlock := s.BlockNumber
		if s.Entry.CreatedAtBlock != nil && *s.Entry.CreatedAtBlock > 0 {
			createdAtBlock = *s.Entry.CreatedAtBlock
		} else {
			h.noteEstimatedCreatedAtBlock(s.Entry.ContractAddress, s.BlockNumber)
		}

		valuation, failReason := h.underlyingValuation(s)
		if failReason != "" {
			h.telemetry.RecordUnderlyingValueFailure(ctx, s.Entry.TokenType, s.Entry.ContractAddress, failReason)
			h.logger.Warn("underlying value not computable; persisting NULL",
				"token", s.Entry.ContractAddress.Hex(),
				"wallet", s.Entry.WalletAddress.Hex(),
				"block", s.BlockNumber,
				"tokenType", s.Entry.TokenType,
				"reason", failReason)
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
			Underlying:     valuation,
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
	return positions, nil
}

// underlyingValuation applies the per-token-type denomination policy
// (VEC-307). Only types whose read result is a value in a known asset's units
// get a valuation. NAV/RWA share tokens (buidl, securitize, superstate,
// centrifuge, proxy) and pool positions (curve, uni_v3) stay nil: their
// balanceOf is a share count, and denominating it in the entry's
// asset_address (a pricing hint, e.g. USTB->USDC) would be plausible-but-wrong
// data. The empty reason means "nil by design, not a failure".
func (h *PrimePositionHandler) underlyingValuation(s *PositionSnapshot) (*entity.UnderlyingValuation, string) {
	switch s.Entry.TokenType {
	case "erc4626":
		if s.Entry.AssetAddress == nil {
			return nil, reasonMissingAssetAddress
		}
		if s.UnderlyingValue == nil {
			return nil, reasonConvertFailed
		}
		return h.valuationFor(*s.Entry.AssetAddress, s.UnderlyingValue)
	case "atoken":
		// Aave balanceOf = scaledBalance x liquidityIndex: 1:1 in the
		// underlying's units and decimals by construction. The balance IS the
		// value; no extra call.
		if s.Entry.AssetAddress == nil {
			return nil, reasonMissingAssetAddress
		}
		return h.valuationFor(*s.Entry.AssetAddress, s.Balance)
	case "erc20":
		// A plain token is its own underlying. Deliberately duplicates
		// balance so "underlying_value IS NOT NULL" uniformly means "valued".
		// AssetAddress is ignored: for erc20 entries the axis-synome export
		// uses it as a pricing hint (AUSD->USDC), not a redemption denomination.
		return h.valuationFor(s.Entry.ContractAddress, s.Balance)
	default:
		return nil, ""
	}
}

func (h *PrimePositionHandler) valuationFor(asset common.Address, value *big.Int) (*entity.UnderlyingValuation, string) {
	if value == nil {
		return nil, reasonConvertFailed
	}
	meta, ok := h.metadata.get(asset)
	if !ok {
		return nil, reasonAssetMetadataMissing
	}
	return &entity.UnderlyingValuation{
		Value:         value,
		AssetAddress:  asset,
		AssetSymbol:   meta.symbol,
		AssetDecimals: meta.decimals,
	}, ""
}

// noteEstimatedCreatedAtBlock warns, once per token, that the persisted
// created_at_block is an estimate (the observation block) because the position
// is not recorded in the tracker-owned knownCreatedAtBlocks registry. This keeps
// the data gap observable without flooding the logs on every snapshot.
func (h *PrimePositionHandler) noteEstimatedCreatedAtBlock(token common.Address, block int64) {
	h.estimatedMu.Lock()
	if h.estimatedTokens == nil {
		h.estimatedTokens = make(map[common.Address]struct{})
	}
	_, seen := h.estimatedTokens[token]
	if !seen {
		h.estimatedTokens[token] = struct{}{}
	}
	h.estimatedMu.Unlock()

	if seen {
		return
	}
	h.logger.Warn("created_at_block estimated from observation block (no entry in knownCreatedAtBlocks)",
		"token", token.Hex(),
		"estimatedBlock", block)
}

func (h *PrimePositionHandler) buildSupplyEntities(
	snapshots []*TokenTotalSupplySnapshot,
) ([]*entity.TokenTotalSupply, error) {
	if len(snapshots) == 0 {
		return nil, nil
	}
	out := make([]*entity.TokenTotalSupply, 0, len(snapshots))
	for _, s := range snapshots {
		meta, ok := h.metadata.get(s.TokenAddress)
		if !ok {
			return nil, fmt.Errorf("metadata missing for supply token %s", s.TokenAddress.Hex())
		}
		out = append(out, &entity.TokenTotalSupply{
			ChainID:           s.ChainID,
			TokenAddress:      s.TokenAddress,
			TokenSymbol:       meta.symbol,
			TokenDecimals:     meta.decimals,
			TotalSupply:       s.TotalSupply,
			ScaledTotalSupply: s.ScaledTotalSupply,
			BlockNumber:       s.BlockNumber,
			BlockVersion:      s.BlockVersion,
			BlockTimestamp:    s.BlockTimestamp,
			Source:            s.Source,
			// Use the observation block as a non-zero floor for the token's
			// `created_at_block`. The token upsert applies LEAST(existing,
			// new), so a later allocation_position write with the actual
			// deploy block self-corrects to the smaller value. Passing 0 here
			// would permanently pin token.created_at_block to 0.
			CreatedAtBlock: s.BlockNumber,
		})
	}
	return out, nil
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
