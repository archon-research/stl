package allocation_tracker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"unicode/utf8"

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
	// Their row metadata comes from univ3RowMeta: decimals from the hint
	// asset, symbol composed from the pool pair.
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
			// The pool pair symbols feed the composed pool-row symbol.
			if s.PoolToken0 != nil {
				addrs = append(addrs, *s.PoolToken0)
			}
			if s.PoolToken1 != nil {
				addrs = append(addrs, *s.PoolToken1)
			}
			continue
		}
		addrs = append(addrs, metadataAddress(s))
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

// metadataAddress is the address a position's row metadata (decimals/symbol) is
// read from. For ERC-7540 centrifuge entries the contract_address is a vault
// with no decimals/symbol, so ERC7540Source resolves the share token and sets
// ShareToken (equal to the entry address for a direct share); metadata comes
// from there. Every other entry reads metadata from its own contract_address.
func metadataAddress(s *PositionSnapshot) common.Address {
	if s.ShareToken != nil {
		return *s.ShareToken
	}
	return s.Entry.ContractAddress
}

func (h *PrimePositionHandler) buildPositions(
	ctx context.Context,
	snapshots []*PositionSnapshot,
	nonERC20Types map[string]bool,
) ([]*entity.AllocationPosition, error) {
	positions := make([]*entity.AllocationPosition, 0, len(snapshots))
	for _, s := range snapshots {
		var meta tokenMeta
		if nonERC20Types[s.Entry.TokenType] {
			m, err := h.univ3RowMeta(s)
			if err != nil {
				return nil, err
			}
			meta = m
		} else {
			metaAddr := metadataAddress(s)
			m, ok := h.metadata.get(metaAddr)
			if !ok {
				return nil, fmt.Errorf(
					"metadata missing for token %s",
					metaAddr.Hex(),
				)
			}
			meta = m
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

// univ3RowMeta names a uni_v3 position's token-registry row (keyed by the
// POOL address; a V3 pool contract is not an ERC20 and has no symbol or
// decimals of its own). Decimals come from the hint asset because the row's
// balance is denominated in it; the symbol is composed from the pool pair
// (e.g. UNIV3-LP-AUSD-USDC, matching skyeco's line naming) because reusing the
// hint asset's own symbol labeled the pool row as the asset itself (an impostor
// "USDC" row). Token order follows the pool's on-chain token0/token1, not
// alphabetical, so the composed name matches skyeco exactly.
func (h *PrimePositionHandler) univ3RowMeta(s *PositionSnapshot) (tokenMeta, error) {
	if s.Entry.AssetAddress == nil {
		return tokenMeta{}, fmt.Errorf(
			"uni_v3 entry %s has no asset address",
			s.Entry.ContractAddress.Hex(),
		)
	}
	asset, ok := h.metadata.get(*s.Entry.AssetAddress)
	if !ok {
		return tokenMeta{}, fmt.Errorf(
			"metadata missing for asset %s (pool %s)",
			s.Entry.AssetAddress.Hex(),
			s.Entry.ContractAddress.Hex(),
		)
	}
	if s.PoolToken0 == nil || s.PoolToken1 == nil {
		return tokenMeta{}, fmt.Errorf(
			"uni_v3 snapshot for pool %s is missing the pool token pair; cannot compose the pool row symbol",
			s.Entry.ContractAddress.Hex(),
		)
	}
	token0, err := h.poolPairSymbol(*s.PoolToken0, s.Entry.ContractAddress)
	if err != nil {
		return tokenMeta{}, err
	}
	token1, err := h.poolPairSymbol(*s.PoolToken1, s.Entry.ContractAddress)
	if err != nil {
		return tokenMeta{}, err
	}
	return tokenMeta{
		symbol:   fmt.Sprintf("UNIV3-LP-%s-%s", token0, token1),
		decimals: asset.decimals,
	}, nil
}

// poolPairSymbol resolves one pool token's symbol for composition. The cache
// never holds an unresolved symbol (fetchMissing fails the batch instead of
// caching a fallback), so a cache hit is always safe to compose with.
func (h *PrimePositionHandler) poolPairSymbol(token, pool common.Address) (string, error) {
	meta, ok := h.metadata.get(token)
	if !ok {
		return "", fmt.Errorf(
			"metadata missing for pool token %s (pool %s)",
			token.Hex(), pool.Hex(),
		)
	}
	return meta.symbol, nil
}

// underlyingValuation applies the per-token-type denomination policy
// (VEC-307). Only types whose read result is a value in a known asset's units
// get a valuation. NAV/RWA share tokens (buidl, securitize, superstate,
// centrifuge, proxy) and curve pool positions stay nil: their balanceOf is a
// share count, and denominating it in the entry's asset_address (a pricing
// hint, e.g. USTB->USDC) would be plausible-but-wrong data. The empty reason
// means "nil by design, not a failure".
func (h *PrimePositionHandler) underlyingValuation(s *PositionSnapshot) (*entity.UnderlyingValuation, FailureReason) {
	switch s.Entry.TokenType {
	case "erc4626", "uni_v3_pool", "uni_v3_lp":
		// Both carry a source-computed value denominated in asset_address:
		// erc4626 reads convertToAssets(shares); uni_v3 computes the full
		// position value (both sides at the pool's own spot price). The V3
		// pool is not an ERC20 and can never have its own oracle, so this
		// valuation is the only way the API can price the position.
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

func (h *PrimePositionHandler) valuationFor(asset common.Address, value *big.Int) (*entity.UnderlyingValuation, FailureReason) {
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

	decimalsData, err := c.erc20ABI.Pack("decimals")
	if err != nil {
		return fmt.Errorf("pack decimals: %w", err)
	}
	symbolData, err := c.erc20ABI.Pack("symbol")
	if err != nil {
		return fmt.Errorf("pack symbol: %w", err)
	}
	calls := make([]outbound.Call, 0, len(missing)*2)
	for _, addr := range missing {
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

	// Number-pinned intentionally: decimals/symbol are structurally static
	// (immutable per contract), not versioned per-block state, so the
	// reorg-correctness concern behind ExecuteAtHash (VEC-471) doesn't apply here.
	block := big.NewInt(blockNumber)
	results, err := c.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return fmt.Errorf("multicall metadata: %w", err)
	}
	if len(results) != len(calls) {
		return fmt.Errorf("metadata multicall returned %d results for %d calls", len(results), len(calls))
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// A failed or undecodable read errors BEFORE anything is cached for the
	// address: the cache is long-lived and only consulted for missing
	// addresses, so caching a fallback would make every SQS redelivery reuse
	// it until a process restart, silently persisting rows and pool-pair
	// symbols under an impostor-class name that the token upsert never
	// refreshes on conflict.
	for i, addr := range missing {
		meta, err := c.decodeTokenMeta(addr, results[i*2], results[i*2+1])
		if err != nil {
			return err
		}
		c.cache[addr] = meta
		c.logger.Debug("cached metadata",
			"address", addr.Hex(),
			"symbol", meta.symbol,
			"decimals", meta.decimals)
	}

	return nil
}

// decodeTokenMeta decodes one address's decimals/symbol result pair.
func (c *metadataCache) decodeTokenMeta(addr common.Address, decResult, symResult outbound.Result) (tokenMeta, error) {
	decimals, err := c.decodeDecimals(addr, decResult)
	if err != nil {
		return tokenMeta{}, err
	}
	symbol, err := c.decodeSymbol(addr, symResult)
	if err != nil {
		return tokenMeta{}, err
	}
	return tokenMeta{symbol: symbol, decimals: decimals}, nil
}

func (c *metadataCache) decodeDecimals(addr common.Address, result outbound.Result) (int, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return 0, fmt.Errorf("decimals sub-call failed for %s", addr.Hex())
	}
	unpacked, err := c.erc20ABI.Unpack("decimals", result.ReturnData)
	if err != nil {
		return 0, fmt.Errorf("unpack decimals for %s: %w", addr.Hex(), err)
	}
	d, ok := unpacked[0].(uint8)
	if !ok {
		return 0, fmt.Errorf("decimals for %s returned %T, want uint8", addr.Hex(), unpacked[0])
	}
	return int(d), nil
}

// decodeSymbol decodes a symbol() result. The ERC20 ABI declares string, but
// MKR-class tokens return bytes32, and uni_v3 pool pair tokens come from
// on-chain token0()/token1() rather than the curated registry, so
// hard-failing on that known variant would deterministically poison the queue
// (the read never changes on redelivery). The shape is gated structurally: an
// ABI string is at least 64 bytes (offset + length words), so an
// exactly-32-byte payload is unambiguously a bytes32 symbol.
func (c *metadataCache) decodeSymbol(addr common.Address, result outbound.Result) (string, error) {
	if !result.Success || len(result.ReturnData) == 0 {
		return "", fmt.Errorf("symbol sub-call failed for %s", addr.Hex())
	}
	unpacked, strErr := c.erc20ABI.Unpack("symbol", result.ReturnData)
	if strErr == nil {
		s, ok := unpacked[0].(string)
		if !ok {
			return "", fmt.Errorf("symbol for %s returned %T, want string", addr.Hex(), unpacked[0])
		}
		return s, nil
	}
	if s, ok := decodeBytes32Symbol(result.ReturnData); ok {
		return s, nil
	}
	return "", fmt.Errorf("symbol for %s is neither an ABI string nor a bytes32: %w", addr.Hex(), strErr)
}

// decodeBytes32Symbol trims trailing NUL padding and accepts only non-empty
// valid UTF-8 without interior NULs (Postgres text cannot hold NUL bytes).
func decodeBytes32Symbol(data []byte) (string, bool) {
	if len(data) != 32 {
		return "", false
	}
	trimmed := bytes.TrimRight(data, "\x00")
	if len(trimmed) == 0 || !utf8.Valid(trimmed) || bytes.IndexByte(trimmed, 0) >= 0 {
		return "", false
	}
	return string(trimmed), true
}
