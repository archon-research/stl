package uniswap_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/uniswapv3"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// TrackedPools returns the hardcoded list of Uniswap V3 pools to track.
func TrackedPools() []PoolConfig {
	return []PoolConfig{
		{
			Address: common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d"),
			ChainID: 1,
			Name:    "AUSD/USDC 0.01%",
		},
	}
}

type Service struct {
	ethClient   *ethclient.Client
	multicaller outbound.Multicaller
	poolABI     *abi.ABI
	erc20ABI    *abi.ABI
	repo        outbound.UniswapRepository
	tokenRepo   outbound.TokenRepository
	pools       []PoolConfig
	twapWindow  uint32
	logger      *slog.Logger
}

// DefaultTWAPWindow is the default TWAP observation window in seconds (30 minutes).
const DefaultTWAPWindow uint32 = 1800

func NewService(
	ethClient *ethclient.Client,
	multicaller outbound.Multicaller,
	poolABI *abi.ABI,
	erc20ABI *abi.ABI,
	repo outbound.UniswapRepository,
	tokenRepo outbound.TokenRepository,
	pools []PoolConfig,
	twapWindow uint32,
	logger *slog.Logger,
) *Service {
	if twapWindow == 0 {
		twapWindow = DefaultTWAPWindow
	}
	return &Service{
		ethClient:   ethClient,
		multicaller: multicaller,
		poolABI:     poolABI,
		erc20ABI:    erc20ABI,
		repo:        repo,
		tokenRepo:   tokenRepo,
		pools:       pools,
		twapWindow:  twapWindow,
		logger:      logger.With("component", "uniswap-tracker"),
	}
}

// Run executes a single snapshot cycle for all pools.
func (s *Service) Run(ctx context.Context) error {
	start := time.Now()

	// Finalized block (Ethereum post-merge). Safe for mainnet only.
	// Other chains may need a different finality strategy.
	header, err := s.ethClient.HeaderByNumber(ctx, big.NewInt(int64(rpc.FinalizedBlockNumber)))
	if err != nil {
		return fmt.Errorf("get finalized header: %w", err)
	}
	block := header.Number.Int64()
	blockTime := time.Unix(int64(header.Time), 0)
	s.logger.Info("starting snapshot", "block", block, "blockTime", blockTime, "pools", len(s.pools))

	// Fetch on-chain data for all pools via multicall
	snapshots, err := s.fetchPoolData(ctx, block)
	if err != nil {
		return fmt.Errorf("fetch pool data: %w", err)
	}

	// Enrich with token metadata
	if err := s.enrichTokenMetadata(ctx, snapshots, big.NewInt(block)); err != nil {
		return fmt.Errorf("enrich token metadata: %w", err)
	}

	// Build entities
	entities := make([]*entity.UniswapPoolSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		e, err := s.buildEntity(snap, blockTime)
		if err != nil {
			return fmt.Errorf("build entity for %s: %w", snap.PoolAddress.Hex(), err)
		}
		entities = append(entities, e)
	}

	// Persist
	if err := s.repo.SaveSnapshots(ctx, entities); err != nil {
		return fmt.Errorf("save snapshots: %w", err)
	}

	s.logger.Info("snapshot complete",
		"block", block,
		"pools", len(entities),
		"duration", time.Since(start))
	return nil
}

// fetchPoolData reads on-chain state for all pools via multicall.
// Per pool: slot0, liquidity, fee, token0, token1, balanceOf(token0), balanceOf(token1), observe
func (s *Service) fetchPoolData(ctx context.Context, blockNumber int64) ([]*PoolSnapshot, error) {
	block := big.NewInt(blockNumber)

	type callMeta struct {
		pool  int
		field string
	}

	var calls []outbound.Call
	var metas []callMeta

	for i, p := range s.pools {
		packAndAppend := func(method string, args []any, field string) error {
			var data []byte
			var err error
			if len(args) > 0 {
				data, err = s.poolABI.Pack(method, args...)
			} else {
				data, err = s.poolABI.Pack(method)
			}
			if err != nil {
				return fmt.Errorf("pack %s for %s: %w", method, p.Name, err)
			}
			calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
			metas = append(metas, callMeta{pool: i, field: field})
			return nil
		}

		if err := packAndAppend("slot0", nil, "slot0"); err != nil {
			return nil, err
		}
		if err := packAndAppend("liquidity", nil, "liquidity"); err != nil {
			return nil, err
		}
		if err := packAndAppend("fee", nil, "fee"); err != nil {
			return nil, err
		}
		if err := packAndAppend("token0", nil, "token0"); err != nil {
			return nil, err
		}
		if err := packAndAppend("token1", nil, "token1"); err != nil {
			return nil, err
		}

		// observe for TWAP
		secondsAgos := []uint32{s.twapWindow, 0}
		observeData, err := s.poolABI.Pack("observe", secondsAgos)
		if err != nil {
			return nil, fmt.Errorf("pack observe for %s: %w", p.Name, err)
		}
		calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: observeData})
		metas = append(metas, callMeta{pool: i, field: "observe"})

		// Fee growth accumulators (for APY computation)
		if err := packAndAppend("feeGrowthGlobal0X128", nil, "feeGrowthGlobal0X128"); err != nil {
			return nil, err
		}
		if err := packAndAppend("feeGrowthGlobal1X128", nil, "feeGrowthGlobal1X128"); err != nil {
			return nil, err
		}
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall pool data: %w", err)
	}

	// Initialize snapshots.
	snapshots := make([]*PoolSnapshot, len(s.pools))
	for i, p := range s.pools {
		snapshots[i] = &PoolSnapshot{
			PoolAddress: p.Address,
			ChainID:     p.ChainID,
			BlockNumber: blockNumber,
		}
	}

	// Parse results.
	for i, meta := range metas {
		if i >= len(results) {
			break
		}
		r := results[i]
		snap := snapshots[meta.pool]

		if !r.Success || len(r.ReturnData) == 0 {
			if meta.field != "observe" {
				return nil, fmt.Errorf("%s failed for pool %s", meta.field, s.pools[meta.pool].Name)
			}
			s.logger.Warn("observe failed (low cardinality?)", "pool", s.pools[meta.pool].Name)
			continue
		}

		switch meta.field {
		case "slot0":
			unpacked, err := s.poolABI.Unpack("slot0", r.ReturnData)
			if err != nil {
				return nil, fmt.Errorf("unpack slot0 for %s: %w", s.pools[meta.pool].Name, err)
			}
			sqrtPriceX96 := unpacked[0].(*big.Int)
			tick := unpacked[1].(*big.Int)

			snap.SqrtPriceX96 = sqrtPriceX96.String()
			snap.CurrentTick = int(tick.Int64())
			snap.Price = uniswapv3.ComputePrice(sqrtPriceX96)

		case "liquidity":
			unpacked, err := s.poolABI.Unpack("liquidity", r.ReturnData)
			if err != nil {
				return nil, fmt.Errorf("unpack liquidity for %s: %w", s.pools[meta.pool].Name, err)
			}
			snap.ActiveLiquidity = unpacked[0].(*big.Int)

		case "fee":
			unpacked, err := s.poolABI.Unpack("fee", r.ReturnData)
			if err != nil {
				return nil, fmt.Errorf("unpack fee for %s: %w", s.pools[meta.pool].Name, err)
			}
			snap.Fee = int(unpacked[0].(*big.Int).Int64())

		case "token0":
			unpacked, err := s.poolABI.Unpack("token0", r.ReturnData)
			if err != nil {
				return nil, fmt.Errorf("unpack token0 for %s: %w", s.pools[meta.pool].Name, err)
			}
			snap.Token0.Address = unpacked[0].(common.Address)

		case "token1":
			unpacked, err := s.poolABI.Unpack("token1", r.ReturnData)
			if err != nil {
				return nil, fmt.Errorf("unpack token1 for %s: %w", s.pools[meta.pool].Name, err)
			}
			snap.Token1.Address = unpacked[0].(common.Address)

		case "observe":
			unpacked, err := s.poolABI.Unpack("observe", r.ReturnData)
			if err != nil {
				s.logger.Warn("unpack observe failed", "pool", s.pools[meta.pool].Name, "error", err)
				continue
			}
			tickCumulatives := unpacked[0].([]*big.Int)
			if len(tickCumulatives) == 2 {
				diff := new(big.Int).Sub(tickCumulatives[1], tickCumulatives[0])
				avgTick := int(diff.Int64() / int64(s.twapWindow))
				twapPrice := uniswapv3.ComputePriceFromTick(avgTick)
				snap.TwapTick = &avgTick
				snap.TwapPrice = &twapPrice
			}

		case "feeGrowthGlobal0X128":
			unpacked, err := s.poolABI.Unpack("feeGrowthGlobal0X128", r.ReturnData)
			if err != nil {
				return nil, fmt.Errorf("unpack feeGrowthGlobal0X128 for %s: %w", s.pools[meta.pool].Name, err)
			}
			snap.FeeGrowthGlobal0X128 = unpacked[0].(*big.Int).String()

		case "feeGrowthGlobal1X128":
			unpacked, err := s.poolABI.Unpack("feeGrowthGlobal1X128", r.ReturnData)
			if err != nil {
				return nil, fmt.Errorf("unpack feeGrowthGlobal1X128 for %s: %w", s.pools[meta.pool].Name, err)
			}
			snap.FeeGrowthGlobal1X128 = unpacked[0].(*big.Int).String()
		}
	}

	// Second multicall: balanceOf for token0 and token1 on each pool.
	if err := s.fetchTokenBalances(ctx, snapshots, block); err != nil {
		return nil, fmt.Errorf("fetch token balances: %w", err)
	}

	return snapshots, nil
}

// fetchTokenBalances reads balanceOf(poolAddress) for token0 and token1.
func (s *Service) fetchTokenBalances(ctx context.Context, snapshots []*PoolSnapshot, block *big.Int) error {
	var calls []outbound.Call
	type balMeta struct {
		snapIdx int
		token   int // 0 or 1
	}
	var metas []balMeta

	for i, snap := range snapshots {
		for tokenIdx, tokenAddr := range []common.Address{snap.Token0.Address, snap.Token1.Address} {
			if tokenAddr == (common.Address{}) {
				tokenLabel := "token0"
				if tokenIdx == 1 {
					tokenLabel = "token1"
				}
				return fmt.Errorf("%s is zero address for pool %s", tokenLabel, snap.PoolAddress.Hex())
			}
			data, err := s.erc20ABI.Pack("balanceOf", snap.PoolAddress)
			if err != nil {
				return fmt.Errorf("pack balanceOf: %w", err)
			}
			calls = append(calls, outbound.Call{Target: tokenAddr, AllowFailure: true, CallData: data})
			metas = append(metas, balMeta{snapIdx: i, token: tokenIdx})
		}
	}

	if len(calls) == 0 {
		return nil
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return fmt.Errorf("multicall balanceOf: %w", err)
	}

	for i, meta := range metas {
		if i >= len(results) || !results[i].Success || len(results[i].ReturnData) == 0 {
			tokenLabel := "token0"
			if meta.token == 1 {
				tokenLabel = "token1"
			}
			return fmt.Errorf("balanceOf %s failed for pool %s", tokenLabel, snapshots[meta.snapIdx].PoolAddress.Hex())
		}
		unpacked, err := s.erc20ABI.Unpack("balanceOf", results[i].ReturnData)
		if err != nil {
			return fmt.Errorf("unpack balanceOf: %w", err)
		}
		if len(unpacked) == 0 {
			return fmt.Errorf("balanceOf returned empty result for pool %s", snapshots[meta.snapIdx].PoolAddress.Hex())
		}
		balance := unpacked[0].(*big.Int)
		if meta.token == 0 {
			snapshots[meta.snapIdx].Token0.Balance = balance.String()
		} else {
			snapshots[meta.snapIdx].Token1.Balance = balance.String()
		}
	}

	return nil
}

// enrichTokenMetadata fetches decimals, symbol, and token_id for token0 and token1.
func (s *Service) enrichTokenMetadata(ctx context.Context, snapshots []*PoolSnapshot, block *big.Int) error {
	// Deduplicate token addresses.
	seen := make(map[common.Address]struct{})
	var tokens []common.Address

	for _, snap := range snapshots {
		for idx, addr := range []common.Address{snap.Token0.Address, snap.Token1.Address} {
			if addr == (common.Address{}) {
				tokenLabel := "token0"
				if idx == 1 {
					tokenLabel = "token1"
				}
				return fmt.Errorf("%s is zero address for pool %s", tokenLabel, snap.PoolAddress.Hex())
			}
			if _, exists := seen[addr]; !exists {
				seen[addr] = struct{}{}
				tokens = append(tokens, addr)
			}
		}
	}

	if len(tokens) == 0 {
		return nil
	}

	// Build calls: decimals() and symbol() for each token.
	var calls []outbound.Call
	for _, addr := range tokens {
		decData, err := s.erc20ABI.Pack("decimals")
		if err != nil {
			return fmt.Errorf("pack decimals: %w", err)
		}
		calls = append(calls, outbound.Call{Target: addr, AllowFailure: true, CallData: decData})

		symData, err := s.erc20ABI.Pack("symbol")
		if err != nil {
			return fmt.Errorf("pack symbol: %w", err)
		}
		calls = append(calls, outbound.Call{Target: addr, AllowFailure: true, CallData: symData})
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return fmt.Errorf("multicall metadata: %w", err)
	}

	type metadata struct {
		decimals int
		symbol   string
	}
	metaMap := make(map[common.Address]*metadata, len(tokens))

	for i, addr := range tokens {
		m := &metadata{}
		decIdx := i * 2
		symIdx := i*2 + 1

		if decIdx < len(results) && results[decIdx].Success && len(results[decIdx].ReturnData) > 0 {
			unpacked, err := s.erc20ABI.Unpack("decimals", results[decIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if v, ok := unpacked[0].(uint8); ok {
					m.decimals = int(v)
				}
			}
		}

		if symIdx < len(results) && results[symIdx].Success && len(results[symIdx].ReturnData) > 0 {
			unpacked, err := s.erc20ABI.Unpack("symbol", results[symIdx].ReturnData)
			if err == nil && len(unpacked) > 0 {
				if v, ok := unpacked[0].(string); ok {
					m.symbol = v
				}
			}
		}

		metaMap[addr] = m
	}

	// Resolve token IDs (informational — warn on failure).
	tokenIDs := make(map[common.Address]int64)
	for _, addr := range tokens {
		var chainID int64
		for _, snap := range snapshots {
			if snap.Token0.Address == addr || snap.Token1.Address == addr {
				chainID = snap.ChainID
				break
			}
		}
		id, err := s.tokenRepo.LookupTokenID(ctx, chainID, addr)
		if err != nil {
			if errors.Is(err, outbound.ErrTokenNotFound) {
				s.logger.Debug("token not found in DB", "address", addr.Hex(), "chainID", chainID)
				continue
			}
			s.logger.Warn("failed to lookup token ID", "address", addr.Hex(), "error", err)
			continue
		}
		tokenIDs[addr] = id
	}

	// Look up USD prices from onchain_token_price.
	tokenPrices := make(map[int64]*big.Float)
	var ids []int64
	for _, id := range tokenIDs {
		ids = append(ids, id)
	}
	if len(ids) > 0 {
		priceStrings, err := s.tokenRepo.LookupTokenPrices(ctx, ids)
		if err != nil {
			s.logger.Warn("failed to lookup token prices", "error", err)
		} else {
			for id, priceStr := range priceStrings {
				p, _, err := new(big.Float).Parse(priceStr, 10)
				if err != nil {
					s.logger.Warn("failed to parse price", "tokenID", id, "price", priceStr, "error", err)
					continue
				}
				tokenPrices[id] = p
			}
		}
	}

	// Apply metadata to snapshots.
	for _, snap := range snapshots {
		if m, ok := metaMap[snap.Token0.Address]; ok {
			snap.Token0.Decimals = m.decimals
			snap.Token0.Symbol = m.symbol
		}
		if id, ok := tokenIDs[snap.Token0.Address]; ok {
			snap.Token0.TokenID = id
			if price, ok := tokenPrices[id]; ok {
				snap.Token0.PriceUSD = price
			}
		}

		if m, ok := metaMap[snap.Token1.Address]; ok {
			snap.Token1.Decimals = m.decimals
			snap.Token1.Symbol = m.symbol
		}
		if id, ok := tokenIDs[snap.Token1.Address]; ok {
			snap.Token1.TokenID = id
			if price, ok := tokenPrices[id]; ok {
				snap.Token1.PriceUSD = price
			}
		}

		// Compute TVL now that we have decimals and prices.
		snap.TvlUSD = s.calculateTVL(snap)
	}

	return nil
}

// calculateTVL computes TVL using USD prices from onchain_token_price.
// Falls back to $1 per token if no price is available (safe for stablecoin pools).
func (s *Service) calculateTVL(snap *PoolSnapshot) *big.Float {
	tvl := new(big.Float)

	for _, tb := range []TokenBalance{snap.Token0, snap.Token1} {
		if tb.Decimals == 0 || tb.Balance == "" {
			continue
		}
		rawBal, ok := new(big.Int).SetString(tb.Balance, 10)
		if !ok || rawBal.Sign() == 0 {
			continue
		}
		balFloat := new(big.Float).SetInt(rawBal)
		divisor := new(big.Float).SetInt(new(big.Int).Exp(
			big.NewInt(10), big.NewInt(int64(tb.Decimals)), nil,
		))
		balFloat.Quo(balFloat, divisor)

		// Multiply by USD price (default $1 if not available).
		price := new(big.Float).SetFloat64(1.0)
		if tb.PriceUSD != nil {
			price = tb.PriceUSD
		}
		balFloat.Mul(balFloat, price)

		tvl.Add(tvl, balFloat)
	}

	if tvl.Sign() == 0 {
		return nil
	}
	return tvl
}

func (s *Service) buildEntity(snap *PoolSnapshot, blockTime time.Time) (*entity.UniswapPoolSnapshot, error) {
	token0JSON, err := json.Marshal(snap.Token0)
	if err != nil {
		return nil, fmt.Errorf("marshal token0: %w", err)
	}

	token1JSON, err := json.Marshal(snap.Token1)
	if err != nil {
		return nil, fmt.Errorf("marshal token1: %w", err)
	}

	activeLiquidity := "0"
	if snap.ActiveLiquidity != nil {
		activeLiquidity = snap.ActiveLiquidity.String()
	}

	e := &entity.UniswapPoolSnapshot{
		PoolAddress:          snap.PoolAddress.Bytes(),
		ChainID:              snap.ChainID,
		BlockNumber:          snap.BlockNumber,
		BlockVersion:         0,
		Token0:               token0JSON,
		Token1:               token1JSON,
		Fee:                  snap.Fee,
		SqrtPriceX96:         snap.SqrtPriceX96,
		CurrentTick:          snap.CurrentTick,
		Price:                snap.Price,
		ActiveLiquidity:      activeLiquidity,
		TwapTick:             snap.TwapTick,
		TwapPrice:            snap.TwapPrice,
		FeeGrowthGlobal0X128: snap.FeeGrowthGlobal0X128,
		FeeGrowthGlobal1X128: snap.FeeGrowthGlobal1X128,
		SnapshotTime:         blockTime,
	}

	if snap.TvlUSD != nil {
		tvlStr := snap.TvlUSD.Text('f', 6)
		e.TvlUSD = &tvlStr
	}

	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("entity validation: %w", err)
	}

	return e, nil
}
