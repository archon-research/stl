package curve_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	curveapi "github.com/archon-research/stl/stl-verify/internal/adapters/outbound/curve"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// DefaultPools returns the hardcoded list of Curve pools to track.
func DefaultPools() []PoolConfig {
	return []PoolConfig{
		{
			Address: common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10"),
			ChainID: 1,
			Name:    "sUSDSUSDT",
		},
		{
			Address: common.HexToAddress("0xa632d59b9b804a956bfaa9b48af3a1b74808fc1f"),
			ChainID: 1,
			Name:    "PYUSDUSDS",
		},
		{
			Address: common.HexToAddress("0xe79c1c7e24755574438a26d5e062ad2626c04662"),
			ChainID: 1,
			Name:    "AUSDUSDC",
		},
	}
}

type Service struct {
	ethClient   *ethclient.Client
	multicaller outbound.Multicaller
	poolABI     *abi.ABI
	erc20ABI    *abi.ABI
	curveAPI    *curveapi.Client
	repo        outbound.CurveRepository
	pools       []PoolConfig
	chainName   string
	logger      *slog.Logger
}

func NewService(
	ethClient *ethclient.Client,
	multicaller outbound.Multicaller,
	poolABI *abi.ABI,
	erc20ABI *abi.ABI,
	curveAPI *curveapi.Client,
	repo outbound.CurveRepository,
	pools []PoolConfig,
	chainName string,
	logger *slog.Logger,
) *Service {
	return &Service{
		ethClient:   ethClient,
		multicaller: multicaller,
		poolABI:     poolABI,
		erc20ABI:    erc20ABI,
		curveAPI:    curveAPI,
		repo:        repo,
		pools:       pools,
		chainName:   chainName,
		logger:      logger.With("component", "curve-tracker"),
	}
}

// Run executes a single snapshot cycle for all pools.
func (s *Service) Run(ctx context.Context) error {
	start := time.Now()

	// Get latest block number
	blockNumber, err := s.ethClient.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}
	block := int64(blockNumber)
	s.logger.Info("starting snapshot", "block", block, "pools", len(s.pools))

	// Fetch on-chain data for all pools via multicall
	snapshots, err := s.fetchPoolData(ctx, block)
	if err != nil {
		return fmt.Errorf("fetch pool data: %w", err)
	}

	// Fetch APY data from Curve API (non-fatal)
	poolAddrs := make([]common.Address, len(s.pools))
	for i, p := range s.pools {
		poolAddrs[i] = p.Address
	}
	apyData, err := s.curveAPI.FetchAPYs(ctx, s.chainName, poolAddrs)
	if err != nil {
		s.logger.Warn("curve API fetch failed, proceeding without APY", "error", err)
	}

	// Build entities
	entities := make([]*entity.CurvePoolSnapshot, 0, len(snapshots))
	now := time.Now()

	for _, snap := range snapshots {
		e, err := s.buildEntity(snap, apyData, now)
		if err != nil {
			s.logger.Error("failed to build entity", "pool", snap.PoolAddress.Hex(), "error", err)
			continue
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

// fetchPoolData reads on-chain state for all pools in a single multicall batch.
func (s *Service) fetchPoolData(ctx context.Context, blockNumber int64) ([]*PoolSnapshot, error) {
	block := big.NewInt(blockNumber)

	// Build calls: for each pool we need N_COINS, get_balances, totalSupply,
	// get_virtual_price, A, fee. We'll add price_oracle calls after knowing N_COINS.
	// First pass: fetch N_COINS for all pools.
	nCoinsCalls := make([]outbound.Call, len(s.pools))
	for i, p := range s.pools {
		data, err := s.poolABI.Pack("N_COINS")
		if err != nil {
			return nil, fmt.Errorf("pack N_COINS for %s: %w", p.Name, err)
		}
		nCoinsCalls[i] = outbound.Call{Target: p.Address, AllowFailure: false, CallData: data}
	}

	nCoinsResults, err := s.multicaller.Execute(ctx, nCoinsCalls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall N_COINS: %w", err)
	}

	nCoinsMap := make(map[common.Address]int, len(s.pools))
	for i, p := range s.pools {
		if !nCoinsResults[i].Success {
			return nil, fmt.Errorf("N_COINS failed for %s", p.Name)
		}
		unpacked, err := s.poolABI.Unpack("N_COINS", nCoinsResults[i].ReturnData)
		if err != nil {
			return nil, fmt.Errorf("unpack N_COINS for %s: %w", p.Name, err)
		}
		n := unpacked[0].(*big.Int).Int64()
		nCoinsMap[p.Address] = int(n)
	}

	// Second pass: batch all remaining calls.
	// Per pool: get_balances, totalSupply, get_virtual_price, A, fee, coins(0..n-1), price_oracle(0..n-2)
	type callMeta struct {
		pool  int
		field string
		index int // for coins/price_oracle
	}

	var calls []outbound.Call
	var metas []callMeta

	for i, p := range s.pools {
		nCoins := nCoinsMap[p.Address]

		// get_balances
		data, _ := s.poolABI.Pack("get_balances")
		calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
		metas = append(metas, callMeta{pool: i, field: "get_balances"})

		// totalSupply
		data, _ = s.poolABI.Pack("totalSupply")
		calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
		metas = append(metas, callMeta{pool: i, field: "totalSupply"})

		// get_virtual_price
		data, _ = s.poolABI.Pack("get_virtual_price")
		calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
		metas = append(metas, callMeta{pool: i, field: "get_virtual_price"})

		// A
		data, _ = s.poolABI.Pack("A")
		calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
		metas = append(metas, callMeta{pool: i, field: "A"})

		// fee
		data, _ = s.poolABI.Pack("fee")
		calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
		metas = append(metas, callMeta{pool: i, field: "fee"})

		// coins(i) for each coin
		for j := range nCoins {
			data, _ = s.poolABI.Pack("coins", big.NewInt(int64(j)))
			calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
			metas = append(metas, callMeta{pool: i, field: "coins", index: j})
		}

		// price_oracle(i) for i = 0..n-2 (coin i+1 relative to coin 0)
		for j := range nCoins - 1 {
			data, _ = s.poolABI.Pack("price_oracle", big.NewInt(int64(j)))
			calls = append(calls, outbound.Call{Target: p.Address, AllowFailure: true, CallData: data})
			metas = append(metas, callMeta{pool: i, field: "price_oracle", index: j})
		}
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall pool data: %w", err)
	}

	// Parse results into snapshots.
	snapshots := make([]*PoolSnapshot, len(s.pools))
	for i, p := range s.pools {
		snapshots[i] = &PoolSnapshot{
			PoolAddress: p.Address,
			ChainID:     p.ChainID,
			BlockNumber: blockNumber,
			NCoins:      nCoinsMap[p.Address],
		}
	}

	for i, meta := range metas {
		if i >= len(results) {
			break
		}
		r := results[i]
		snap := snapshots[meta.pool]

		if !r.Success || len(r.ReturnData) == 0 {
			s.logger.Warn("call failed",
				"pool", s.pools[meta.pool].Name,
				"field", meta.field,
				"index", meta.index)
			continue
		}

		switch meta.field {
		case "get_balances":
			unpacked, err := s.poolABI.Unpack("get_balances", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				if bals, ok := unpacked[0].([]*big.Int); ok {
					snap.CoinBalances = make([]CoinBalance, len(bals))
					for j, b := range bals {
						snap.CoinBalances[j].Balance = b.String()
					}
				}
			}

		case "totalSupply":
			unpacked, err := s.poolABI.Unpack("totalSupply", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				snap.TotalSupply = unpacked[0].(*big.Int)
			}

		case "get_virtual_price":
			unpacked, err := s.poolABI.Unpack("get_virtual_price", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				snap.VirtualPrice = unpacked[0].(*big.Int)
			}

		case "A":
			unpacked, err := s.poolABI.Unpack("A", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				snap.AmpFactor = unpacked[0].(*big.Int).Int64()
			}

		case "fee":
			unpacked, err := s.poolABI.Unpack("fee", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				snap.Fee = unpacked[0].(*big.Int)
			}

		case "coins":
			unpacked, err := s.poolABI.Unpack("coins", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				addr := unpacked[0].(common.Address)
				if meta.index < len(snap.CoinBalances) {
					snap.CoinBalances[meta.index].Address = addr
				}
			}

		case "price_oracle":
			unpacked, err := s.poolABI.Unpack("price_oracle", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				price := unpacked[0].(*big.Int)
				snap.OraclePrices = append(snap.OraclePrices, OraclePrice{
					Index: meta.index,
					Price: shared.FormatAmount(price, 18),
				})
			}
		}
	}

	// Fetch decimals and symbols for all coins via ERC20 multicall.
	if err := s.enrichCoinMetadata(ctx, snapshots, block); err != nil {
		s.logger.Warn("failed to enrich coin metadata", "error", err)
	}

	return snapshots, nil
}

// enrichCoinMetadata fetches decimals, symbol, and token_id for each coin via ERC20 multicall
// and token table lookup.
func (s *Service) enrichCoinMetadata(ctx context.Context, snapshots []*PoolSnapshot, block *big.Int) error {
	// Deduplicate coin addresses.
	seen := make(map[common.Address]bool)
	var coins []common.Address

	for _, snap := range snapshots {
		for _, cb := range snap.CoinBalances {
			if cb.Address == (common.Address{}) {
				continue
			}
			if !seen[cb.Address] {
				seen[cb.Address] = true
				coins = append(coins, cb.Address)
			}
		}
	}

	if len(coins) == 0 {
		return nil
	}

	// Build calls: decimals() and symbol() for each coin.
	var calls []outbound.Call
	for _, addr := range coins {
		decData, _ := s.erc20ABI.Pack("decimals")
		calls = append(calls, outbound.Call{Target: addr, AllowFailure: true, CallData: decData})

		symData, _ := s.erc20ABI.Pack("symbol")
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
	metaMap := make(map[common.Address]*metadata, len(coins))

	for i, addr := range coins {
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

	// Resolve token IDs from the token table.
	tokenIDs, err := s.resolveTokenIDs(ctx, coins, snapshots)
	if err != nil {
		s.logger.Warn("failed to resolve token IDs", "error", err)
	}

	// Apply metadata + token IDs to snapshots.
	for _, snap := range snapshots {
		for j := range snap.CoinBalances {
			addr := snap.CoinBalances[j].Address
			if m, ok := metaMap[addr]; ok {
				snap.CoinBalances[j].Decimals = m.decimals
				snap.CoinBalances[j].Symbol = m.symbol
			}
			if id, ok := tokenIDs[addr]; ok {
				snap.CoinBalances[j].TokenID = id
			}
		}
	}

	return nil
}

// resolveTokenIDs looks up token IDs from the token table for each coin address.
// Tokens should already exist from the allocation tracker. If not found, token_id stays 0.
func (s *Service) resolveTokenIDs(ctx context.Context, coins []common.Address, snapshots []*PoolSnapshot) (map[common.Address]int64, error) {
	result := make(map[common.Address]int64, len(coins))
	for _, addr := range coins {
		// Find chainID from the first snapshot that has this coin.
		var chainID int64
		for _, snap := range snapshots {
			for _, cb := range snap.CoinBalances {
				if cb.Address == addr {
					chainID = snap.ChainID
					break
				}
			}
			if chainID > 0 {
				break
			}
		}

		id, err := s.repo.LookupTokenID(ctx, chainID, addr)
		if err != nil {
			s.logger.Debug("token not found in DB",
				"address", addr.Hex(),
				"chainID", chainID)
			continue
		}
		result[addr] = id
	}

	return result, nil
}

// calculateTVL computes the approximate TVL in USD for a stablecoin pool.
// All coins are assumed ~$1 since these are stablecoin pools. This slightly
// underreports sUSDS (~8%) since it accrues yield, but is accurate for
// USDT, USDC, USDS, PYUSD, AUSD.
func (s *Service) calculateTVL(snap *PoolSnapshot) *big.Float {
	if len(snap.CoinBalances) == 0 {
		return nil
	}

	tvl := new(big.Float)

	for _, cb := range snap.CoinBalances {
		if cb.Decimals == 0 {
			continue
		}

		rawBal, ok := new(big.Int).SetString(cb.Balance, 10)
		if !ok || rawBal.Sign() == 0 {
			continue
		}

		// Normalize: rawBalance / 10^decimals ≈ USD value for stablecoins
		balFloat := new(big.Float).SetInt(rawBal)
		divisor := new(big.Float).SetInt(new(big.Int).Exp(
			big.NewInt(10), big.NewInt(int64(cb.Decimals)), nil,
		))
		balFloat.Quo(balFloat, divisor)

		tvl.Add(tvl, balFloat)
	}

	return tvl
}

func (s *Service) buildEntity(snap *PoolSnapshot, apyData map[common.Address]*curveapi.PoolAPY, now time.Time) (*entity.CurvePoolSnapshot, error) {
	coinJSON, err := json.Marshal(snap.CoinBalances)
	if err != nil {
		return nil, fmt.Errorf("marshal coin balances: %w", err)
	}

	var oraclePricesJSON json.RawMessage
	if len(snap.OraclePrices) > 0 {
		oraclePricesJSON, err = json.Marshal(snap.OraclePrices)
		if err != nil {
			return nil, fmt.Errorf("marshal oracle prices: %w", err)
		}
	}

	totalSupply := "0"
	if snap.TotalSupply != nil {
		totalSupply = shared.FormatAmount(snap.TotalSupply, 18)
	}

	virtualPrice := "0"
	if snap.VirtualPrice != nil {
		virtualPrice = shared.FormatAmount(snap.VirtualPrice, 18)
	}

	fee := "0"
	if snap.Fee != nil {
		fee = shared.FormatAmount(snap.Fee, 10) // fee is 10 decimals
	}

	e := &entity.CurvePoolSnapshot{
		PoolAddress:  snap.PoolAddress.Bytes(),
		ChainID:      snap.ChainID,
		BlockNumber:  snap.BlockNumber,
		CoinBalances: coinJSON,
		NCoins:       snap.NCoins,
		TotalSupply:  totalSupply,
		VirtualPrice: virtualPrice,
		AmpFactor:    int(snap.AmpFactor),
		Fee:          fee,
		OraclePrices: oraclePricesJSON,
		SnapshotTime: now,
	}

	// TVL
	tvl := s.calculateTVL(snap)
	if tvl != nil {
		tvlStr := tvl.Text('f', 6)
		e.TvlUSD = &tvlStr
	}

	// APY data (nullable)
	if apyData != nil {
		if apy, ok := apyData[snap.PoolAddress]; ok {
			feeStr := fmt.Sprintf("%f", apy.FeeAPY)
			e.FeeAPY = &feeStr
			if apy.CrvAPYMin > 0 || apy.CrvAPYMax > 0 {
				minStr := fmt.Sprintf("%f", apy.CrvAPYMin)
				maxStr := fmt.Sprintf("%f", apy.CrvAPYMax)
				e.CrvAPYMin = &minStr
				e.CrvAPYMax = &maxStr
			}
		}
	}

	return e, nil
}
