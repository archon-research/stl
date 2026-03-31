package curve_tracker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

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
	repo        outbound.CurveRepository
	pools       []PoolConfig
	logger      *slog.Logger
}

func NewService(
	ethClient *ethclient.Client,
	multicaller outbound.Multicaller,
	poolABI *abi.ABI,
	erc20ABI *abi.ABI,
	repo outbound.CurveRepository,
	pools []PoolConfig,
	logger *slog.Logger,
) *Service {
	return &Service{
		ethClient:   ethClient,
		multicaller: multicaller,
		poolABI:     poolABI,
		erc20ABI:    erc20ABI,
		repo:        repo,
		pools:       pools,
		logger:      logger.With("component", "curve-tracker"),
	}
}

// Run executes a single snapshot cycle for all pools.
// Fee APY is not computed here — it is derived at query time from
// virtual_price deltas between any two snapshots.
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

	// Build entities
	entities := make([]*entity.CurvePoolSnapshot, 0, len(snapshots))
	for _, snap := range snapshots {
		e, err := s.buildEntity(snap, blockTime)
		if err != nil {
			return fmt.Errorf("build entity for %s: %w", snap.PoolAddress.Hex(), err)
		}

		// Compute fee APY from virtual_price delta vs previous snapshot.
		feeAPY, err := s.computeFeeAPY(ctx, e)
		if err != nil {
			s.logger.Warn("failed to compute fee APY", "pool", snap.PoolAddress.Hex(), "error", err)
		} else if feeAPY != nil {
			e.FeeAPY = feeAPY
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
	type callMeta struct {
		pool     int
		field    string
		index    int  // for coins/price_oracle
		required bool // if true, missing data drops the pool
	}

	var calls []outbound.Call
	var metas []callMeta

	for i, p := range s.pools {
		nCoins := nCoinsMap[p.Address]

		packAndAppend := func(method string, args []any, field string, index int, required bool) error {
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
			metas = append(metas, callMeta{pool: i, field: field, index: index, required: required})
			return nil
		}

		// Required fields — pool is dropped if any of these fail
		if err := packAndAppend("get_balances", nil, "get_balances", 0, true); err != nil {
			return nil, err
		}
		if err := packAndAppend("totalSupply", nil, "totalSupply", 0, true); err != nil {
			return nil, err
		}
		if err := packAndAppend("get_virtual_price", nil, "get_virtual_price", 0, true); err != nil {
			return nil, err
		}
		if err := packAndAppend("A", nil, "A", 0, true); err != nil {
			return nil, err
		}
		if err := packAndAppend("fee", nil, "fee", 0, true); err != nil {
			return nil, err
		}

		for j := range nCoins {
			if err := packAndAppend("coins", []any{big.NewInt(int64(j))}, "coins", j, true); err != nil {
				return nil, err
			}
		}

		// Optional fields — pool is kept with partial data
		for j := range nCoins - 1 {
			if err := packAndAppend("price_oracle", []any{big.NewInt(int64(j))}, "price_oracle", j, false); err != nil {
				return nil, err
			}
			if err := packAndAppend("last_price", []any{big.NewInt(int64(j))}, "last_price", j, false); err != nil {
				return nil, err
			}
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

	// Track which pools have failed required fields.
	failedPools := make(map[int]struct{})

	for i, meta := range metas {
		if i >= len(results) {
			break
		}
		r := results[i]
		snap := snapshots[meta.pool]

		if _, failed := failedPools[meta.pool]; failed {
			continue
		}

		if !r.Success || len(r.ReturnData) == 0 {
			if meta.required {
				s.logger.Error("required call failed, dropping pool",
					"pool", s.pools[meta.pool].Name,
					"field", meta.field,
					"index", meta.index)
				failedPools[meta.pool] = struct{}{}
			} else {
				s.logger.Warn("optional call failed",
					"pool", s.pools[meta.pool].Name,
					"field", meta.field,
					"index", meta.index)
			}
			continue
		}

		switch meta.field {
		case "get_balances":
			unpacked, err := s.poolABI.Unpack("get_balances", r.ReturnData)
			if err != nil || len(unpacked) == 0 {
				s.logger.Error("unpack get_balances failed", "pool", s.pools[meta.pool].Name, "error", err)
				failedPools[meta.pool] = struct{}{}
				continue
			}
			if bals, ok := unpacked[0].([]*big.Int); ok {
				snap.CoinBalances = make([]CoinBalance, len(bals))
				for j, b := range bals {
					snap.CoinBalances[j].Balance = b.String()
				}
			}

		case "totalSupply":
			unpacked, err := s.poolABI.Unpack("totalSupply", r.ReturnData)
			if err != nil || len(unpacked) == 0 {
				s.logger.Error("unpack totalSupply failed", "pool", s.pools[meta.pool].Name, "error", err)
				failedPools[meta.pool] = struct{}{}
				continue
			}
			snap.TotalSupply = unpacked[0].(*big.Int)

		case "get_virtual_price":
			unpacked, err := s.poolABI.Unpack("get_virtual_price", r.ReturnData)
			if err != nil || len(unpacked) == 0 {
				s.logger.Error("unpack get_virtual_price failed", "pool", s.pools[meta.pool].Name, "error", err)
				failedPools[meta.pool] = struct{}{}
				continue
			}
			snap.VirtualPrice = unpacked[0].(*big.Int)

		case "A":
			unpacked, err := s.poolABI.Unpack("A", r.ReturnData)
			if err != nil || len(unpacked) == 0 {
				s.logger.Error("unpack A failed", "pool", s.pools[meta.pool].Name, "error", err)
				failedPools[meta.pool] = struct{}{}
				continue
			}
			snap.AmpFactor = unpacked[0].(*big.Int).Int64()

		case "fee":
			unpacked, err := s.poolABI.Unpack("fee", r.ReturnData)
			if err != nil || len(unpacked) == 0 {
				s.logger.Error("unpack fee failed", "pool", s.pools[meta.pool].Name, "error", err)
				failedPools[meta.pool] = struct{}{}
				continue
			}
			snap.Fee = unpacked[0].(*big.Int)

		case "coins":
			unpacked, err := s.poolABI.Unpack("coins", r.ReturnData)
			if err != nil || len(unpacked) == 0 {
				s.logger.Error("unpack coins failed", "pool", s.pools[meta.pool].Name, "error", err)
				failedPools[meta.pool] = struct{}{}
				continue
			}
			addr := unpacked[0].(common.Address)
			if meta.index < len(snap.CoinBalances) {
				snap.CoinBalances[meta.index].Address = addr
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

		case "last_price":
			unpacked, err := s.poolABI.Unpack("last_price", r.ReturnData)
			if err == nil && len(unpacked) > 0 {
				price := unpacked[0].(*big.Int)
				snap.LastPrices = append(snap.LastPrices, OraclePrice{
					Index: meta.index,
					Price: shared.FormatAmount(price, 18),
				})
			}
		}
	}

	// Filter out pools with failed required data.
	var validSnapshots []*PoolSnapshot
	for i, snap := range snapshots {
		if _, failed := failedPools[i]; failed {
			continue
		}
		if snap.TotalSupply == nil || snap.VirtualPrice == nil || snap.Fee == nil || len(snap.CoinBalances) == 0 {
			s.logger.Error("dropping pool with incomplete required data",
				"pool", snap.PoolAddress.Hex())
			continue
		}
		validSnapshots = append(validSnapshots, snap)
	}

	// Fetch decimals and symbols for all coins via ERC20 multicall (fatal on failure).
	if err := s.enrichCoinMetadata(ctx, validSnapshots, block); err != nil {
		return nil, fmt.Errorf("enrich coin metadata: %w", err)
	}

	// Fetch exchange rates via get_dy (optional — uses decimals from enrichCoinMetadata).
	if err := s.fetchExchangeRates(ctx, validSnapshots, block); err != nil {
		s.logger.Warn("failed to fetch exchange rates", "error", err)
	}

	return validSnapshots, nil
}

// enrichCoinMetadata fetches decimals, symbol, and token_id for each coin via ERC20 multicall
// and token table lookup.
func (s *Service) enrichCoinMetadata(ctx context.Context, snapshots []*PoolSnapshot, block *big.Int) error {
	seen := make(map[common.Address]struct{})
	var coins []common.Address

	for _, snap := range snapshots {
		for _, cb := range snap.CoinBalances {
			if cb.Address == (common.Address{}) {
				continue
			}
			if _, exists := seen[cb.Address]; !exists {
				seen[cb.Address] = struct{}{}
				coins = append(coins, cb.Address)
			}
		}
	}

	if len(coins) == 0 {
		return nil
	}

	var calls []outbound.Call
	for _, addr := range coins {
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

	// Resolve token IDs (informational — warn on failure, don't fail cycle).
	tokenIDs, err := s.resolveTokenIDs(ctx, coins, snapshots)
	if err != nil {
		s.logger.Warn("failed to resolve token IDs", "error", err)
	}

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
func (s *Service) resolveTokenIDs(ctx context.Context, coins []common.Address, snapshots []*PoolSnapshot) (map[common.Address]int64, error) {
	result := make(map[common.Address]int64, len(coins))
	for _, addr := range coins {
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
			if errors.Is(err, outbound.ErrTokenNotFound) {
				s.logger.Debug("token not found in DB",
					"address", addr.Hex(),
					"chainID", chainID)
				continue
			}
			return nil, fmt.Errorf("lookup token ID for %s: %w", addr.Hex(), err)
		}
		result[addr] = id
	}

	return result, nil
}

// fetchExchangeRates calls get_dy(i, j, 1 unit) for each coin pair in each pool.
func (s *Service) fetchExchangeRates(ctx context.Context, snapshots []*PoolSnapshot, block *big.Int) error {
	type rateMeta struct {
		snapIdx int
		from    int
		to      int
		dx      *big.Int
	}

	var calls []outbound.Call
	var metas []rateMeta

	for si, snap := range snapshots {
		for i := range snap.NCoins {
			for j := range snap.NCoins {
				if i == j {
					continue
				}
				decimals := snap.CoinBalances[i].Decimals
				if decimals == 0 {
					continue
				}
				dx := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)

				data, err := s.poolABI.Pack("get_dy", big.NewInt(int64(i)), big.NewInt(int64(j)), dx)
				if err != nil {
					return fmt.Errorf("pack get_dy for %s (%d→%d): %w", snap.PoolAddress.Hex(), i, j, err)
				}
				calls = append(calls, outbound.Call{Target: snap.PoolAddress, AllowFailure: true, CallData: data})
				metas = append(metas, rateMeta{snapIdx: si, from: i, to: j, dx: dx})
			}
		}
	}

	if len(calls) == 0 {
		return nil
	}

	results, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return fmt.Errorf("multicall get_dy: %w", err)
	}

	for i, meta := range metas {
		if i >= len(results) {
			break
		}
		r := results[i]
		if !r.Success || len(r.ReturnData) == 0 {
			continue
		}

		unpacked, err := s.poolABI.Unpack("get_dy", r.ReturnData)
		if err != nil || len(unpacked) == 0 {
			continue
		}

		dy := unpacked[0].(*big.Int)
		snapshots[meta.snapIdx].ExchangeRates = append(snapshots[meta.snapIdx].ExchangeRates, ExchangeRate{
			From: meta.from,
			To:   meta.to,
			Dx:   meta.dx.String(),
			Dy:   dy.String(),
		})
	}

	return nil
}

// calculateTVL computes the approximate TVL in USD for a stablecoin pool.
// TODO: Use prices table for accurate sUSDS value.
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

		balFloat := new(big.Float).SetInt(rawBal)
		divisor := new(big.Float).SetInt(new(big.Int).Exp(
			big.NewInt(10), big.NewInt(int64(cb.Decimals)), nil,
		))
		balFloat.Quo(balFloat, divisor)

		tvl.Add(tvl, balFloat)
	}

	return tvl
}

// computeFeeAPY derives annualized fee APY from virtual_price delta.
// fee_apy = (vp_new - vp_old) / vp_old × (365 days / elapsed days) × 100
// Returns nil if no previous snapshot exists (first run).
func (s *Service) computeFeeAPY(ctx context.Context, e *entity.CurvePoolSnapshot) (*string, error) {
	prevVP, prevTime, err := s.repo.GetPreviousSnapshot(ctx, e.PoolAddress, e.ChainID)
	if err != nil {
		return nil, fmt.Errorf("get previous snapshot: %w", err)
	}
	if prevVP == "" || prevTime.IsZero() {
		return nil, nil // first snapshot for this pool
	}

	vpOld, _, err1 := new(big.Float).Parse(prevVP, 10)
	vpNew, _, err2 := new(big.Float).Parse(e.VirtualPrice, 10)
	if err1 != nil || err2 != nil || vpOld.Sign() == 0 {
		return nil, nil
	}

	elapsed := e.SnapshotTime.Sub(prevTime).Seconds()
	if elapsed <= 0 {
		return nil, nil
	}

	// (vpNew - vpOld) / vpOld
	delta := new(big.Float).Sub(vpNew, vpOld)
	ratio := new(big.Float).Quo(delta, vpOld)

	// Annualize: × (365 * 86400 / elapsed) × 100
	secsPerYear := new(big.Float).SetFloat64(365.0 * 86400.0)
	elapsedF := new(big.Float).SetFloat64(elapsed)
	annualized := new(big.Float).Quo(secsPerYear, elapsedF)
	apy := new(big.Float).Mul(ratio, annualized)
	apy.Mul(apy, new(big.Float).SetFloat64(100.0))

	result := apy.Text('f', 6)
	return &result, nil
}

func (s *Service) buildEntity(snap *PoolSnapshot, blockTime time.Time) (*entity.CurvePoolSnapshot, error) {
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

	var lastPricesJSON json.RawMessage
	if len(snap.LastPrices) > 0 {
		lastPricesJSON, err = json.Marshal(snap.LastPrices)
		if err != nil {
			return nil, fmt.Errorf("marshal last prices: %w", err)
		}
	}

	var exchangeRatesJSON json.RawMessage
	if len(snap.ExchangeRates) > 0 {
		exchangeRatesJSON, err = json.Marshal(snap.ExchangeRates)
		if err != nil {
			return nil, fmt.Errorf("marshal exchange rates: %w", err)
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
		fee = shared.FormatAmount(snap.Fee, 10)
	}

	e := &entity.CurvePoolSnapshot{
		PoolAddress:   snap.PoolAddress.Bytes(),
		ChainID:       snap.ChainID,
		BlockNumber:   snap.BlockNumber,
		CoinBalances:  coinJSON,
		NCoins:        snap.NCoins,
		TotalSupply:   totalSupply,
		VirtualPrice:  virtualPrice,
		AmpFactor:     int(snap.AmpFactor),
		Fee:           fee,
		OraclePrices:  oraclePricesJSON,
		LastPrices:    lastPricesJSON,
		ExchangeRates: exchangeRatesJSON,
		SnapshotTime:  blockTime,
	}

	tvl := s.calculateTVL(snap)
	if tvl != nil {
		tvlStr := tvl.Text('f', 6)
		e.TvlUSD = &tvlStr
	}

	if err := e.Validate(); err != nil {
		return nil, fmt.Errorf("entity validation: %w", err)
	}

	return e, nil
}
