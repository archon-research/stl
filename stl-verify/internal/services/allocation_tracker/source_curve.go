package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const curveABIJson = `[
	{
		"inputs": [{"name": "account", "type": "address"}],
		"name": "balanceOf",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [{"name": "i", "type": "uint256"}],
		"name": "coins",
		"outputs": [{"name": "", "type": "address"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [{"name": "_token_amount", "type": "uint256"}, {"name": "i", "type": "int128"}],
		"name": "calc_withdraw_one_coin",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`

// CurveSource fetches Curve LP positions via three multicall rounds:
//  1. balanceOf(wallet) → LP shares (stored as ScaledBalance)
//  2. coins(0..3) → discover which coin index matches AssetAddress
//  3. calc_withdraw_one_coin(shares, index) → underlying value (stored as Balance)
type CurveSource struct {
	multicaller outbound.Multicaller
	poolABI     abi.ABI
	logger      *slog.Logger
}

func NewCurveSource(
	multicaller outbound.Multicaller,
	logger *slog.Logger,
) (*CurveSource, error) {
	parsed, err := abi.JSON(strings.NewReader(curveABIJson))
	if err != nil {
		return nil, fmt.Errorf("parse curve ABI: %w", err)
	}
	return &CurveSource{
		multicaller: multicaller,
		poolABI:     parsed,
		logger:      logger.With("source", "curve"),
	}, nil
}

func (s *CurveSource) Name() string { return "curve" }

func (s *CurveSource) Supports(tokenType string, protocol string) bool {
	return tokenType == "curve"
}

func (s *CurveSource) FetchBalances(
	ctx context.Context,
	entries []*TokenEntry,
	blockNumber int64,
) (map[EntryKey]*PositionBalance, error) {
	if len(entries) == 0 {
		return make(map[EntryKey]*PositionBalance), nil
	}

	var block *big.Int
	if blockNumber > 0 {
		block = big.NewInt(blockNumber)
	}

	// Round 1: balanceOf → LP shares
	shares, valid1, err := s.fetchShares(ctx, entries, block)
	if err != nil {
		return nil, fmt.Errorf("fetch shares: %w", err)
	}

	results := make(map[EntryKey]*PositionBalance, len(entries))

	// Filter to entries with non-zero shares
	var withShares []*TokenEntry
	for _, e := range valid1 {
		sh := shares[e.Key()]
		if sh != nil && sh.Sign() > 0 {
			withShares = append(withShares, e)
		} else {
			results[e.Key()] = &PositionBalance{
				Balance:       big.NewInt(0),
				ScaledBalance: big.NewInt(0),
			}
		}
	}

	if len(withShares) == 0 {
		return results, nil
	}

	// Round 2: discover coin indices
	coinIndices, err := s.discoverCoinIndices(ctx, withShares, block)
	if err != nil {
		s.logger.Warn("coin index discovery failed, using shares as fallback",
			"error", err)
		for _, e := range withShares {
			sh := shares[e.Key()]
			results[e.Key()] = &PositionBalance{
				Balance:       new(big.Int).Set(sh),
				ScaledBalance: sh,
			}
		}
		return results, nil
	}

	// Round 3: calc_withdraw_one_coin
	var toConvert []*TokenEntry
	for _, e := range withShares {
		if _, ok := coinIndices[e.Key()]; ok {
			toConvert = append(toConvert, e)
		} else {
			assetHex := "<nil>"
			if e.AssetAddress != nil {
				assetHex = e.AssetAddress.Hex()
			}
			s.logger.Warn("coin index not found for asset, using shares as fallback",
				"pool", e.ContractAddress.Hex(),
				"asset", assetHex)
			sh := shares[e.Key()]
			results[e.Key()] = &PositionBalance{
				Balance:       new(big.Int).Set(sh),
				ScaledBalance: sh,
			}
		}
	}

	if len(toConvert) == 0 {
		return results, nil
	}

	calls := make([]outbound.Call, 0, len(toConvert))
	var valid3 []*TokenEntry
	for _, e := range toConvert {
		idx := coinIndices[e.Key()]
		data, err := s.poolABI.Pack(
			"calc_withdraw_one_coin",
			shares[e.Key()],
			big.NewInt(int64(idx)),
		)
		if err != nil {
			s.logger.Warn("pack calc_withdraw_one_coin failed",
				"pool", e.ContractAddress.Hex(),
				"error", err)
			sh := shares[e.Key()]
			results[e.Key()] = &PositionBalance{
				Balance:       new(big.Int).Set(sh),
				ScaledBalance: sh,
			}
			continue
		}
		calls = append(calls, outbound.Call{
			Target:       e.ContractAddress,
			AllowFailure: true,
			CallData:     data,
		})
		valid3 = append(valid3, e)
	}

	mc, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		for _, e := range valid3 {
			sh := shares[e.Key()]
			results[e.Key()] = &PositionBalance{
				Balance:       new(big.Int).Set(sh),
				ScaledBalance: sh,
			}
		}
		s.logger.Warn("calc_withdraw_one_coin multicall failed, using shares as fallback",
			"error", err)
		return results, nil
	}

	for i, e := range valid3 {
		if i >= len(mc) {
			break
		}
		sh := shares[e.Key()]
		bal := &PositionBalance{
			ScaledBalance: sh,
			Balance:       new(big.Int).Set(sh), // fallback
		}
		if mc[i].Success && len(mc[i].ReturnData) > 0 {
			unpacked, err := s.poolABI.Unpack(
				"calc_withdraw_one_coin", mc[i].ReturnData,
			)
			if err == nil && len(unpacked) > 0 {
				if v, ok := unpacked[0].(*big.Int); ok {
					bal.Balance = v
				}
			}
		}
		results[e.Key()] = bal

		s.logger.Debug("curve position",
			"pool", e.ContractAddress.Hex(),
			"shares", sh.String(),
			"underlying", bal.Balance.String(),
			"coinIndex", coinIndices[e.Key()])
	}

	return results, nil
}

func (s *CurveSource) fetchShares(
	ctx context.Context,
	entries []*TokenEntry,
	block *big.Int,
) (map[EntryKey]*big.Int, []*TokenEntry, error) {
	calls := make([]outbound.Call, 0, len(entries))
	var valid []*TokenEntry

	for _, e := range entries {
		data, err := s.poolABI.Pack("balanceOf", e.WalletAddress)
		if err != nil {
			s.logger.Warn("pack balanceOf failed",
				"pool", e.ContractAddress.Hex(),
				"error", err)
			calls = append(calls, outbound.Call{
				Target:       e.ContractAddress,
				AllowFailure: true,
				CallData:     []byte{},
			})
			valid = append(valid, e)
			continue
		}
		calls = append(calls, outbound.Call{
			Target:       e.ContractAddress,
			AllowFailure: true,
			CallData:     data,
		})
		valid = append(valid, e)
	}

	if len(calls) == 0 {
		return nil, nil, nil
	}

	mc, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, nil, fmt.Errorf("balanceOf multicall: %w", err)
	}

	shares := make(map[EntryKey]*big.Int, len(valid))
	for i, e := range valid {
		if i >= len(mc) {
			break
		}
		shares[e.Key()] = big.NewInt(0)
		if mc[i].Success && len(mc[i].ReturnData) > 0 {
			unpacked, err := s.poolABI.Unpack(
				"balanceOf", mc[i].ReturnData,
			)
			if err == nil && len(unpacked) > 0 {
				if v, ok := unpacked[0].(*big.Int); ok {
					shares[e.Key()] = v
				}
			}
		}
	}

	return shares, valid, nil
}

// discoverCoinIndices calls coins(0)..coins(maxCoins-1) on each pool
// and returns the index that matches the entry's AssetAddress.
func (s *CurveSource) discoverCoinIndices(
	ctx context.Context,
	entries []*TokenEntry,
	block *big.Int,
) (map[EntryKey]int, error) {
	const maxCoins = 4

	calls := make([]outbound.Call, 0, len(entries)*maxCoins)
	for _, e := range entries {
		for i := 0; i < maxCoins; i++ {
			data, err := s.poolABI.Pack("coins", big.NewInt(int64(i)))
			if err != nil {
				// Pad with empty call to keep alignment
				calls = append(calls, outbound.Call{
					Target:       e.ContractAddress,
					AllowFailure: true,
					CallData:     []byte{},
				})
				continue
			}
			calls = append(calls, outbound.Call{
				Target:       e.ContractAddress,
				AllowFailure: true,
				CallData:     data,
			})
		}
	}

	mc, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("coins multicall: %w", err)
	}

	result := make(map[EntryKey]int)
	for i, e := range entries {
		if e.AssetAddress == nil {
			s.logger.Warn("curve entry missing AssetAddress",
				"pool", e.ContractAddress.Hex())
			continue
		}
		target := *e.AssetAddress
		for j := 0; j < maxCoins; j++ {
			idx := i*maxCoins + j
			if idx >= len(mc) {
				break
			}
			if !mc[idx].Success || len(mc[idx].ReturnData) == 0 {
				continue
			}
			unpacked, err := s.poolABI.Unpack("coins", mc[idx].ReturnData)
			if err != nil || len(unpacked) == 0 {
				continue
			}
			coinAddr, ok := unpacked[0].(common.Address)
			if !ok {
				continue
			}
			if coinAddr == target {
				result[e.Key()] = j
				s.logger.Debug("found coin index",
					"pool", e.ContractAddress.Hex(),
					"asset", target.Hex(),
					"index", j)
				break
			}
		}
	}

	return result, nil
}
