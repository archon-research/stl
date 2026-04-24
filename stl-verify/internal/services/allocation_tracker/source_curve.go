package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// CurveSource fetches Curve LP token balances via balanceOf(proxy).
// We store the held LP token balance in Balance so the tracked amount matches
// the on-chain token balance, while ScaledBalance preserves the raw LP shares.
type CurveSource struct {
	multicaller outbound.Multicaller
	poolABI     abi.ABI
	logger      *slog.Logger
}

func NewCurveSource(
	multicaller outbound.Multicaller,
	curveABI *abi.ABI,
	logger *slog.Logger,
) *CurveSource {
	return &CurveSource{
		multicaller: multicaller,
		poolABI:     *curveABI,
		logger:      logger.With("source", "curve"),
	}
}

func (s *CurveSource) Name() string { return "curve" }

func (s *CurveSource) Supports(tokenType string, protocol string) bool {
	return tokenType == "curve"
}

func (s *CurveSource) FetchBalances(
	ctx context.Context,
	entries []*TokenEntry,
	blockNumber int64,
) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	var block *big.Int
	if blockNumber > 0 {
		block = big.NewInt(blockNumber)
	}

	shares, valid1, err := s.fetchShares(ctx, entries, block)
	if err != nil {
		return nil, fmt.Errorf("fetch shares: %w", err)
	}

	for _, e := range valid1 {
		sh := shares[e.Key()]
		if sh == nil || sh.Sign() == 0 {
			result.Balances[e.Key()] = &PositionBalance{
				Balance:       big.NewInt(0),
				ScaledBalance: big.NewInt(0),
			}
			continue
		}
		s.logger.Debug("curve position",
			"pool", e.ContractAddress.Hex(),
			"shares", sh.String())
		result.Balances[e.Key()] = &PositionBalance{
			Balance:       new(big.Int).Set(sh),
			ScaledBalance: new(big.Int).Set(sh),
		}
	}

	return result, nil
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
