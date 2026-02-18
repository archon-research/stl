package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// BalanceOfSource calls balanceOf(proxy) on the contract.
// Handles any token where balanceOf gives the meaningful position value.
type BalanceOfSource struct {
	multicaller    outbound.Multicaller
	erc20ABI       *abi.ABI
	logger         *slog.Logger
	supportedTypes map[string]bool
}

func NewBalanceOfSource(multicaller outbound.Multicaller, erc20ABI *abi.ABI, logger *slog.Logger) *BalanceOfSource {
	return &BalanceOfSource{
		multicaller: multicaller,
		erc20ABI:    erc20ABI,
		logger:      logger.With("source", "balanceof"),
		supportedTypes: map[string]bool{
			"erc20":      true,
			"buidl":      true,
			"securitize": true,
			"superstate": true,
			"curve":      true,
			"proxy":      true,
		},
	}
}

func (s *BalanceOfSource) Name() string { return "balanceof" }

func (s *BalanceOfSource) Supports(tokenType string, protocol string) bool {
	return s.supportedTypes[tokenType]
}

func (s *BalanceOfSource) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (map[EntryKey]*PositionBalance, error) {
	if len(entries) == 0 {
		return make(map[EntryKey]*PositionBalance), nil
	}

	calls := make([]outbound.Call, 0, len(entries))
	valid := make([]*TokenEntry, 0, len(entries))

	for _, e := range entries {
		data, err := s.erc20ABI.Pack("balanceOf", e.WalletAddress)
		if err != nil {
			s.logger.Warn("pack balanceOf failed", "contract", e.ContractAddress.Hex(), "error", err)
			continue
		}
		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: data})
		valid = append(valid, e)
	}

	if len(calls) == 0 {
		return make(map[EntryKey]*PositionBalance), nil
	}

	var block *big.Int
	if blockNumber > 0 {
		block = big.NewInt(blockNumber)
	}

	mc, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall: %w", err)
	}

	results := make(map[EntryKey]*PositionBalance, len(valid))
	for i, e := range valid {
		if i >= len(mc) {
			break
		}
		bal := &PositionBalance{Balance: big.NewInt(0)}
		if mc[i].Success && len(mc[i].ReturnData) > 0 {
			if unpacked, err := s.erc20ABI.Unpack("balanceOf", mc[i].ReturnData); err == nil && len(unpacked) > 0 {
				if v, ok := unpacked[0].(*big.Int); ok {
					bal.Balance = v
				}
			}
		}
		results[e.Key()] = bal
	}

	return results, nil
}
