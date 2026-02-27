package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const erc4626ABIJson = `[
	{
		"inputs": [{"name": "account", "type": "address"}],
		"name": "balanceOf",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [{"name": "shares", "type": "uint256"}],
		"name": "convertToAssets",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`

// ERC4626Source fetches vault positions via two multicall rounds:
//  1. balanceOf(proxy) → shares (stored as ScaledBalance)
//  2. convertToAssets(shares) → underlying (stored as Balance)
type ERC4626Source struct {
	multicaller outbound.Multicaller
	vaultABI    abi.ABI
	logger      *slog.Logger
}

func NewERC4626Source(multicaller outbound.Multicaller, logger *slog.Logger) (*ERC4626Source, error) {
	parsed, err := abi.JSON(strings.NewReader(erc4626ABIJson))
	if err != nil {
		return nil, fmt.Errorf("parse erc4626 ABI: %w", err)
	}
	return &ERC4626Source{
		multicaller: multicaller,
		vaultABI:    parsed,
		logger:      logger.With("source", "erc4626"),
	}, nil
}

func (s *ERC4626Source) Name() string { return "erc4626" }

func (s *ERC4626Source) Supports(tokenType string, protocol string) bool {
	return tokenType == "erc4626"
}

func (s *ERC4626Source) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (map[EntryKey]*PositionBalance, error) {
	if len(entries) == 0 {
		return make(map[EntryKey]*PositionBalance), nil
	}

	var block *big.Int
	if blockNumber > 0 {
		block = big.NewInt(blockNumber)
	}

	// Round 1: balanceOf → shares
	shares, valid1, err := s.fetchShares(ctx, entries, block)
	if err != nil {
		return nil, fmt.Errorf("fetch shares: %w", err)
	}

	// Round 2: convertToAssets for non-zero shares
	results := make(map[EntryKey]*PositionBalance, len(entries))

	// Prepare entries with non-zero shares for convert call
	var toConvert []*TokenEntry
	for _, e := range valid1 {
		sh := shares[e.Key()]
		if sh != nil && sh.Sign() > 0 {
			toConvert = append(toConvert, e)
		} else {
			results[e.Key()] = &PositionBalance{
				Balance:       big.NewInt(0),
				ScaledBalance: big.NewInt(0),
			}
		}
	}

	if len(toConvert) == 0 {
		return results, nil
	}

	// Build convertToAssets calls
	calls := make([]outbound.Call, 0, len(toConvert))
	var valid2 []*TokenEntry
	for _, e := range toConvert {
		data, err := s.vaultABI.Pack("convertToAssets", shares[e.Key()])
		if err != nil {
			s.logger.Warn("pack convertToAssets failed", "contract", e.ContractAddress.Hex(), "error", err)
			continue
		}
		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: data})
		valid2 = append(valid2, e)
	}

	mc, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		// Fallback: use shares as balance
		for _, e := range valid2 {
			sh := shares[e.Key()]
			results[e.Key()] = &PositionBalance{
				Balance:       new(big.Int).Set(sh),
				ScaledBalance: sh,
			}
		}
		return results, fmt.Errorf("convertToAssets multicall: %w", err)
	}

	for i, e := range valid2 {
		if i >= len(mc) {
			break
		}
		sh := shares[e.Key()]
		bal := &PositionBalance{
			ScaledBalance: sh,
			Balance:       new(big.Int).Set(sh), // fallback
		}
		if mc[i].Success && len(mc[i].ReturnData) > 0 {
			if unpacked, err := s.vaultABI.Unpack("convertToAssets", mc[i].ReturnData); err == nil && len(unpacked) > 0 {
				if v, ok := unpacked[0].(*big.Int); ok {
					bal.Balance = v
				}
			}
		}
		results[e.Key()] = bal

		s.logger.Debug("erc4626 position",
			"contract", e.ContractAddress.Hex(),
			"shares", sh.String(),
			"assets", bal.Balance.String())
	}

	return results, nil
}

func (s *ERC4626Source) fetchShares(ctx context.Context, entries []*TokenEntry, block *big.Int) (map[EntryKey]*big.Int, []*TokenEntry, error) {
	calls := make([]outbound.Call, 0, len(entries))
	var valid []*TokenEntry

	for _, e := range entries {
		data, err := s.vaultABI.Pack("balanceOf", e.WalletAddress)
		if err != nil {
			s.logger.Warn("pack balanceOf failed", "contract", e.ContractAddress.Hex(), "error", err)
			continue
		}
		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: data})
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
			if unpacked, err := s.vaultABI.Unpack("balanceOf", mc[i].ReturnData); err == nil && len(unpacked) > 0 {
				if v, ok := unpacked[0].(*big.Int); ok {
					shares[e.Key()] = v
				}
			}
		}
	}

	return shares, valid, nil
}
