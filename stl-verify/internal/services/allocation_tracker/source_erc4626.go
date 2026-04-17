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

// ERC4626Source fetches vault share balances via balanceOf(proxy).
// We store the token's own on-chain units in Balance so allocation tracking
// matches the held receipt token, while ScaledBalance preserves the raw share
// amount for consumers that want it explicitly.
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

	shares, valid1, err := s.fetchShares(ctx, entries, block)
	if err != nil {
		return nil, fmt.Errorf("fetch shares: %w", err)
	}

	results := make(map[EntryKey]*PositionBalance, len(entries))
	for _, e := range valid1 {
		sh := shares[e.Key()]
		if sh == nil || sh.Sign() == 0 {
			results[e.Key()] = &PositionBalance{
				Balance:       big.NewInt(0),
				ScaledBalance: big.NewInt(0),
			}
			continue
		}
		s.logger.Debug("erc4626 position",
			"contract", e.ContractAddress.Hex(),
			"shares", sh.String())
		results[e.Key()] = &PositionBalance{
			Balance:       new(big.Int).Set(sh),
			ScaledBalance: new(big.Int).Set(sh),
		}
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
