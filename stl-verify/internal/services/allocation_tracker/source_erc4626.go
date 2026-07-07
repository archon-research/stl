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

const erc4626ABIJson = `[
	{
		"inputs": [{"name": "account", "type": "address"}],
		"name": "balanceOf",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
	,{
		"inputs": [{"name": "shares", "type": "uint256"}],
		"name": "convertToAssets",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`

// ERC4626Source fetches vault share balances in two multicall rounds, both
// pinned to the same block hash. Round 1: balanceOf(wallet) per entry; round 2:
// convertToAssets(shares) per non-zero entry. Balance and ScaledBalance hold
// raw shares; UnderlyingValue holds the asset equivalent or NULL when
// convertToAssets reverts or is undecodable (NULL = unknown, not 0).
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

func (s *ERC4626Source) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64, blockHash common.Hash) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	shares, valid1, err := s.fetchShares(ctx, entries, blockHash)
	if err != nil {
		return nil, fmt.Errorf("fetch shares: %w", err)
	}

	var toConvert []*TokenEntry
	for _, e := range valid1 {
		sh := shares[e.Key()]
		if sh == nil {
			return nil, fmt.Errorf("missing erc4626 share result for %s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex())
		}
		if sh.Sign() == 0 {
			result.Balances[e.Key()] = &PositionBalance{
				Balance:         big.NewInt(0),
				ScaledBalance:   big.NewInt(0),
				UnderlyingValue: big.NewInt(0),
			}
			continue
		}
		s.logger.Debug("erc4626 position",
			"contract", e.ContractAddress.Hex(),
			"shares", sh.String())
		result.Balances[e.Key()] = &PositionBalance{
			Balance:       new(big.Int).Set(sh),
			ScaledBalance: new(big.Int).Set(sh),
		}
		toConvert = append(toConvert, e)
	}

	if err := s.fetchUnderlyingValues(ctx, toConvert, shares, blockHash, result); err != nil {
		return nil, err
	}

	return result, nil
}

// fetchUnderlyingValues is the second multicall round: convertToAssets(shares)
// per non-zero position, pinned to the same block hash as the balanceOf round.
// Multicall3 cannot feed one call's output into another's input, so the two
// reads cannot share a batch; pinning both rounds to the same block hash keeps
// the snapshot self-consistent and reorg-correct (VEC-471). Raw (undecimalised)
// shares go in -- VEC-307 §8.4.
//
// A reverting/undecodable convertToAssets (observed on grove-bbqUSDC-V2)
// leaves UnderlyingValue nil: NULL is "unknown", while 0 or the share count
// would be plausible-but-wrong data poisoning downstream USD exposure. A
// transport-level multicall failure propagates instead, so SQS redelivers the
// block (VEC-188 invariant).
func (s *ERC4626Source) fetchUnderlyingValues(ctx context.Context, entries []*TokenEntry, shares map[EntryKey]*big.Int, blockHash common.Hash, result *FetchResult) error {
	if len(entries) == 0 {
		return nil
	}

	calls := make([]outbound.Call, 0, len(entries))
	var valid []*TokenEntry
	for _, e := range entries {
		sh := shares[e.Key()]
		if sh == nil {
			// Invariant: every entry passed here must have a shares result.
			return fmt.Errorf("missing shares for %s/%s in convertToAssets round", e.ContractAddress.Hex(), e.WalletAddress.Hex())
		}
		data, err := s.vaultABI.Pack("convertToAssets", sh)
		if err != nil {
			s.logger.Warn("pack convertToAssets failed", "contract", e.ContractAddress.Hex(), "error", err)
			continue
		}
		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: data})
		valid = append(valid, e)
	}
	if len(calls) == 0 {
		return nil
	}

	mc, err := s.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return fmt.Errorf("convertToAssets multicall: %w", err)
	}
	if len(mc) != len(valid) {
		return fmt.Errorf("convertToAssets multicall returned %d results for %d calls", len(mc), len(valid))
	}

	for i, e := range valid {
		if !mc[i].Success || len(mc[i].ReturnData) == 0 {
			s.logger.Warn("convertToAssets failed; underlying value will be NULL",
				"contract", e.ContractAddress.Hex(),
				"wallet", e.WalletAddress.Hex(),
				"blockHash", blockHash)
			continue
		}
		v := unpackUint256(&s.vaultABI, "convertToAssets", mc[i].ReturnData)
		if v == nil {
			s.logger.Warn("convertToAssets decode failed; underlying value will be NULL",
				"contract", e.ContractAddress.Hex(),
				"wallet", e.WalletAddress.Hex(),
				"blockHash", blockHash)
			continue
		}
		result.Balances[e.Key()].UnderlyingValue = v
	}
	return nil
}

func (s *ERC4626Source) fetchShares(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (map[EntryKey]*big.Int, []*TokenEntry, error) {
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

	mc, err := s.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, nil, fmt.Errorf("balanceOf multicall: %w", err)
	}

	shares := make(map[EntryKey]*big.Int, len(valid))
	var failures []string
	for i, e := range valid {
		if i >= len(mc) || !mc[i].Success || len(mc[i].ReturnData) == 0 {
			failures = append(failures, fmt.Sprintf("%s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex()))
			continue
		}
		unpacked, err := s.vaultABI.Unpack("balanceOf", mc[i].ReturnData)
		if err != nil || len(unpacked) == 0 {
			failures = append(failures, fmt.Sprintf("%s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex()))
			continue
		}
		v, ok := unpacked[0].(*big.Int)
		if !ok {
			failures = append(failures, fmt.Sprintf("%s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex()))
			continue
		}
		shares[e.Key()] = v
	}
	if len(failures) > 0 {
		return nil, nil, fmt.Errorf("erc4626 balanceOf call failures: %s", strings.Join(failures, ", "))
	}

	return shares, valid, nil
}
