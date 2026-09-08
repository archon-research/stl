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
	,{
		"inputs": [],
		"name": "totalSupply",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`

// ERC4626Source fetches vault share balances in two multicall rounds, both
// pinned to the same block hash. Round 1: balanceOf(wallet) per entry plus one
// totalSupply() per unique vault; round 2: convertToAssets(shares) per non-zero
// entry. Balance and ScaledBalance hold raw shares; UnderlyingValue holds the
// asset equivalent or NULL when convertToAssets reverts or is undecodable
// (NULL = unknown, not 0). Each vault's totalSupply is surfaced through
// FetchResult.Supplies (ScaledTotalSupply always nil — vaults have none).
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

func (s *ERC4626Source) FetchBalances(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	shares, valid1, err := s.fetchShares(ctx, entries, blockHash, result)
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
			return fmt.Errorf("pack convertToAssets for %s: %w", e.ContractAddress.Hex(), err)
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

// fetchShares is the first multicall round: balanceOf(wallet) per entry plus one
// totalSupply() per unique vault, all pinned to the same block hash. It decodes
// shares per entry and records each vault's supply into result.Supplies.
func (s *ERC4626Source) fetchShares(ctx context.Context, entries []*TokenEntry, blockHash common.Hash, result *FetchResult) (map[EntryKey]*big.Int, []*TokenEntry, error) {
	calls := make([]outbound.Call, 0, len(entries))
	var valid []*TokenEntry

	for _, e := range entries {
		data, err := s.vaultABI.Pack("balanceOf", e.WalletAddress)
		if err != nil {
			return nil, nil, fmt.Errorf("pack balanceOf for %s: %w", e.WalletAddress.Hex(), err)
		}
		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: data})
		valid = append(valid, e)
	}

	if len(calls) == 0 {
		return nil, nil, nil
	}

	supplyStart := len(calls)
	calls, supplyContracts, err := s.appendSupplyCalls(calls, valid)
	if err != nil {
		return nil, nil, err
	}

	mc, err := s.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, nil, fmt.Errorf("balanceOf multicall: %w", err)
	}
	if len(mc) != len(calls) {
		return nil, nil, fmt.Errorf("erc4626 round-1 multicall returned %d results for %d calls", len(mc), len(calls))
	}

	shares, err := s.decodeShares(valid, mc[:supplyStart])
	if err != nil {
		return nil, nil, err
	}
	if err := s.decodeSupplies(supplyContracts, mc[supplyStart:], blockHash, result); err != nil {
		return nil, nil, err
	}

	return shares, valid, nil
}

// appendSupplyCalls schedules one totalSupply() read per unique vault in the
// batch, deduped by contract address, returning the extended call list and the
// contracts in call order so decodeSupplies can map each result back.
func (s *ERC4626Source) appendSupplyCalls(calls []outbound.Call, entries []*TokenEntry) ([]outbound.Call, []common.Address, error) {
	seen := make(map[common.Address]bool)
	var contracts []common.Address
	for _, e := range entries {
		if seen[e.ContractAddress] {
			continue
		}
		seen[e.ContractAddress] = true
		data, err := s.vaultABI.Pack("totalSupply")
		if err != nil {
			return nil, nil, fmt.Errorf("pack totalSupply for %s: %w", e.ContractAddress.Hex(), err)
		}
		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: data})
		contracts = append(contracts, e.ContractAddress)
	}
	return calls, contracts, nil
}

func (s *ERC4626Source) decodeShares(valid []*TokenEntry, mc []outbound.Result) (map[EntryKey]*big.Int, error) {
	shares := make(map[EntryKey]*big.Int, len(valid))
	var failures []string
	for i, e := range valid {
		if !mc[i].Success || len(mc[i].ReturnData) == 0 {
			failures = append(failures, fmt.Sprintf("%s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex()))
			continue
		}
		v := unpackUint256(&s.vaultABI, "balanceOf", mc[i].ReturnData)
		if v == nil {
			failures = append(failures, fmt.Sprintf("%s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex()))
			continue
		}
		shares[e.Key()] = v
	}
	if len(failures) > 0 {
		return nil, fmt.Errorf("erc4626 balanceOf call failures: %s", strings.Join(failures, ", "))
	}
	return shares, nil
}

// decodeSupplies records each vault's totalSupply into result.Supplies. A
// totalSupply read on a call we issued is expected to succeed, so per the
// never-swallow / AllowFailure-still-bubbles-up rule a failed or undecodable
// return is escalated rather than dropped. The aToken source's warn-and-drop is
// an older deliberate deviation this source does not copy.
func (s *ERC4626Source) decodeSupplies(contracts []common.Address, mc []outbound.Result, blockHash common.Hash, result *FetchResult) error {
	for i, addr := range contracts {
		if !mc[i].Success || len(mc[i].ReturnData) == 0 {
			return fmt.Errorf("erc4626 totalSupply call failed for %s at block %s", addr.Hex(), blockHash)
		}
		v := unpackUint256(&s.vaultABI, "totalSupply", mc[i].ReturnData)
		if v == nil {
			return fmt.Errorf("erc4626 totalSupply decode failed for %s at block %s", addr.Hex(), blockHash)
		}
		result.Supplies[addr] = &PoolSupply{TotalSupply: v, ScaledTotalSupply: nil}
	}
	return nil
}
