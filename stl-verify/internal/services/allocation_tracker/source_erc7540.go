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

const erc7540ABIJson = `[
	{
		"inputs": [],
		"name": "share",
		"outputs": [{"name": "", "type": "address"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"inputs": [{"name": "account", "type": "address"}],
		"name": "balanceOf",
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`

// ERC7540Source fetches positions held through ERC-7540 async vaults (e.g.
// Centrifuge). The vault itself is not a token — the settled position is the
// wallet's balance of the vault's ERC-20 share token. Per fetch it resolves
// share() on each unique vault, then reads share.balanceOf(wallet), and stores
// the share amount in both Balance and ScaledBalance (matching ERC4626Source).
//
// In-flight amounts (pendingDepositRequest / claimableDepositRequest) are not
// read; balances reflect claimed shares only.
type ERC7540Source struct {
	multicaller outbound.Multicaller
	vaultABI    abi.ABI
	logger      *slog.Logger
}

func NewERC7540Source(multicaller outbound.Multicaller, logger *slog.Logger) (*ERC7540Source, error) {
	parsed, err := abi.JSON(strings.NewReader(erc7540ABIJson))
	if err != nil {
		return nil, fmt.Errorf("parse erc7540 ABI: %w", err)
	}
	return &ERC7540Source{
		multicaller: multicaller,
		vaultABI:    parsed,
		logger:      logger.With("source", "erc7540"),
	}, nil
}

func (s *ERC7540Source) Name() string { return "erc7540" }

// Supports claims token_type=centrifuge: those entries point at ERC-7540
// vaults. NOTE: BalanceOfSource currently also claims this type — this source
// stays unregistered until the axis-synome data moves the centrifuge entries
// from share-token to vault addresses; the registration and the BalanceOfSource
// removal must land together with that bump (VEC-337 part 2).
func (s *ERC7540Source) Supports(tokenType string, protocol string) bool {
	return tokenType == "centrifuge"
}

func (s *ERC7540Source) FetchBalances(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	shareTokens, err := s.resolveShares(ctx, entries, blockHash)
	if err != nil {
		return nil, fmt.Errorf("resolve vault shares: %w", err)
	}

	if err := s.checkDuplicateShares(entries, shareTokens); err != nil {
		return nil, err
	}

	balances, err := s.fetchShareBalances(ctx, entries, shareTokens, blockHash)
	if err != nil {
		return nil, fmt.Errorf("fetch share balances: %w", err)
	}

	for _, e := range entries {
		bal := balances[e.Key()]
		s.logger.Debug("erc7540 position",
			"vault", e.ContractAddress.Hex(),
			"share", shareTokens[e.ContractAddress].Hex(),
			"shares", bal.String())
		result.Balances[e.Key()] = &PositionBalance{
			Balance:       new(big.Int).Set(bal),
			ScaledBalance: new(big.Int).Set(bal),
		}
	}

	return result, nil
}

// resolveShares calls share() once per unique vault address and returns the
// vault → share token mapping. Any failed or undecodable call is a hard error:
// an entry routed here whose contract has no share() is misconfigured.
func (s *ERC7540Source) resolveShares(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (map[common.Address]common.Address, error) {
	data, err := s.vaultABI.Pack("share")
	if err != nil {
		return nil, fmt.Errorf("pack share: %w", err)
	}

	var vaults []common.Address
	seen := make(map[common.Address]bool)
	for _, e := range entries {
		if seen[e.ContractAddress] {
			continue
		}
		seen[e.ContractAddress] = true
		vaults = append(vaults, e.ContractAddress)
	}

	calls := make([]outbound.Call, len(vaults))
	for i, vault := range vaults {
		calls[i] = outbound.Call{Target: vault, AllowFailure: true, CallData: data}
	}

	mc, err := s.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("share multicall: %w", err)
	}

	shares := make(map[common.Address]common.Address, len(vaults))
	var failures []string
	for i, vault := range vaults {
		if i >= len(mc) || !mc[i].Success || len(mc[i].ReturnData) == 0 {
			failures = append(failures, vault.Hex())
			continue
		}
		unpacked, err := s.vaultABI.Unpack("share", mc[i].ReturnData)
		if err != nil || len(unpacked) == 0 {
			failures = append(failures, vault.Hex())
			continue
		}
		addr, ok := unpacked[0].(common.Address)
		if !ok || addr == (common.Address{}) {
			failures = append(failures, vault.Hex())
			continue
		}
		shares[vault] = addr
	}
	if len(failures) > 0 {
		return nil, fmt.Errorf("erc7540 share() call failures (not ERC-7540 vaults?): %s", strings.Join(failures, ", "))
	}

	return shares, nil
}

// checkDuplicateShares fails hard when two entries for the same wallet resolve
// to the same share token. ERC-7540 deploys one vault per (pool, tranche,
// deposit asset), so distinct vaults can front the same share — tracking both
// would read the same balanceOf twice and double count the position.
func (s *ERC7540Source) checkDuplicateShares(entries []*TokenEntry, shareTokens map[common.Address]common.Address) error {
	firstVault := make(map[string]common.Address, len(entries))
	for _, e := range entries {
		share := shareTokens[e.ContractAddress]
		key := fmt.Sprintf("%s/%s", share.Hex(), e.WalletAddress.Hex())
		if prev, ok := firstVault[key]; ok && prev != e.ContractAddress {
			return fmt.Errorf("vaults %s and %s both resolve to share %s for wallet %s; tracking both would double count",
				prev.Hex(), e.ContractAddress.Hex(), share.Hex(), e.WalletAddress.Hex())
		}
		firstVault[key] = e.ContractAddress
	}
	return nil
}

// fetchShareBalances reads share.balanceOf(wallet) for every entry, keyed by
// the entry's own key (vault + wallet).
func (s *ERC7540Source) fetchShareBalances(ctx context.Context, entries []*TokenEntry, shareTokens map[common.Address]common.Address, blockHash common.Hash) (map[EntryKey]*big.Int, error) {
	calls := make([]outbound.Call, len(entries))
	for i, e := range entries {
		data, err := s.vaultABI.Pack("balanceOf", e.WalletAddress)
		if err != nil {
			return nil, fmt.Errorf("pack balanceOf for %s: %w", e.WalletAddress.Hex(), err)
		}
		calls[i] = outbound.Call{Target: shareTokens[e.ContractAddress], AllowFailure: true, CallData: data}
	}

	mc, err := s.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("balanceOf multicall: %w", err)
	}

	balances := make(map[EntryKey]*big.Int, len(entries))
	var failures []string
	for i, e := range entries {
		var v *big.Int
		if i < len(mc) && mc[i].Success {
			v = unpackUint256(&s.vaultABI, "balanceOf", mc[i].ReturnData)
		}
		if v == nil {
			failures = append(failures, fmt.Sprintf("%s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex()))
			continue
		}
		balances[e.Key()] = v
	}
	if len(failures) > 0 {
		return nil, fmt.Errorf("erc7540 share balanceOf call failures: %s", strings.Join(failures, ", "))
	}

	return balances, nil
}
