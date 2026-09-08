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

// The alias path finds this source by type assertion, so a signature change here
// would silently stop every share transfer from matching (TestCentrifugeRoutesToAShareResolver).
var _ shareResolver = (*ERC7540Source)(nil)

func (s *ERC7540Source) Name() string { return "erc7540" }

// Supports claims token_type=centrifuge, whose entries point at ERC-7540 vault
// addresses this source resolves to their share() token. BalanceOfSource does
// not claim it: balanceOf/decimals revert on a vault and poison-stall the block.
func (s *ERC7540Source) Supports(tokenType string, protocol string) bool {
	return tokenType == TokenTypeCentrifuge
}

func (s *ERC7540Source) FetchBalances(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	shares, err := s.shareTokens(ctx, entries, blockHash)
	if err != nil {
		return nil, err
	}

	balances, err := s.fetchShareBalances(ctx, entries, shares, blockHash)
	if err != nil {
		return nil, fmt.Errorf("fetch share balances: %w", err)
	}

	for _, e := range entries {
		bal, ok := balances[e.Key()]
		if !ok {
			return nil, fmt.Errorf("no balance read for entry %s/%s", e.ContractAddress.Hex(), e.WalletAddress.Hex())
		}
		share, ok := shares[e.ContractAddress]
		if !ok {
			return nil, fmt.Errorf("no share token resolved for vault %s", e.ContractAddress.Hex())
		}
		s.logger.Debug("erc7540 position",
			"vault", e.ContractAddress.Hex(),
			"share", share.Hex(),
			"shares", bal.String())
		result.Balances[e.Key()] = &PositionBalance{
			Balance:       new(big.Int).Set(bal),
			ScaledBalance: new(big.Int).Set(bal),
			ShareToken:    &share,
		}
	}

	return result, nil
}

// shareTokens is the single way in to the vault → share mapping — this source's
// own read and the alias path's — so the double-count guard cannot be bypassed
// by taking one and not the other.
func (s *ERC7540Source) shareTokens(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (map[common.Address]common.Address, error) {
	shares, err := s.resolveShares(ctx, entries, blockHash)
	if err != nil {
		return nil, fmt.Errorf("resolve vault shares: %w", err)
	}
	if err := s.checkDuplicateShares(entries, shares); err != nil {
		return nil, err
	}
	return shares, nil
}

// resolveShares calls share() once per unique vault address and returns the
// vault → share token mapping. A transport error or an undecodable response is a
// hard error; a clean revert is the direct-share shape (see the branch below).
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
		if i >= len(mc) {
			failures = append(failures, vault.Hex())
			continue
		}
		// A clean revert (the call executed and reverted) means the contract has
		// no share() method: the address is itself the ERC-20 share token, not an
		// ERC-7540 vault. The axis-synome 0.2.0 centrifuge migration is mixed —
		// grove's entries are vaults, Spark's JTRSY stayed a direct share — and
		// the entries are otherwise indistinguishable, so we detect the shape here
		// rather than route on it. An address with no code is NOT this case: it
		// returns Success with empty returndata and fails at the branch below, as do
		// transport errors and malformed responses (short slice, undecodable data,
		// zero address).
		if !mc[i].Success {
			shares[vault] = vault
			continue
		}
		if len(mc[i].ReturnData) == 0 {
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
		return nil, fmt.Errorf("erc7540 share() resolution failures: %s", strings.Join(failures, ", "))
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
		share, ok := shareTokens[e.ContractAddress]
		if !ok {
			return fmt.Errorf("no share token resolved for vault %s", e.ContractAddress.Hex())
		}
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
		share, ok := shareTokens[e.ContractAddress]
		if !ok {
			return nil, fmt.Errorf("no share token resolved for vault %s", e.ContractAddress.Hex())
		}
		calls[i] = outbound.Call{Target: share, AllowFailure: true, CallData: data}
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
