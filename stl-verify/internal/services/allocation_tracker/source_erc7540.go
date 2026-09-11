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
	},
	{
		"inputs": [],
		"name": "decimals",
		"outputs": [{"name": "", "type": "uint8"}],
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

// The alias path finds this source by type assertion; a signature change here fails
// every block with "needs a share alias but its source cannot name one".
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
		share, err := shareFor(shares, e.ContractAddress)
		if err != nil {
			return nil, err
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
// vault → share token mapping. The axis-synome centrifuge entries are mixed and
// otherwise indistinguishable — grove's are ERC-7540 vaults, Spark's JTRSY is the
// share itself — so the shape is detected here: a decodable share() names the
// share; a revert is the direct-share shape only once confirmDirectShares has seen
// decimals() answer on the same address. Transport errors, an address with no
// code (Success with empty returndata) and malformed responses are hard errors.
func (s *ERC7540Source) resolveShares(ctx context.Context, entries []*TokenEntry, blockHash common.Hash) (map[common.Address]common.Address, error) {
	vaults := uniqueContracts(entries)
	mc, err := s.callEach(ctx, "share", vaults, blockHash)
	if err != nil {
		return nil, err
	}

	shares := make(map[common.Address]common.Address, len(vaults))
	var reverted []common.Address
	var failures []string
	for i, vault := range vaults {
		if i >= len(mc) {
			failures = append(failures, vault.Hex())
			continue
		}
		if !mc[i].Success {
			reverted = append(reverted, vault)
			continue
		}
		share, ok := s.decodeShare(mc[i].ReturnData)
		if !ok {
			failures = append(failures, vault.Hex())
			continue
		}
		shares[vault] = share
	}
	if len(failures) > 0 {
		return nil, fmt.Errorf("erc7540 share() resolution failures: %s", strings.Join(failures, ", "))
	}
	if err := s.confirmDirectShares(ctx, reverted, blockHash, shares); err != nil {
		return nil, err
	}
	return shares, nil
}

// confirmDirectShares admits a reverting share() as the direct-share shape only
// when decimals() answers on the same address: a vault has neither, a token has
// the second, so one wrong revert cannot key a vault onto itself.
func (s *ERC7540Source) confirmDirectShares(ctx context.Context, candidates []common.Address, blockHash common.Hash, shares map[common.Address]common.Address) error {
	if len(candidates) == 0 {
		return nil
	}
	mc, err := s.callEach(ctx, "decimals", candidates, blockHash)
	if err != nil {
		return err
	}
	var failures []string
	for i, addr := range candidates {
		if i >= len(mc) || !mc[i].Success || !s.decodesAsDecimals(mc[i].ReturnData) {
			failures = append(failures, addr.Hex())
			continue
		}
		shares[addr] = addr
	}
	if len(failures) > 0 {
		return fmt.Errorf("erc7540 share() reverted and decimals() did not answer, neither a vault nor a token: %s", strings.Join(failures, ", "))
	}
	return nil
}

// callEach issues one no-argument view call per target, each allowed to fail so
// a revert is an answer about that address rather than a failure of the batch.
func (s *ERC7540Source) callEach(ctx context.Context, method string, targets []common.Address, blockHash common.Hash) ([]outbound.Result, error) {
	data, err := s.vaultABI.Pack(method)
	if err != nil {
		return nil, fmt.Errorf("pack %s: %w", method, err)
	}
	calls := make([]outbound.Call, len(targets))
	for i, target := range targets {
		calls[i] = outbound.Call{Target: target, AllowFailure: true, CallData: data}
	}
	mc, err := s.multicaller.ExecuteAtHash(ctx, calls, blockHash)
	if err != nil {
		return nil, fmt.Errorf("%s multicall: %w", method, err)
	}
	return mc, nil
}

func (s *ERC7540Source) decodeShare(data []byte) (common.Address, bool) {
	if len(data) == 0 {
		return common.Address{}, false
	}
	unpacked, err := s.vaultABI.Unpack("share", data)
	if err != nil || len(unpacked) == 0 {
		return common.Address{}, false
	}
	addr, ok := unpacked[0].(common.Address)
	if !ok || addr == (common.Address{}) {
		return common.Address{}, false
	}
	return addr, true
}

func (s *ERC7540Source) decodesAsDecimals(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	unpacked, err := s.vaultABI.Unpack("decimals", data)
	return err == nil && len(unpacked) == 1
}

// uniqueContracts keeps the first occurrence of each contract, in entry order.
func uniqueContracts(entries []*TokenEntry) []common.Address {
	var out []common.Address
	seen := make(map[common.Address]bool, len(entries))
	for _, e := range entries {
		if seen[e.ContractAddress] {
			continue
		}
		seen[e.ContractAddress] = true
		out = append(out, e.ContractAddress)
	}
	return out
}

// shareFor is the one lookup into a resolved share map; a miss is a programming
// error, since shareTokens answers for every entry it was given or fails.
func shareFor(shares map[common.Address]common.Address, vault common.Address) (common.Address, error) {
	share, ok := shares[vault]
	if !ok {
		return common.Address{}, fmt.Errorf("no share token resolved for vault %s", vault.Hex())
	}
	return share, nil
}

// checkDuplicateShares fails hard when two entries for the same wallet resolve
// to the same share token. ERC-7540 deploys one vault per (pool, tranche,
// deposit asset), so distinct vaults can front the same share — tracking both
// would read the same balanceOf twice and double count the position.
func (s *ERC7540Source) checkDuplicateShares(entries []*TokenEntry, shareTokens map[common.Address]common.Address) error {
	firstVault := make(map[string]common.Address, len(entries))
	for _, e := range entries {
		share, err := shareFor(shareTokens, e.ContractAddress)
		if err != nil {
			return err
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
		share, err := shareFor(shareTokens, e.ContractAddress)
		if err != nil {
			return nil, err
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
