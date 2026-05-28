package maple_indexer

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// shareUnit is the share-side argument passed to convertToAssets() when
// computing the displayed share price. Syrup vaults (USDC/USDT) use 6
// decimals, matching their underlying asset, so 1e6 shares = "1 share unit"
// for price-display purposes. The on-chain share price is recorded in the
// vault's `decimals` precision; SharePrice() / share_unit gives a 1.0-anchored
// rate that downstream consumers (APY, charts) can use without divisor
// gymnastics.
//
// If Maple ever ships a Syrup vault with a non-6-decimal underlying, the
// blockchain service will need to read decimals() first and use 10^decimals
// here. For now SyrupUSDC + SyrupUSDT are the only known instances and
// both are 6.
var shareUnit = big.NewInt(1_000_000)

// VaultStateRaw is the on-chain snapshot of a Syrup vault at a given block,
// before mapping to the domain entity.
type VaultStateRaw struct {
	TotalAssets *big.Int
	TotalSupply *big.Int
	SharePrice  *big.Int // convertToAssets(shareUnit)
}

// UserPosition is the on-chain snapshot of a single user's stake in a Syrup
// vault at a given block.
type UserPosition struct {
	Shares *big.Int
	Assets *big.Int
}

// BlockchainService wraps multicall3 + the Syrup view ABI for the maple
// indexer. It is stateless across blocks; the service is safe to share
// across goroutines as long as the underlying outbound.Multicaller is.
type BlockchainService struct {
	multicaller outbound.Multicaller
	viewABI     *abi.ABI
	telemetry   *Telemetry

	// Pre-packed call data for the no-argument vault-level views, so we
	// don't re-pack on every block. Vault-level views are call-data-free
	// (no per-vault arg), so packing once is correct.
	totalAssetsData []byte
	totalSupplyData []byte
}

// NewBlockchainService loads the Syrup views ABI and pre-packs the
// no-argument view calls.
func NewBlockchainService(mc outbound.Multicaller, telemetry *Telemetry) (*BlockchainService, error) {
	if mc == nil {
		return nil, fmt.Errorf("multicaller is nil")
	}
	a, err := abis.GetSyrupVaultViewsABI()
	if err != nil {
		return nil, fmt.Errorf("loading Syrup views ABI: %w", err)
	}
	pack := func(method string) ([]byte, error) {
		data, err := a.Pack(method)
		if err != nil {
			return nil, fmt.Errorf("packing %s: %w", method, err)
		}
		return data, nil
	}
	ta, err := pack("totalAssets")
	if err != nil {
		return nil, err
	}
	ts, err := pack("totalSupply")
	if err != nil {
		return nil, err
	}
	return &BlockchainService{
		multicaller:     mc,
		viewABI:         a,
		telemetry:       telemetry,
		totalAssetsData: ta,
		totalSupplyData: ts,
	}, nil
}

// FetchVaultState issues a single multicall returning (totalAssets,
// totalSupply, convertToAssets(shareUnit)) for the vault at the given block.
func (s *BlockchainService) FetchVaultState(ctx context.Context, vault common.Address, blockNumber *big.Int) (_ *VaultStateRaw, retErr error) {
	start := time.Now()
	ctx, span := s.telemetry.StartSpan(ctx, "maple.rpc.fetchVaultState",
		attribute.String("vault", vault.Hex()),
	)
	defer span.End()
	defer func() { s.telemetry.RecordRPCCall(ctx, "fetchVaultState", time.Since(start), retErr) }()

	convData, err := s.viewABI.Pack("convertToAssets", shareUnit)
	if err != nil {
		return nil, fmt.Errorf("packing convertToAssets: %w", err)
	}

	calls := []outbound.Call{
		{Target: vault, CallData: s.totalAssetsData},
		{Target: vault, CallData: s.totalSupplyData},
		{Target: vault, CallData: convData},
	}

	results, err := s.multicaller.Execute(ctx, calls, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("multicall vault state for %s: %w", vault.Hex(), err)
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("multicall returned %d results, expected %d", len(results), len(calls))
	}

	totalAssets, err := s.decodeUint256("totalAssets", results[0])
	if err != nil {
		return nil, fmt.Errorf("vault %s totalAssets: %w", vault.Hex(), err)
	}
	totalSupply, err := s.decodeUint256("totalSupply", results[1])
	if err != nil {
		return nil, fmt.Errorf("vault %s totalSupply: %w", vault.Hex(), err)
	}
	sharePrice, err := s.decodeUint256("convertToAssets", results[2])
	if err != nil {
		return nil, fmt.Errorf("vault %s convertToAssets: %w", vault.Hex(), err)
	}

	return &VaultStateRaw{
		TotalAssets: totalAssets,
		TotalSupply: totalSupply,
		SharePrice:  sharePrice,
	}, nil
}

// FetchUserPositions returns each user's share balance and the asset
// equivalent (`convertToAssets(balance)`) at the given block. Implemented
// as two multicalls back-to-back: balanceOf for every user, then
// convertToAssets for every returned balance.
//
// Two-call structure is necessary because convertToAssets takes the
// share-balance as input — we can't pack the second call set until the
// first batch returns. Both batches are issued in a single goroutine; the
// caller's context controls cancellation.
//
// Returns an empty map (not nil) when users is empty so callers can range
// without nil-checking.
func (s *BlockchainService) FetchUserPositions(
	ctx context.Context,
	vault common.Address,
	users []common.Address,
	blockNumber *big.Int,
) (map[common.Address]*UserPosition, error) {
	if len(users) == 0 {
		return map[common.Address]*UserPosition{}, nil
	}

	ctx, span := s.telemetry.StartSpan(ctx, "maple.rpc.fetchUserPositions",
		attribute.String("vault", vault.Hex()),
	)
	defer span.End()

	balanceCalls := make([]outbound.Call, len(users))
	for i, u := range users {
		data, err := s.viewABI.Pack("balanceOf", u)
		if err != nil {
			return nil, fmt.Errorf("packing balanceOf(%s): %w", u.Hex(), err)
		}
		balanceCalls[i] = outbound.Call{Target: vault, CallData: data}
	}
	balanceStart := time.Now()
	balanceResults, err := s.multicaller.Execute(ctx, balanceCalls, blockNumber)
	s.telemetry.RecordRPCCall(ctx, "balanceOf", time.Since(balanceStart), err)
	if err != nil {
		return nil, fmt.Errorf("multicall balanceOf for vault %s: %w", vault.Hex(), err)
	}
	if len(balanceResults) != len(users) {
		return nil, fmt.Errorf("balanceOf returned %d results, expected %d", len(balanceResults), len(users))
	}

	balances := make([]*big.Int, len(users))
	for i, u := range users {
		bal, err := s.decodeUint256("balanceOf", balanceResults[i])
		if err != nil {
			return nil, fmt.Errorf("balanceOf(%s): %w", u.Hex(), err)
		}
		balances[i] = bal
	}

	assetCalls := make([]outbound.Call, len(users))
	for i, b := range balances {
		data, err := s.viewABI.Pack("convertToAssets", b)
		if err != nil {
			return nil, fmt.Errorf("packing convertToAssets: %w", err)
		}
		assetCalls[i] = outbound.Call{Target: vault, CallData: data}
	}
	assetStart := time.Now()
	assetResults, err := s.multicaller.Execute(ctx, assetCalls, blockNumber)
	s.telemetry.RecordRPCCall(ctx, "convertToAssets", time.Since(assetStart), err)
	if err != nil {
		return nil, fmt.Errorf("multicall convertToAssets for vault %s: %w", vault.Hex(), err)
	}
	if len(assetResults) != len(users) {
		return nil, fmt.Errorf("convertToAssets returned %d results, expected %d", len(assetResults), len(users))
	}

	out := make(map[common.Address]*UserPosition, len(users))
	for i, u := range users {
		assets, err := s.decodeUint256("convertToAssets", assetResults[i])
		if err != nil {
			return nil, fmt.Errorf("convertToAssets(%s): %w", u.Hex(), err)
		}
		out[u] = &UserPosition{Shares: balances[i], Assets: assets}
	}
	return out, nil
}

// decodeUint256 unpacks an ABI-encoded uint256 return value from a multicall result.
func (s *BlockchainService) decodeUint256(method string, r outbound.Result) (*big.Int, error) {
	if !r.Success {
		return nil, fmt.Errorf("%s call reverted", method)
	}
	if len(r.ReturnData) == 0 {
		return nil, fmt.Errorf("%s returned empty data", method)
	}
	vals, err := s.viewABI.Unpack(method, r.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking %s: %w", method, err)
	}
	if len(vals) != 1 {
		return nil, fmt.Errorf("%s expected 1 return value, got %d", method, len(vals))
	}
	v, ok := vals[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("%s: expected *big.Int, got %T", method, vals[0])
	}
	return v, nil
}
