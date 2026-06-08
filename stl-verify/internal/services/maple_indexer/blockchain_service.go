package maple_indexer

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel/attribute"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// shareUnit returns the share-side argument passed to convertToAssets() when
// computing the displayed share price: 10^decimals, i.e. "1 whole share" in
// the vault's own precision. An ERC-4626 vault inherits its share decimals
// from the underlying asset at deploy time, so a 6-decimal vault (SyrupUSDC /
// SyrupUSDT) uses 1e6 while an 18-decimal vault uses 1e18. The on-chain share
// price is recorded in the vault's `decimals` precision; SharePrice() /
// share_unit gives a 1.0-anchored rate that downstream consumers (APY, charts)
// can use without divisor gymnastics.
//
// decimals comes from the vault's underlying-asset token row (loaded into the
// registry by the repository), so the share unit tracks the seeded decimals
// instead of a hardcoded assumption.
//
// Returns a fresh *big.Int each call to prevent callers from mutating a shared value.
func shareUnit(decimals uint8) *big.Int {
	return new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
}

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

	// chunkSize bounds the per-multicall call count in FetchUserPositions so a
	// high-churn block can't produce one oversized aggregate that reverts
	// wholesale. Always > 0 (NewBlockchainService clamps a non-positive arg to
	// DefaultMulticallChunkSize).
	chunkSize int

	// Pre-packed call data for the no-argument vault-level views, so we
	// don't re-pack on every block. Vault-level views are call-data-free
	// (no per-vault arg), so packing once is correct.
	totalAssetsData []byte
	totalSupplyData []byte
}

// NewBlockchainService loads the Syrup views ABI and pre-packs the
// no-argument view calls. A non-positive chunkSize falls back to
// DefaultMulticallChunkSize so callers can't accidentally request zero-size
// chunks.
func NewBlockchainService(mc outbound.Multicaller, telemetry *Telemetry, chunkSize int) (*BlockchainService, error) {
	if mc == nil {
		return nil, fmt.Errorf("multicaller is nil")
	}
	if chunkSize <= 0 {
		chunkSize = DefaultMulticallChunkSize
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
		chunkSize:       chunkSize,
		totalAssetsData: ta,
		totalSupplyData: ts,
	}, nil
}

// FetchVaultState issues a single multicall returning (totalAssets,
// totalSupply, convertToAssets(shareUnit)) for the vault at the given block.
//
// decimals is the vault's share decimals (10^decimals = one whole share);
// it must be non-zero. A zero value means the caller never resolved the
// vault's decimals — we reject it rather than silently defaulting to 6, which
// would mis-scale the share price for a non-6-decimal vault.
func (s *BlockchainService) FetchVaultState(ctx context.Context, vault common.Address, decimals uint8, blockNumber *big.Int) (_ *VaultStateRaw, retErr error) {
	start := time.Now()
	ctx, span := s.telemetry.StartSpan(ctx, "maple.rpc.fetchVaultState",
		attribute.String("vault", vault.Hex()),
	)
	defer span.End()
	defer func() { s.telemetry.RecordRPCCall(ctx, "fetchVaultState", time.Since(start), retErr) }()

	if decimals == 0 {
		return nil, fmt.Errorf("vault %s: decimals must be non-zero for share-unit computation", vault.Hex())
	}

	convData, err := s.viewABI.Pack("convertToAssets", shareUnit(decimals))
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
	balanceResults, err := s.executeChunked(ctx, balanceCalls, blockNumber)
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

	// convertToAssets(0) is deterministically 0, so short-circuit zero-share
	// users locally and only batch non-zero accounts. This cuts RPC fan-out and
	// latency on a hot path.
	out := make(map[common.Address]*UserPosition, len(users))
	nonZeroUsers := make([]common.Address, 0, len(users))
	nonZeroBalances := make([]*big.Int, 0, len(users))
	assetCalls := make([]outbound.Call, 0, len(users))
	for i, b := range balances {
		if b.Sign() == 0 {
			out[users[i]] = &UserPosition{Shares: b, Assets: big.NewInt(0)}
			continue
		}
		data, err := s.viewABI.Pack("convertToAssets", b)
		if err != nil {
			return nil, fmt.Errorf("packing convertToAssets: %w", err)
		}
		nonZeroUsers = append(nonZeroUsers, users[i])
		nonZeroBalances = append(nonZeroBalances, b)
		assetCalls = append(assetCalls, outbound.Call{Target: vault, CallData: data})
	}
	if len(assetCalls) == 0 {
		return out, nil
	}
	assetStart := time.Now()
	assetResults, err := s.executeChunked(ctx, assetCalls, blockNumber)
	s.telemetry.RecordRPCCall(ctx, "convertToAssets", time.Since(assetStart), err)
	if err != nil {
		return nil, fmt.Errorf("multicall convertToAssets for vault %s: %w", vault.Hex(), err)
	}
	if len(assetResults) != len(assetCalls) {
		return nil, fmt.Errorf("convertToAssets returned %d results, expected %d", len(assetResults), len(assetCalls))
	}

	for i, u := range nonZeroUsers {
		assets, err := s.decodeUint256("convertToAssets", assetResults[i])
		if err != nil {
			return nil, fmt.Errorf("convertToAssets(%s): %w", u.Hex(), err)
		}
		out[u] = &UserPosition{Shares: nonZeroBalances[i], Assets: assets}
	}
	return out, nil
}

// executeChunked runs one multicall per fixed-size chunk of calls and stitches
// the per-chunk results back into a single slice, preserving call order.
// Chunking caps the size of any single aggregate3 so a high-churn block can't
// build one oversized multicall that exceeds the node's eth_call gas cap or the
// RPC provider's compute limit and reverts wholesale.
//
// Fails hard: any chunk error aborts the whole batch with a wrapped error and
// no partial results. The caller redelivers and reprocesses the block; writes
// are idempotent, so a retry is safe.
func (s *BlockchainService) executeChunked(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	results := make([]outbound.Result, 0, len(calls))
	for chunk := range slices.Chunk(calls, s.chunkSize) {
		chunkResults, err := s.multicaller.Execute(ctx, chunk, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("multicall chunk of %d calls: %w", len(chunk), err)
		}
		// Assert each chunk's length here, where the boundary is known. The
		// call-site aggregate check only validates the stitched total — two
		// chunks with compensating over/under counts would net to the right
		// total yet shift every later result by a position, silently writing
		// one user's balance under another's address.
		if len(chunkResults) != len(chunk) {
			return nil, fmt.Errorf("multicall chunk returned %d results, expected %d", len(chunkResults), len(chunk))
		}
		results = append(results, chunkResults...)
	}
	return results, nil
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
