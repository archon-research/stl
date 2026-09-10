package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// blockKey identifies one multicall batch: every row sharing a (chain,
// block_number) pair is read in a single aggregate3 call, since a multicall
// is evaluated at exactly one block. There is no per-row block_hash to pin
// to (allocation_position predates that column), so the read is number-pinned
// -- the documented fallback ExecutePinned uses for backfills replaying
// already-settled blocks with no live fork ambiguity (see VEC-471).
type blockKey struct {
	chainID     int64
	blockNumber int64
}

// erc4626ArchiveResolver batches convertToAssets(shares) reads for erc4626-like
// receipt tokens against a real archive RPC, one multicall per distinct block
// among the candidates rather than one eth_call per row.
type erc4626ArchiveResolver struct {
	erc4626ABI   *abi.ABI
	multicallers map[int64]outbound.Multicaller // memoized per chain; dialed lazily
}

func newERC4626ArchiveResolver() (*erc4626ArchiveResolver, error) {
	erc4626ABI, err := abis.GetERC4626ABI()
	if err != nil {
		return nil, fmt.Errorf("loading erc4626 ABI: %w", err)
	}
	return &erc4626ArchiveResolver{
		erc4626ABI:   erc4626ABI,
		multicallers: make(map[int64]outbound.Multicaller),
	}, nil
}

// multicallerForChain dials a chain's archive RPC on first use and memoizes
// it. A test pre-populates multicallers directly (an outbound port, mocked
// per repo convention) so it never runs this dial.
func (r *erc4626ArchiveResolver) multicallerForChain(ctx context.Context, chainID int64) (outbound.Multicaller, error) {
	if mc, ok := r.multicallers[chainID]; ok {
		return mc, nil
	}
	rpcURL, err := chainutil.AlchemyRPCURL(chainID)
	if err != nil {
		return nil, fmt.Errorf("resolving archive RPC for chain %d: %w", chainID, err)
	}
	ethClient, err := rpchttp.DialEthereum(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("dialing archive RPC for chain %d: %w", chainID, err)
	}
	mc, err := multicall.NewClient(ethClient, blockchain.Multicall3)
	if err != nil {
		return nil, fmt.Errorf("creating multicaller for chain %d: %w", chainID, err)
	}
	r.multicallers[chainID] = mc
	return mc, nil
}

// isERC4626ArchiveCandidate reports whether a row is an erc4626-like receipt
// token with enough registry data to attempt a real conversion read. Rows the
// registry cannot resolve an underlying for are left to the same fallback
// path a missing archive result takes, unchanged from before this file existed.
func isERC4626ArchiveCandidate(c candidateRow) bool {
	return c.isReceiptToken && !c.underlyingIsOneToOne && c.underlyingAddress != nil && c.underlyingDecimals != nil
}

// resolve reads convertToAssets(balance) for every erc4626 candidate at its
// own pinned block_number, batched by block. A row absent from the returned
// map is not an error: the vault call reverted (AllowFailure), which is a
// legitimate historical gap -- an archive node lacking that exact state, or
// the vault not yet deployed -- and the caller falls back to the price-ratio
// derivation for it. A transport-level failure (the eth_call itself erroring,
// a decode failure) is returned as an error and aborts the run: those are not
// a per-vault fact, so continuing would silently drop rows this run could
// have gotten right.
func (r *erc4626ArchiveResolver) resolve(ctx context.Context, candidates []candidateRow, logger *slog.Logger) (map[int]*big.Int, error) {
	groups := make(map[blockKey][]int)
	for i, c := range candidates {
		if !isERC4626ArchiveCandidate(c) {
			continue
		}
		key := blockKey{chainID: c.chainID, blockNumber: c.blockNumber}
		groups[key] = append(groups[key], i)
	}

	out := make(map[int]*big.Int, len(candidates))
	for key, idxs := range groups {
		if err := r.resolveGroup(ctx, key, idxs, candidates, out, logger); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (r *erc4626ArchiveResolver) resolveGroup(
	ctx context.Context,
	key blockKey,
	idxs []int,
	candidates []candidateRow,
	out map[int]*big.Int,
	logger *slog.Logger,
) error {
	mc, err := r.multicallerForChain(ctx, key.chainID)
	if err != nil {
		return err
	}

	calls := make([]outbound.Call, len(idxs))
	for j, idx := range idxs {
		data, err := r.erc4626ABI.Pack("convertToAssets", candidates[idx].balance)
		if err != nil {
			return fmt.Errorf("packing convertToAssets for chain %d block %d: %w", key.chainID, key.blockNumber, err)
		}
		calls[j] = outbound.Call{Target: candidates[idx].tokenAddress, AllowFailure: true, CallData: data}
	}

	results, err := blockchain.ExecutePinned(ctx, mc, calls, key.blockNumber, common.Hash{})
	if err != nil {
		return fmt.Errorf("archive multicall for chain %d block %d: %w", key.chainID, key.blockNumber, err)
	}
	if len(results) != len(idxs) {
		return fmt.Errorf("archive multicall for chain %d block %d: expected %d results, got %d",
			key.chainID, key.blockNumber, len(idxs), len(results))
	}

	for j, idx := range idxs {
		if !results[j].Success {
			logger.Warn("convertToAssets reverted, falling back to price ratio",
				"chain_id", key.chainID, "block_number", key.blockNumber, "vault", candidates[idx].tokenAddress.Hex())
			continue
		}
		assets, err := blockchain.UnpackConvertToAssets(r.erc4626ABI, results[j].ReturnData)
		if err != nil {
			return fmt.Errorf("decoding convertToAssets for chain %d block %d vault %s: %w",
				key.chainID, key.blockNumber, candidates[idx].tokenAddress.Hex(), err)
		}
		out[idx] = assets
	}
	return nil
}
