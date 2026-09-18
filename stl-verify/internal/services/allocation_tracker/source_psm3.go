package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// PSM3Source values Spark's PSM3 LP stake on each L2 deployment (VEC-833) from
// the already-indexed psm3_alm_shares table rather than a live chain read:
// PSM3.shares is a non-transferable internal mapping with no ERC20 surface (no
// balanceOf/transfer/approve), so a generic balanceOf-style source cannot read
// it. The psm3-indexer already walks the chain and stores both the raw shares
// and their on-chain par valuation (PSM3.convertToAssetValue) per sweep.
//
// Balance/ScaledBalance hold the raw internal share count and UnderlyingValue
// the raw par valuation, both PSM3's own 1e18 "USD-like" accounting units, not
// a token amount (see psm3_alm_shares' column comments) -- rescaling into the
// entry's hint asset's own decimals happens in
// PrimePositionHandler.underlyingValuation, which already has that asset's
// live-fetched decimals.
type PSM3Source struct {
	reader     outbound.Psm3PositionReader
	blockState outbound.BlockHashResolver
	logger     *slog.Logger
}

// NewPSM3Source creates a new PSM3Source backed by Postgres reads.
func NewPSM3Source(reader outbound.Psm3PositionReader, blockState outbound.BlockHashResolver, logger *slog.Logger) *PSM3Source {
	return &PSM3Source{
		reader:     reader,
		blockState: blockState,
		logger:     logger.With("component", "psm3-source"),
	}
}

// Name returns the source name.
func (s *PSM3Source) Name() string { return "psm3" }

// Supports returns true for the psm3 token type.
func (s *PSM3Source) Supports(tokenType, protocol string) bool {
	return tokenType == "psm3"
}

// FetchBalances values every entry's PSM3 ALM stake as of blockHash. Unlike
// UniV4Source there is no discovery/registry step: each entry already names
// its own PSM3 pool and ALM proxy directly.
func (s *PSM3Source) FetchBalances(
	ctx context.Context,
	entries []*TokenEntry,
	blockHash common.Hash,
) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	blockNumber, blockTimestamp, err := s.resolveBlockCoordinates(ctx, blockHash)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if err := s.fetchEntryBalance(ctx, entry, blockNumber, blockTimestamp, result.Balances); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// fetchEntryBalance values one PSM3 entry. A held stake with no indexed share
// reading is an invariant break, not a legitimate zero: the entry says this
// ALM is tracked on this PSM3, so an absent snapshot means the indexer has a
// coverage gap, not that the stake never existed (mirrors UniV4Source's
// positionValue).
func (s *PSM3Source) fetchEntryBalance(
	ctx context.Context,
	entry *TokenEntry,
	blockNumber int64,
	blockTimestamp time.Time,
	result map[EntryKey]*PositionBalance,
) error {
	chainID, ok := chainIDForName(entry.Chain)
	if !ok {
		return fmt.Errorf("no chain ID registered for chain %q (psm3 entry %s/%s)",
			entry.Chain, entry.ContractAddress.Hex(), entry.WalletAddress.Hex())
	}

	snapshot, err := s.reader.AlmShareAtBlock(ctx, chainID, entry.ContractAddress, entry.WalletAddress, blockNumber, blockTimestamp)
	if err != nil {
		return fmt.Errorf("psm3 alm share for %s/%s: %w", entry.ContractAddress.Hex(), entry.WalletAddress.Hex(), err)
	}
	if snapshot == nil {
		return fmt.Errorf(
			"psm3 ALM %s has no indexed share reading in pool %s at or before block %d",
			entry.WalletAddress.Hex(), entry.ContractAddress.Hex(), blockNumber,
		)
	}

	s.logger.Debug("computed PSM3 ALM stake",
		"pool", entry.ContractAddress.Hex(),
		"alm", entry.WalletAddress.Hex(),
		"shares", snapshot.Shares,
		"assetValue", snapshot.AssetValue,
	)
	result[entry.Key()] = &PositionBalance{
		Balance:         new(big.Int).Set(snapshot.Shares),
		ScaledBalance:   new(big.Int).Set(snapshot.Shares),
		UnderlyingValue: new(big.Int).Set(snapshot.AssetValue),
	}
	return nil
}

// resolveBlockCoordinates looks blockHash up in block_states, exactly as
// UniV4Source's resolveBlockCoordinates does (see its doc comment for why
// this lookup is always safe here).
func (s *PSM3Source) resolveBlockCoordinates(ctx context.Context, blockHash common.Hash) (int64, time.Time, error) {
	state, err := s.blockState.GetBlockByHash(ctx, blockHash.Hex())
	if err != nil {
		return 0, time.Time{}, fmt.Errorf("resolve block number for hash %s: %w", blockHash.Hex(), err)
	}
	if state == nil {
		return 0, time.Time{}, fmt.Errorf(
			"block hash %s not found in block_states; cannot resolve a block number for the psm3 read",
			blockHash.Hex(),
		)
	}
	return state.Number, time.Unix(state.BlockTimestamp, 0).UTC(), nil
}
