package entity

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// BlockQuerier abstracts block number lookups from an Ethereum node.
type BlockQuerier interface {
	BlockNumber(ctx context.Context) (uint64, error)
}

// DebtQuery represents a single vault to query debt for.
type DebtQuery struct {
	Ilk          [32]byte
	VaultAddress common.Address
}

// DebtResult holds the on-chain debt data for one vault. Exactly one of
// {Rate+Art populated, Reverted=true, Err != nil} is meaningful:
//
//   - Rate+Art populated: success.
//   - Reverted=true: the contract returned `Success: false` for at least
//     one of the per-vault calls (vat.ilks or vat.urns). This is the
//     contract's "no debt this block" signal under AllowFailure: true and
//     the consumer should skip-and-continue without retrying. Distinguished
//     from Err so the consumer doesn't have to re-classify a stringly-typed
//     error or unwrap an rpc.Error that the producer already discarded.
//   - Err != nil: a non-revert structural problem — ABI decode failure,
//     unexpected type, missing multicall result, etc. The consumer should
//     surface this as a hard error (NACK the source event for investigation).
//
// Whole-batch transport failures (HTTP 429 / 5xx / network) come back as
// the outer `error` from VatCaller.ReadDebts, not as a per-vault DebtResult.
type DebtResult struct {
	VaultAddress common.Address
	Rate         *big.Int // cumulative stability fee rate (ray, 1e27)
	Art          *big.Int // normalized debt (wad, 1e18)
	Reverted     bool     // contract-level "no data this block" — skip
	Err          error    // structural failure — hard error
}
