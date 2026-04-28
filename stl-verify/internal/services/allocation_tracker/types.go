package allocation_tracker

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Direction indicates whether tokens moved into or out of the ALM proxy.
type Direction string

const (
	DirectionIn    Direction = "in"
	DirectionOut   Direction = "out"
	DirectionSweep Direction = "sweep"
)

// TokenEntry represents a single known position from the TOKENS_DATA registry.
type TokenEntry struct {
	ContractAddress common.Address
	WalletAddress   common.Address
	AssetAddress    *common.Address
	Star            string
	Chain           string
	Protocol        string
	AllocationType  string
	TokenType       string
	CreatedAtBlock  *int64
}

// EntryKey uniquely identifies a position.
type EntryKey struct {
	ContractAddress common.Address
	WalletAddress   common.Address
}

func (e *TokenEntry) Key() EntryKey {
	return EntryKey{ContractAddress: e.ContractAddress, WalletAddress: e.WalletAddress}
}

// PositionBalance is what a PositionSource returns per entry.
type PositionBalance struct {
	Balance       *big.Int // primary tracked balance in the entry's tracked unit (token units for ERC20-like entries; underlying-asset units for pool-style entries like UniV3)
	ScaledBalance *big.Int // optional auxiliary balance (typically raw shares)
}

// PoolSupply holds the totalSupply and (optionally) scaledTotalSupply of a pool
// contract at the block the source read for. Returned once per contract per
// batch; the handler persists them as token_total_supply rows.
type PoolSupply struct {
	TotalSupply       *big.Int
	ScaledTotalSupply *big.Int // nil when not applicable / call failed
}

// FetchResult is what a PositionSource returns from one multicall batch:
// per-entry balances and — for sources that read it atomically in the same
// multicall — per-contract supply. Sources that do not read supply leave
// Supplies as an empty (non-nil) map.
type FetchResult struct {
	Balances map[EntryKey]*PositionBalance
	Supplies map[common.Address]*PoolSupply
}

// NewFetchResult returns an empty, initialized FetchResult.
func NewFetchResult() *FetchResult {
	return &FetchResult{
		Balances: make(map[EntryKey]*PositionBalance),
		Supplies: make(map[common.Address]*PoolSupply),
	}
}

// PositionSnapshot is the final output: entry + balance + trigger context.
type PositionSnapshot struct {
	Entry         *TokenEntry
	Balance       *big.Int
	ScaledBalance *big.Int

	ChainID      int64
	BlockNumber  int64
	BlockVersion int

	// Transfer that triggered the snapshot (zero values for periodic sweep)
	TxHash    string
	LogIndex  int
	TxAmount  *big.Int
	Direction Direction

	BlockTimestamp time.Time // block timestamp for hypertable partition column
}

// TokenTotalSupplySnapshot is the per-contract, per-block supply snapshot the
// service produces alongside PositionSnapshots. The handler converts these into
// entity.TokenTotalSupply rows for persistence.
type TokenTotalSupplySnapshot struct {
	ChainID           int64
	TokenAddress      common.Address
	TotalSupply       *big.Int
	ScaledTotalSupply *big.Int
	BlockNumber       int64
	BlockVersion      int
	BlockTimestamp    time.Time
	Source            string // "event" | "sweep"
}

// SnapshotBatch is the full output of one block's balance+supply read, routed
// as a single unit to handlers so both writes can land in one transaction.
type SnapshotBatch struct {
	Snapshots []*PositionSnapshot
	Supplies  []*TokenTotalSupplySnapshot
}

// PositionSource knows how to fetch on-chain balances (and optionally supplies)
// for specific token types.
type PositionSource interface {
	Name() string
	Supports(tokenType string, protocol string) bool
	FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (*FetchResult, error)
}

// AllocationHandler processes position+supply batches.
type AllocationHandler interface {
	HandleBatch(ctx context.Context, batch *SnapshotBatch) error
}
