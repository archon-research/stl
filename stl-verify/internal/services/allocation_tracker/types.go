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

// PositionBalance is what a PositionSource returns.
type PositionBalance struct {
	Balance       *big.Int // primary tracked balance in the entry's tracked unit (token units for ERC20-like entries; underlying-asset units for pool-style entries like UniV3)
	ScaledBalance *big.Int // optional auxiliary balance (typically raw shares)
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

// PositionSource knows how to fetch on-chain balances for specific token types.
type PositionSource interface {
	Name() string
	Supports(tokenType string, protocol string) bool
	FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (map[EntryKey]*PositionBalance, error)
}

// AllocationHandler processes position snapshots.
type AllocationHandler interface {
	HandleSnapshots(ctx context.Context, snapshots []*PositionSnapshot) error
}
