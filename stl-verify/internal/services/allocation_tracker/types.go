package allocation_tracker

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Direction indicates whether tokens moved into or out of the ALM proxy.
type Direction string

const (
	DirectionIn  Direction = "in"
	DirectionOut Direction = "out"
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
	Balance       *big.Int // actual value (underlying for erc4626, rebased for atoken)
	ScaledBalance *big.Int // raw shares (nil if not applicable)
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
