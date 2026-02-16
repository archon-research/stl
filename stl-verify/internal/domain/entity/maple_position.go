package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// MaplePosition represents a user's lending position in a Maple pool at a
// point in time. LendingBalance is the USD value with 6 decimals (divide by
// 1e6 for USD).
type MaplePosition struct {
	ID             int64
	UserID         int64
	ProtocolID     int64
	PoolAddress    common.Address
	PoolName       string
	AssetSymbol    string // pool's underlying asset (e.g. "USDC", "USDT")
	AssetDecimals  int
	LendingBalance *big.Int // 6-decimal USD value
	SnapshotBlock  int64    // ethereum block that triggered the snapshot
	SnapshotTime   time.Time
}

// NewMaplePosition creates a new MaplePosition with validation.
func NewMaplePosition(
	id, userID, protocolID int64,
	poolAddress common.Address,
	poolName, assetSymbol string,
	assetDecimals int,
	lendingBalance *big.Int,
	snapshotBlock int64,
	snapshotTime time.Time,
) (*MaplePosition, error) {
	p := &MaplePosition{
		ID:             id,
		UserID:         userID,
		ProtocolID:     protocolID,
		PoolAddress:    poolAddress,
		PoolName:       poolName,
		AssetSymbol:    assetSymbol,
		AssetDecimals:  assetDecimals,
		LendingBalance: lendingBalance,
		SnapshotBlock:  snapshotBlock,
		SnapshotTime:   snapshotTime,
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *MaplePosition) validate() error {
	if p.UserID <= 0 {
		return fmt.Errorf("userID must be positive, got %d", p.UserID)
	}
	if p.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", p.ProtocolID)
	}
	if p.PoolAddress == (common.Address{}) {
		return fmt.Errorf("pool address cannot be empty")
	}
	if p.PoolName == "" {
		return fmt.Errorf("poolName must not be empty")
	}
	if p.AssetSymbol == "" {
		return fmt.Errorf("assetSymbol must not be empty")
	}
	if p.LendingBalance == nil {
		return fmt.Errorf("lendingBalance must not be nil")
	}
	if p.LendingBalance.Sign() < 0 {
		return fmt.Errorf("lendingBalance must be non-negative")
	}
	if p.SnapshotBlock <= 0 {
		return fmt.Errorf("snapshotBlock must be positive, got %d", p.SnapshotBlock)
	}
	if p.SnapshotTime.IsZero() {
		return fmt.Errorf("snapshotTime must not be zero")
	}
	return nil
}

// LendingBalanceUSD returns the lending balance as a USD float (divides by 1e6).
func (p *MaplePosition) LendingBalanceUSD() *big.Float {
	f := new(big.Float).SetInt(p.LendingBalance)
	divisor := new(big.Float).SetFloat64(1e6)
	return new(big.Float).Quo(f, divisor)
}
