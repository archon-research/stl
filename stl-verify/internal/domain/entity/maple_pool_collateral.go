package entity

import (
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// MaplePoolCollateral represents a single collateral asset backing a Maple
// pool at a point in time. AssetValueUSD uses 6 decimals, as does TVL.
type MaplePoolCollateral struct {
	ID            int64
	PoolAddress   common.Address
	PoolName      string
	Asset         string   // collateral symbol, e.g. "BTC", "XRP"
	AssetDecimals int      // native decimals of the collateral asset
	AssetValueUSD *big.Int // 6-decimal USD value of this collateral
	PoolTVL       *big.Int // 6-decimal USD total value locked in pool
	SnapshotBlock int64
	SnapshotTime  time.Time
}

// NewMaplePoolCollateral creates a new MaplePoolCollateral with validation.
func NewMaplePoolCollateral(
	id int64,
	poolAddress common.Address,
	poolName, asset string,
	assetDecimals int,
	assetValueUSD, poolTVL *big.Int,
	snapshotBlock int64,
	snapshotTime time.Time,
) (*MaplePoolCollateral, error) {
	c := &MaplePoolCollateral{
		ID:            id,
		PoolAddress:   poolAddress,
		PoolName:      poolName,
		Asset:         asset,
		AssetDecimals: assetDecimals,
		AssetValueUSD: assetValueUSD,
		PoolTVL:       poolTVL,
		SnapshotBlock: snapshotBlock,
		SnapshotTime:  snapshotTime,
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *MaplePoolCollateral) validate() error {
	if c.PoolAddress == (common.Address{}) {
		return fmt.Errorf("pool address cannot be empty")
	}
	if c.PoolName == "" {
		return fmt.Errorf("poolName must not be empty")
	}
	if c.Asset == "" {
		return fmt.Errorf("asset must not be empty")
	}
	if c.AssetValueUSD == nil {
		return fmt.Errorf("assetValueUSD must not be nil")
	}
	if c.AssetValueUSD.Sign() < 0 {
		return fmt.Errorf("assetValueUSD must be non-negative")
	}
	if c.PoolTVL == nil {
		return fmt.Errorf("poolTVL must not be nil")
	}
	if c.PoolTVL.Sign() <= 0 {
		return fmt.Errorf("poolTVL must be positive")
	}
	if c.SnapshotBlock <= 0 {
		return fmt.Errorf("snapshotBlock must be positive, got %d", c.SnapshotBlock)
	}
	if c.SnapshotTime.IsZero() {
		return fmt.Errorf("snapshotTime must not be zero")
	}
	return nil
}

// AssetPercent returns the percentage of the pool TVL that this collateral
// represents as a big.Float (e.g. 0.2462 for 24.62%).
func (c *MaplePoolCollateral) AssetPercent() *big.Float {
	num := new(big.Float).SetInt(c.AssetValueUSD)
	den := new(big.Float).SetInt(c.PoolTVL)
	return new(big.Float).Quo(num, den)
}

// AssetValueFloat returns the USD value as a float (divides by 1e6).
func (c *MaplePoolCollateral) AssetValueFloat() *big.Float {
	f := new(big.Float).SetInt(c.AssetValueUSD)
	divisor := new(big.Float).SetFloat64(1e6)
	return new(big.Float).Quo(f, divisor)
}
