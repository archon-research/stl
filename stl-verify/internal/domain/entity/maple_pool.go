package entity

import "fmt"

// MaplePool represents a Maple Finance PoolV2 lending pool discovered via the
// Maple GraphQL API. Asset details are stored raw (no token FK): pools span
// assets we do not seed, and Maple collateral assets (BTC, SOL) have no
// Ethereum token address at all.
type MaplePool struct {
	ID            int64
	ChainID       int64
	ProtocolID    int64
	Address       []byte // 20 bytes, poolV2.id
	Name          string
	AssetAddress  []byte // 20 bytes, poolV2.asset.id
	AssetSymbol   string
	AssetDecimals int16
	IsSyrup       bool // poolV2.syrupRouter != null
}

// NewMaplePool creates a new MaplePool entity with validation.
func NewMaplePool(chainID, protocolID int64, address []byte, name string, assetAddress []byte, assetSymbol string, assetDecimals int16, isSyrup bool) (*MaplePool, error) {
	p := &MaplePool{
		ChainID:       chainID,
		ProtocolID:    protocolID,
		Address:       address,
		Name:          name,
		AssetAddress:  assetAddress,
		AssetSymbol:   assetSymbol,
		AssetDecimals: assetDecimals,
		IsSyrup:       isSyrup,
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("NewMaplePool: %w", err)
	}
	return p, nil
}

// Validate checks that all fields have valid values.
func (p *MaplePool) Validate() error {
	if p.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", p.ChainID)
	}
	if p.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", p.ProtocolID)
	}
	if len(p.Address) != 20 {
		return fmt.Errorf("address must be 20 bytes, got %d", len(p.Address))
	}
	if len(p.AssetAddress) != 20 {
		return fmt.Errorf("assetAddress must be 20 bytes, got %d", len(p.AssetAddress))
	}
	if p.AssetSymbol == "" {
		return fmt.Errorf("assetSymbol must not be empty")
	}
	if p.AssetDecimals < 0 {
		return fmt.Errorf("assetDecimals must be non-negative, got %d", p.AssetDecimals)
	}
	return nil
}
