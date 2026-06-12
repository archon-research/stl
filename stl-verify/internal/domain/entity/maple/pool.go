package maple

import "fmt"

// Pool represents a Maple Finance PoolV2 lending pool discovered via the
// Maple GraphQL API. The pool's underlying asset is always a mainnet ERC-20,
// referenced by its token table id (resolved by the service before
// construction). Loan collateral assets (BTC, SOL) stay raw on
// LoanCollateral instead: they have no Ethereum token address.
type Pool struct {
	ID           int64
	ChainID      int64
	ProtocolID   int64
	Address      []byte // 20 bytes, poolV2.id
	Name         string
	AssetTokenID int64 // token.id of poolV2.asset
	IsSyrup      bool  // poolV2.syrupRouter != null
}

// NewPool creates a new Pool entity with validation.
func NewPool(chainID, protocolID int64, address []byte, name string, assetTokenID int64, isSyrup bool) (*Pool, error) {
	p := &Pool{
		ChainID:      chainID,
		ProtocolID:   protocolID,
		Address:      address,
		Name:         name,
		AssetTokenID: assetTokenID,
		IsSyrup:      isSyrup,
	}
	if err := p.Validate(); err != nil {
		return nil, fmt.Errorf("NewPool: %w", err)
	}
	return p, nil
}

// Validate checks that all fields have valid values.
func (p *Pool) Validate() error {
	if p.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", p.ChainID)
	}
	if p.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", p.ProtocolID)
	}
	if len(p.Address) != 20 {
		return fmt.Errorf("address must be 20 bytes, got %d", len(p.Address))
	}
	if p.AssetTokenID <= 0 {
		return fmt.Errorf("assetTokenID must be positive, got %d", p.AssetTokenID)
	}
	return nil
}
