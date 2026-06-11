package maple

import "fmt"

// SkyStrategy represents a Sky strategy (internal Maple deployment of
// pool assets into DeFi positions) discovered via the Maple GraphQL API.
type SkyStrategy struct {
	ID              int64
	ChainID         int64
	StrategyAddress []byte // 20 bytes, skyStrategy.id
	PoolID          int64
	Version         int
}

// NewSkyStrategy creates a new SkyStrategy entity with validation.
func NewSkyStrategy(chainID int64, strategyAddress []byte, maplePoolID int64, version int) (*SkyStrategy, error) {
	s := &SkyStrategy{
		ChainID:         chainID,
		StrategyAddress: strategyAddress,
		PoolID:          maplePoolID,
		Version:         version,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewSkyStrategy: %w", err)
	}
	return s, nil
}

// Validate checks that all fields have valid values.
func (s *SkyStrategy) Validate() error {
	if s.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", s.ChainID)
	}
	if len(s.StrategyAddress) != 20 {
		return fmt.Errorf("strategyAddress must be 20 bytes, got %d", len(s.StrategyAddress))
	}
	if s.PoolID <= 0 {
		return fmt.Errorf("maplePoolID must be positive, got %d", s.PoolID)
	}
	if s.Version < 0 {
		return fmt.Errorf("version must be non-negative, got %d", s.Version)
	}
	return nil
}
