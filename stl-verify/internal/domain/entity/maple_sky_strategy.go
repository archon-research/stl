package entity

import "fmt"

// MapleSkyStrategy represents a Sky strategy (internal Maple deployment of
// pool assets into DeFi positions) discovered via the Maple GraphQL API.
type MapleSkyStrategy struct {
	ID              int64
	ChainID         int64
	StrategyAddress []byte // 20 bytes, skyStrategy.id
	MaplePoolID     int64
	Version         int
}

// NewMapleSkyStrategy creates a new MapleSkyStrategy entity with validation.
func NewMapleSkyStrategy(chainID int64, strategyAddress []byte, maplePoolID int64, version int) (*MapleSkyStrategy, error) {
	s := &MapleSkyStrategy{
		ChainID:         chainID,
		StrategyAddress: strategyAddress,
		MaplePoolID:     maplePoolID,
		Version:         version,
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("NewMapleSkyStrategy: %w", err)
	}
	return s, nil
}

// Validate checks that all fields have valid values.
func (s *MapleSkyStrategy) Validate() error {
	if s.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", s.ChainID)
	}
	if len(s.StrategyAddress) != 20 {
		return fmt.Errorf("strategyAddress must be 20 bytes, got %d", len(s.StrategyAddress))
	}
	if s.MaplePoolID <= 0 {
		return fmt.Errorf("maplePoolID must be positive, got %d", s.MaplePoolID)
	}
	if s.Version < 0 {
		return fmt.Errorf("version must be non-negative, got %d", s.Version)
	}
	return nil
}
