// Package entity contains the core domain entities for the Sentinel risk data layer.
// These entities represent the fundamental business objects and have no external dependencies.
package entity

import "fmt"

// Chain represents a blockchain network.
type Chain struct {
	ChainID int
	Name    string
}

// NewChain creates a new Chain entity with validation.
func NewChain(chainID int, name string) (*Chain, error) {
	c := &Chain{
		ChainID: chainID,
		Name:    name,
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// validate checks that all fields have valid values.
func (c *Chain) validate() error {
	if c.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", c.ChainID)
	}
	if c.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	return nil
}

// ChainNameToID maps chain names to their chain IDs.
var ChainNameToID = map[string]int{
	"mainnet": 1,
	"gnosis":  100,
}

// ChainIDToName maps chain IDs to their names.
var ChainIDToName = map[int]string{
	1:   "mainnet",
	100: "gnosis",
}
