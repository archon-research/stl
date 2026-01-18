// Package entity contains the core domain entities for the Sentinel risk data layer.
// These entities represent the fundamental business objects and have no external dependencies.
package entity

// Chain represents a blockchain network.
type Chain struct {
	ChainID int
	Name    string
}

// NewChain creates a new Chain entity with validation.
func NewChain(chainID int, name string) *Chain {
	return &Chain{
		ChainID: chainID,
		Name:    name,
	}
}

// ChainNameToID maps chain names to their chain IDs.
var ChainNameToID = map[string]int{
	"mainnet": 1,
}

// ChainIDToName maps chain IDs to their names.
var ChainIDToName = map[int]string{
	1: "mainnet",
}
