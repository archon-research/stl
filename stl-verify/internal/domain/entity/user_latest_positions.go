package entity

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// AssetAmount represents a token position amount for a single asset.
type AssetAmount struct {
	TokenAddress common.Address
	Symbol       string
	Amount       *big.Int
}

// UserLatestPositions holds the latest debt and collateral positions for a single user.
type UserLatestPositions struct {
	UserAddress string
	Debt        []AssetAmount
	Collateral  []AssetAmount
}
