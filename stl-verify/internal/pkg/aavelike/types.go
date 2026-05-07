package aavelike

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type CollateralData struct {
	Asset             common.Address
	Decimals          int
	Symbol            string
	Name              string
	ActualBalance     *big.Int
	CollateralEnabled bool
}

// DebtData holds the current outstanding debt for a single asset as returned by
// getUserReserveData. CurrentDebt is CurrentVariableDebt — the full balance, not a delta.
type DebtData struct {
	Asset       common.Address
	Decimals    int
	Symbol      string
	Name        string
	CurrentDebt *big.Int // CurrentVariableDebt from getUserReserveData
}
