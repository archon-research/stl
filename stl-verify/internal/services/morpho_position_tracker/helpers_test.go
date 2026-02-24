package morpho_position_tracker

import "math/big"

func bigFromStr(s string) *big.Int {
	n := new(big.Int)
	n.SetString(s, 10)
	return n
}
