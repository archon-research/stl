package abis

import (
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

func ParseABI(abiJSON string) (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
