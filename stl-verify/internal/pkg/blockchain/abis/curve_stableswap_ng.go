package abis

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

const curveStableswapNGJSON = `[
	{
		"name": "coins",
		"inputs": [{"name": "i", "type": "uint256"}],
		"outputs": [{"name": "", "type": "address"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "balances",
		"inputs": [{"name": "i", "type": "uint256"}],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "get_balances",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256[]"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "N_COINS",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "get_virtual_price",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "totalSupply",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "A",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "fee",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "price_oracle",
		"inputs": [{"name": "i", "type": "uint256"}],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`

func GetCurveStableswapNGABI() (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(curveStableswapNGJSON))
	if err != nil {
		return nil, fmt.Errorf("parse curve stableswap-ng ABI: %w", err)
	}
	return &parsed, nil
}
