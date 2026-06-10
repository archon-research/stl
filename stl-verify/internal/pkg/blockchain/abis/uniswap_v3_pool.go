package abis

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

const uniswapV3PoolJSON = `[
	{
		"name": "slot0",
		"inputs": [],
		"outputs": [
			{"name": "sqrtPriceX96", "type": "uint160"},
			{"name": "tick", "type": "int24"},
			{"name": "observationIndex", "type": "uint16"},
			{"name": "observationCardinality", "type": "uint16"},
			{"name": "observationCardinalityNext", "type": "uint16"},
			{"name": "feeProtocol", "type": "uint8"},
			{"name": "unlocked", "type": "bool"}
		],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "liquidity",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint128"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "fee",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint24"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "token0",
		"inputs": [],
		"outputs": [{"name": "", "type": "address"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "token1",
		"inputs": [],
		"outputs": [{"name": "", "type": "address"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "observe",
		"inputs": [{"name": "secondsAgos", "type": "uint32[]"}],
		"outputs": [
			{"name": "tickCumulatives", "type": "int56[]"},
			{"name": "secondsPerLiquidityCumulativeX128s", "type": "uint160[]"}
		],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "feeGrowthGlobal0X128",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	},
	{
		"name": "feeGrowthGlobal1X128",
		"inputs": [],
		"outputs": [{"name": "", "type": "uint256"}],
		"stateMutability": "view",
		"type": "function"
	}
]`

func GetUniswapV3PoolABI() (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(uniswapV3PoolJSON))
	if err != nil {
		return nil, fmt.Errorf("parse uniswap v3 pool ABI: %w", err)
	}
	return &parsed, nil
}
