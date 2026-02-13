package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetAggregatorV3ABI returns the ABI for the Chainlink AggregatorV3Interface.
// Used by Chainlink, Chronicle, and Redstone price feeds.
func GetAggregatorV3ABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "latestRoundData",
			"outputs": [
				{"name": "roundId", "type": "uint80"},
				{"name": "answer", "type": "int256"},
				{"name": "startedAt", "type": "uint256"},
				{"name": "updatedAt", "type": "uint256"},
				{"name": "answeredInRound", "type": "uint80"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "latestAnswer",
			"outputs": [{"name": "", "type": "int256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "decimals",
			"outputs": [{"name": "", "type": "uint8"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
