package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetMetaMorphoEventsABI returns the ABI for MetaMorpho vault events.
func GetMetaMorphoEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "sender", "type": "address"},
				{"indexed": true, "name": "owner", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"},
				{"indexed": false, "name": "shares", "type": "uint256"}
			],
			"name": "Deposit",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "sender", "type": "address"},
				{"indexed": true, "name": "receiver", "type": "address"},
				{"indexed": true, "name": "owner", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"},
				{"indexed": false, "name": "shares", "type": "uint256"}
			],
			"name": "Withdraw",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "newTotalAssets", "type": "uint256"},
				{"indexed": false, "name": "feeShares", "type": "uint256"}
			],
			"name": "AccrueInterest",
			"type": "event"
		}
	]`)
}

// GetMetaMorphoV2AccrueInterestABI returns a separate ABI for the V2 AccrueInterest event.
// V2 has a different signature: AccrueInterest(uint256,uint256,uint256,uint256) vs V1's AccrueInterest(uint256,uint256).
// Since go-ethereum can't have two events with the same name in one ABI, this must be a separate ABI.
func GetMetaMorphoV2AccrueInterestABI() (*abi.ABI, error) {
	return ParseABI(`[{
		"anonymous": false,
		"inputs": [
			{"indexed": false, "name": "newTotalAssets", "type": "uint256"},
			{"indexed": false, "name": "interest", "type": "uint256"},
			{"indexed": false, "name": "feeShares", "type": "uint256"},
			{"indexed": false, "name": "feeAssets", "type": "uint256"}
		],
		"name": "AccrueInterest",
		"type": "event"
	}]`)
}

// GetMetaMorphoReadABI returns the ABI for MetaMorpho vault read functions.
func GetMetaMorphoReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "totalAssets",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "totalSupply",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "account", "type": "address"}],
			"name": "balanceOf",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "name",
			"outputs": [{"name": "", "type": "string"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "symbol",
			"outputs": [{"name": "", "type": "string"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "asset",
			"outputs": [{"name": "", "type": "address"}],
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
