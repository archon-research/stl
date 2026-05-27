package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetSyrupVaultEventsABI returns the parsed Syrup ERC-4626 events ABI.
// Syrup vaults are standard ERC-4626 + ERC-20, so the topic hashes for
// Deposit/Withdraw/Transfer are identical to MetaMorpho — but we keep the
// definitions local for clarity and so refactors to the morpho ABI cannot
// drift this contract surface unexpectedly.
func GetSyrupVaultEventsABI() (*abi.ABI, error) {
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
		}
	]`)
}

// GetSyrupVaultViewsABI returns the parsed view-function ABI used by multicall:
// totalAssets, totalSupply, convertToAssets, balanceOf, decimals, asset, name, symbol.
func GetSyrupVaultViewsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{"inputs": [],                                    "name": "totalAssets",     "outputs": [{"name":"","type":"uint256"}], "stateMutability": "view", "type": "function"},
		{"inputs": [],                                    "name": "totalSupply",     "outputs": [{"name":"","type":"uint256"}], "stateMutability": "view", "type": "function"},
		{"inputs": [{"name":"shares","type":"uint256"}],  "name": "convertToAssets", "outputs": [{"name":"","type":"uint256"}], "stateMutability": "view", "type": "function"},
		{"inputs": [{"name":"account","type":"address"}], "name": "balanceOf",       "outputs": [{"name":"","type":"uint256"}], "stateMutability": "view", "type": "function"},
		{"inputs": [],                                    "name": "decimals",        "outputs": [{"name":"","type":"uint8"}],   "stateMutability": "view", "type": "function"},
		{"inputs": [],                                    "name": "asset",           "outputs": [{"name":"","type":"address"}], "stateMutability": "view", "type": "function"},
		{"inputs": [],                                    "name": "name",            "outputs": [{"name":"","type":"string"}],  "stateMutability": "view", "type": "function"},
		{"inputs": [],                                    "name": "symbol",          "outputs": [{"name":"","type":"string"}],  "stateMutability": "view", "type": "function"}
	]`)
}
