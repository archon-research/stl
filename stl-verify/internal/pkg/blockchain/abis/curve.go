package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

func GetCurvePoolABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "account", "type": "address"}],
			"name": "balanceOf",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "i", "type": "uint256"}],
			"name": "coins",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "_token_amount", "type": "uint256"}, {"name": "i", "type": "int128"}],
			"name": "calc_withdraw_one_coin",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
