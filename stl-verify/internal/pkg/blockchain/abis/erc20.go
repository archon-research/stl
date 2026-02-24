package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

func GetERC20ABI() (*abi.ABI, error) {
	return ParseABI(`[
       {
          "inputs": [],
          "name": "decimals",
          "outputs": [{"name": "", "type": "uint8"}],
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
          "name": "name",
          "outputs": [{"name": "", "type": "string"}],
          "stateMutability": "view",
          "type": "function"
       },
       {
          "inputs": [{"name": "account", "type": "address"}],
          "name": "balanceOf",
          "outputs": [{"name": "", "type": "uint256"}],
          "stateMutability": "view",
          "type": "function"
       }
    ]`)
}
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
