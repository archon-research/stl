package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetMorphoBlueEventsABI returns the ABI for Morpho Blue events.
func GetMorphoBlueEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "marketParams", "type": "tuple", "components": [
					{"name": "loanToken", "type": "address"},
					{"name": "collateralToken", "type": "address"},
					{"name": "oracle", "type": "address"},
					{"name": "irm", "type": "address"},
					{"name": "lltv", "type": "uint256"}
				]}
			],
			"name": "CreateMarket",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "caller", "type": "address"},
				{"indexed": true, "name": "onBehalf", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"},
				{"indexed": false, "name": "shares", "type": "uint256"}
			],
			"name": "Supply",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "caller", "type": "address"},
				{"indexed": true, "name": "onBehalf", "type": "address"},
				{"indexed": true, "name": "receiver", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"},
				{"indexed": false, "name": "shares", "type": "uint256"}
			],
			"name": "Withdraw",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "caller", "type": "address"},
				{"indexed": true, "name": "onBehalf", "type": "address"},
				{"indexed": true, "name": "receiver", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"},
				{"indexed": false, "name": "shares", "type": "uint256"}
			],
			"name": "Borrow",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "caller", "type": "address"},
				{"indexed": true, "name": "onBehalf", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"},
				{"indexed": false, "name": "shares", "type": "uint256"}
			],
			"name": "Repay",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "caller", "type": "address"},
				{"indexed": true, "name": "onBehalf", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"}
			],
			"name": "SupplyCollateral",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "caller", "type": "address"},
				{"indexed": true, "name": "onBehalf", "type": "address"},
				{"indexed": true, "name": "receiver", "type": "address"},
				{"indexed": false, "name": "assets", "type": "uint256"}
			],
			"name": "WithdrawCollateral",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "caller", "type": "address"},
				{"indexed": true, "name": "borrower", "type": "address"},
				{"indexed": false, "name": "repaidAssets", "type": "uint256"},
				{"indexed": false, "name": "repaidShares", "type": "uint256"},
				{"indexed": false, "name": "seizedAssets", "type": "uint256"},
				{"indexed": false, "name": "badDebtAssets", "type": "uint256"},
				{"indexed": false, "name": "badDebtShares", "type": "uint256"}
			],
			"name": "Liquidate",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "prevBorrowRate", "type": "uint256"},
				{"indexed": false, "name": "interest", "type": "uint256"},
				{"indexed": false, "name": "feeShares", "type": "uint256"}
			],
			"name": "AccrueInterest",
			"type": "event"
		},
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "id", "type": "bytes32"},
				{"indexed": false, "name": "newFee", "type": "uint256"}
			],
			"name": "SetFee",
			"type": "event"
		}
	]`)
}

// GetMorphoBlueReadABI returns the ABI for Morpho Blue read functions (market, position, idToMarketParams).
func GetMorphoBlueReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [{"name": "id", "type": "bytes32"}],
			"name": "market",
			"outputs": [
				{"name": "totalSupplyAssets", "type": "uint128"},
				{"name": "totalSupplyShares", "type": "uint128"},
				{"name": "totalBorrowAssets", "type": "uint128"},
				{"name": "totalBorrowShares", "type": "uint128"},
				{"name": "lastUpdate", "type": "uint128"},
				{"name": "fee", "type": "uint128"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [
				{"name": "id", "type": "bytes32"},
				{"name": "user", "type": "address"}
			],
			"name": "position",
			"outputs": [
				{"name": "supplyShares", "type": "uint256"},
				{"name": "borrowShares", "type": "uint128"},
				{"name": "collateral", "type": "uint128"}
			],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "id", "type": "bytes32"}],
			"name": "idToMarketParams",
			"outputs": [
				{"name": "loanToken", "type": "address"},
				{"name": "collateralToken", "type": "address"},
				{"name": "oracle", "type": "address"},
				{"name": "irm", "type": "address"},
				{"name": "lltv", "type": "uint256"}
			],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
