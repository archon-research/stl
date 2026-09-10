package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetMetaMorphoV1EventsABI returns the ABI for MetaMorpho V1 / V1.1 vault events.
//
// Both V1 and V1.1 emit the standard ERC4626 Deposit/Withdraw, ERC20 Transfer,
// and a 2-arg AccrueInterest (same topic hash on both — V1.1 only differs on
// the read surface, not on the AccrueInterest signature). Morpho VaultV2
// inherits ERC4626 and ERC20 so its Deposit/Withdraw/Transfer logs use the
// same topic hashes; its AccrueInterest is 4-field and lives in a separate
// ABI (see GetMetaMorphoV2AccrueInterestABI).
func GetMetaMorphoV1EventsABI() (*abi.ABI, error) {
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

// GetMetaMorphoV2AccrueInterestABI returns a separate ABI for the Morpho
// VaultV2 AccrueInterest event, which has 4 fields rather than V1/V1.1's 2.
//
// V2 fields: previousTotalAssets, newTotalAssets, performanceFeeShares,
// managementFeeShares. Since go-ethereum can't have two events with the same
// name in one ABI, this must live in its own ABI from V1 / V1.1.
func GetMetaMorphoV2AccrueInterestABI() (*abi.ABI, error) {
	return ParseABI(`[{
		"anonymous": false,
		"inputs": [
			{"indexed": false, "name": "previousTotalAssets", "type": "uint256"},
			{"indexed": false, "name": "newTotalAssets", "type": "uint256"},
			{"indexed": false, "name": "performanceFeeShares", "type": "uint256"},
			{"indexed": false, "name": "managementFeeShares", "type": "uint256"}
		],
		"name": "AccrueInterest",
		"type": "event"
	}]`)
}

// GetMetaMorphoReadABI returns the ABI for MetaMorpho V1 / V1.1 read functions.
//
// MORPHO() and skimRecipient() are MetaMorpho-only — VaultV2 reverts on these
// (see GetVaultV2ReadABI for VaultV2 selectors). The shared selectors
// (totalAssets, totalSupply, balanceOf, name, symbol, asset, decimals) are
// included here so a single ABI handle can decode reads on any vault flavour.
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
		},
		{
			"inputs": [],
			"name": "MORPHO",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "skimRecipient",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}

// GetVaultV2AdapterReadABI returns the ABI for the read functions on a VaultV2
// liquidity adapter (not the vault itself).
//
// A VaultV2 never touches a downstream venue directly: it holds adapter
// contracts, each wrapping one venue. The five type-discriminating selectors are
// mutually exclusive on a real adapter, which answers its own and reverts on
// every other — morpho() (0xd8fbc833) on a MorphoMarketV1AdapterV2 (wraps a
// Morpho Blue market), morphoVaultV1() (0xe4baaddf) on a MorphoVaultV1Adapter
// (wraps a nested MetaMorpho V1 vault), erc4626Vault() (0x5920b225) on an
// ERC4626MerklAdapter (wraps an external ERC-4626 vault, rewards claimable via
// Merkl), box() (0x754215a1) on a BoxAdapter (wraps a Morpho Box), and comet()
// (0xba3e9c12) on a CompoundV3Adapter (wraps a Compound V3 Comet).
// realAssets() (0x56c07573) exists on all of them and returns the adapter's
// current holdings in the vault's underlying-asset base units. Chain-verified
// against sparkUSDTbc's adapter and, for the latter three families, against
// mainnet adapters 0x7d82bc573672e5b7587368b1148e09f88ae14dbd,
// 0xb83a8af84f9123d3afd7b2e6de56c5b6e02ff19b and
// 0x099789bd9de92cac98af6abb6a62579e2efc9050.
func GetVaultV2AdapterReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "morpho",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "morphoVaultV1",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "erc4626Vault",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "box",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "comet",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "realAssets",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}

// GetVaultV2ReadABI returns the ABI for Morpho VaultV2-specific read functions.
//
// VaultV2 (deployed by 0xa1d94f746defa1928926b84fb2596c06926c0405 on mainnet)
// shares the ERC4626/ERC20 surface with MetaMorpho but reverts on MORPHO() and
// skimRecipient(). Its presence is identified by curator() and
// liquidityAdapter() returning addresses successfully.
//
// absoluteCap(bytes32) / relativeCap(bytes32) return the two current allocation
// limits for a cap id (id = keccak256(idData)). Both are declared uint128 on the
// contract (ABI-encoded as full 32-byte words, decoded into *big.Int) — the
// indexer reads them at a cap event's block hash to snapshot the full cap state.
// Chain-verified against sparkUSDTbc: absoluteCap selector 0xbc0dd374,
// relativeCap selector 0xa68bafa3.
//
// adaptersLength() / adapters(uint256) enumerate the vault's registered adapter
// set. Note the no-arg adapters() returning address[] REVERTS on the deployed
// VaultV2 (see vault_probe.go); the enumerable form (a length getter plus an
// index getter) is the working surface. Discovery-time enumeration reads these
// to seed the adapter registry for a V2 vault found mid-life, whose historical
// AddAdapter events never replay on the live stream. Chain-verified against
// sparkUSDTbc: adaptersLength() 0x5aa22bc8, adapters(uint256) 0x4ef501ac.
//
// performanceFee() / managementFee() (both uint96 WAD, unscaled) and
// performanceFeeRecipient() / managementFeeRecipient() (address) return the
// vault's full fee configuration. performanceFee is a WAD fraction of accrued
// interest (1e18 = 100%); managementFee is a WAD per-second rate. The indexer
// reads all four at a Set* fee event's block hash to snapshot the full fee
// state. Chain-verified against sparkUSDTbc: performanceFee() 0x87788782,
// managementFee() 0xa6f7f5d6, performanceFeeRecipient() 0xed27f7c9,
// managementFeeRecipient() 0x6d9a3010.
func GetVaultV2ReadABI() (*abi.ABI, error) {
	return ParseABI(`[
		{
			"inputs": [],
			"name": "curator",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "liquidityAdapter",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "adaptersLength",
			"outputs": [{"name": "", "type": "uint256"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "", "type": "uint256"}],
			"name": "adapters",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "id", "type": "bytes32"}],
			"name": "absoluteCap",
			"outputs": [{"name": "", "type": "uint128"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [{"name": "id", "type": "bytes32"}],
			"name": "relativeCap",
			"outputs": [{"name": "", "type": "uint128"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "performanceFee",
			"outputs": [{"name": "", "type": "uint96"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "managementFee",
			"outputs": [{"name": "", "type": "uint96"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "performanceFeeRecipient",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		},
		{
			"inputs": [],
			"name": "managementFeeRecipient",
			"outputs": [{"name": "", "type": "address"}],
			"stateMutability": "view",
			"type": "function"
		}
	]`)
}
