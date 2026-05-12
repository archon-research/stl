package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetVaultV2EventsABI returns the full ABI for events emitted by Morpho VaultV2
// contracts (e.g. sparkUSDTbc at 0xc7CDcFDEfC64631ED6799C95e3b110cd42F2bD22).
//
// The list is sourced from morpho-org/vault-v2 src/libraries/EventsLib.sol
// and was reconciled against on-chain log emissions on 2026-05-06. Earlier
// drafts of `docs/morpho_spec.md` listed adapter management as
// AdapterAdded/AdapterRemoved; the deployed contract uses AddAdapter/
// RemoveAdapter (imperative form), and several Set*/timelock events were
// missing from the spec entirely. Both the spec and this ABI now match chain.
//
// Excluded (already defined elsewhere or out of scope):
//   - Deposit, Withdraw, Transfer, AccrueInterest (4-field): already in
//     metamorpho_abi.go (V1/V1.1 + V2 share Deposit/Withdraw/Transfer topic
//     hashes; V2 AccrueInterest is in GetMetaMorphoV2AccrueInterestABI).
//
// All other events are registered here regardless of whether the indexer
// currently extracts their fields — registration ensures `IsMetaMorphoEvent`
// recognises them, so they reach the protocol_event audit log.
func GetVaultV2EventsABI() (*abi.ABI, error) {
	return ParseABI(`[
		{"anonymous": false, "type": "event", "name": "Approval", "inputs": [
			{"name": "owner", "type": "address", "indexed": true},
			{"name": "spender", "type": "address", "indexed": true},
			{"name": "shares", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "AllowanceUpdatedByTransferFrom", "inputs": [
			{"name": "owner", "type": "address", "indexed": true},
			{"name": "spender", "type": "address", "indexed": true},
			{"name": "shares", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "Permit", "inputs": [
			{"name": "owner", "type": "address", "indexed": true},
			{"name": "spender", "type": "address", "indexed": true},
			{"name": "shares", "type": "uint256", "indexed": false},
			{"name": "nonce", "type": "uint256", "indexed": false},
			{"name": "deadline", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "Constructor", "inputs": [
			{"name": "owner", "type": "address", "indexed": true},
			{"name": "asset", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "Allocate", "inputs": [
			{"name": "sender", "type": "address", "indexed": true},
			{"name": "adapter", "type": "address", "indexed": true},
			{"name": "assets", "type": "uint256", "indexed": false},
			{"name": "ids", "type": "bytes32[]", "indexed": false},
			{"name": "change", "type": "int256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "Deallocate", "inputs": [
			{"name": "sender", "type": "address", "indexed": true},
			{"name": "adapter", "type": "address", "indexed": true},
			{"name": "assets", "type": "uint256", "indexed": false},
			{"name": "ids", "type": "bytes32[]", "indexed": false},
			{"name": "change", "type": "int256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "ForceDeallocate", "inputs": [
			{"name": "sender", "type": "address", "indexed": true},
			{"name": "adapter", "type": "address", "indexed": false},
			{"name": "assets", "type": "uint256", "indexed": false},
			{"name": "onBehalf", "type": "address", "indexed": true},
			{"name": "ids", "type": "bytes32[]", "indexed": false},
			{"name": "penaltyAssets", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "Submit", "inputs": [
			{"name": "selector", "type": "bytes4", "indexed": true},
			{"name": "data", "type": "bytes", "indexed": false},
			{"name": "executableAt", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "Accept", "inputs": [
			{"name": "selector", "type": "bytes4", "indexed": true},
			{"name": "data", "type": "bytes", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "Revoke", "inputs": [
			{"name": "sender", "type": "address", "indexed": true},
			{"name": "selector", "type": "bytes4", "indexed": true},
			{"name": "data", "type": "bytes", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "IncreaseTimelock", "inputs": [
			{"name": "selector", "type": "bytes4", "indexed": true},
			{"name": "newDuration", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "DecreaseTimelock", "inputs": [
			{"name": "selector", "type": "bytes4", "indexed": true},
			{"name": "newDuration", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "Abdicate", "inputs": [
			{"name": "selector", "type": "bytes4", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetOwner", "inputs": [
			{"name": "newOwner", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetCurator", "inputs": [
			{"name": "newCurator", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetIsSentinel", "inputs": [
			{"name": "account", "type": "address", "indexed": true},
			{"name": "newIsSentinel", "type": "bool", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "SetIsAllocator", "inputs": [
			{"name": "account", "type": "address", "indexed": true},
			{"name": "newIsAllocator", "type": "bool", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "SetName", "inputs": [
			{"name": "newName", "type": "string", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "SetSymbol", "inputs": [
			{"name": "newSymbol", "type": "string", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "SetReceiveSharesGate", "inputs": [
			{"name": "newReceiveSharesGate", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetSendSharesGate", "inputs": [
			{"name": "newSendSharesGate", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetReceiveAssetsGate", "inputs": [
			{"name": "newReceiveAssetsGate", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetSendAssetsGate", "inputs": [
			{"name": "newSendAssetsGate", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetAdapterRegistry", "inputs": [
			{"name": "newAdapterRegistry", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetLiquidityAdapterAndData", "inputs": [
			{"name": "sender", "type": "address", "indexed": true},
			{"name": "newLiquidityAdapter", "type": "address", "indexed": true},
			{"name": "newLiquidityData", "type": "bytes", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "AddAdapter", "inputs": [
			{"name": "account", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "RemoveAdapter", "inputs": [
			{"name": "account", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetPerformanceFee", "inputs": [
			{"name": "newPerformanceFee", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "SetManagementFee", "inputs": [
			{"name": "newManagementFee", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "SetPerformanceFeeRecipient", "inputs": [
			{"name": "newPerformanceFeeRecipient", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetManagementFeeRecipient", "inputs": [
			{"name": "newManagementFeeRecipient", "type": "address", "indexed": true}
		]},
		{"anonymous": false, "type": "event", "name": "SetMaxRate", "inputs": [
			{"name": "newMaxRate", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "SetForceDeallocatePenalty", "inputs": [
			{"name": "adapter", "type": "address", "indexed": true},
			{"name": "forceDeallocatePenalty", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "IncreaseAbsoluteCap", "inputs": [
			{"name": "id", "type": "bytes32", "indexed": true},
			{"name": "idData", "type": "bytes", "indexed": false},
			{"name": "newAbsoluteCap", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "DecreaseAbsoluteCap", "inputs": [
			{"name": "sender", "type": "address", "indexed": true},
			{"name": "id", "type": "bytes32", "indexed": true},
			{"name": "idData", "type": "bytes", "indexed": false},
			{"name": "newAbsoluteCap", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "IncreaseRelativeCap", "inputs": [
			{"name": "id", "type": "bytes32", "indexed": true},
			{"name": "idData", "type": "bytes", "indexed": false},
			{"name": "newRelativeCap", "type": "uint256", "indexed": false}
		]},
		{"anonymous": false, "type": "event", "name": "DecreaseRelativeCap", "inputs": [
			{"name": "sender", "type": "address", "indexed": true},
			{"name": "id", "type": "bytes32", "indexed": true},
			{"name": "idData", "type": "bytes", "indexed": false},
			{"name": "newRelativeCap", "type": "uint256", "indexed": false}
		]}
	]`)
}
