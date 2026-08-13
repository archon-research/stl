package abis

import "github.com/ethereum/go-ethereum/accounts/abi"

// GetFluidVaultEventsABI returns the Fluid event surface the indexer matches on
// by topic0: the VaultFactory's VaultDeployed (new-vault discovery) and a
// vault's own LogOperate / LogLiquidate (position-change triggers). Only the
// signatures matter — the indexer reads end-of-block state from the resolver
// rather than decoding event payloads, so the inputs exist solely to derive the
// correct topic0 hashes (topic0 = keccak of the canonical signature and is
// independent of the indexed flags, which are reproduced here to match source).
//
// Signatures verified verbatim against Instadapp/fluid-contracts-public:
//   - VaultDeployed(address indexed vault, uint256 indexed vaultId) — factory/main.sol
//   - LogOperate(address user_, uint256 nftId_, int256 colAmt_, int256 debtAmt_, address to_) — vaultT1 coreModule/events.sol (no indexed params)
//   - LogLiquidate(address liquidator_, uint256 colAmt_, uint256 debtAmt_, address to_) — same (no indexed params)
func GetFluidVaultEventsABI() (*abi.ABI, error) {
	return ParseABI(`[
       {
          "anonymous": false,
          "name": "VaultDeployed",
          "type": "event",
          "inputs": [
             {"name": "vault", "type": "address", "indexed": true},
             {"name": "vaultId", "type": "uint256", "indexed": true}
          ]
       },
       {
          "anonymous": false,
          "name": "LogOperate",
          "type": "event",
          "inputs": [
             {"name": "user", "type": "address", "indexed": false},
             {"name": "nftId", "type": "uint256", "indexed": false},
             {"name": "colAmt", "type": "int256", "indexed": false},
             {"name": "debtAmt", "type": "int256", "indexed": false},
             {"name": "to", "type": "address", "indexed": false}
          ]
       },
       {
          "anonymous": false,
          "name": "LogLiquidate",
          "type": "event",
          "inputs": [
             {"name": "liquidator", "type": "address", "indexed": false},
             {"name": "colAmt", "type": "uint256", "indexed": false},
             {"name": "debtAmt", "type": "uint256", "indexed": false},
             {"name": "to", "type": "address", "indexed": false}
          ]
       }
    ]`)
}
