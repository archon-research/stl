package allocation_tracker

import "testing"

// TestKnownCreatedAtBlocks_KeysMatchContractEntries is the SF8 guardrail. Each entry in
// knownCreatedAtBlocks must correspond to a real (chain, contract) in the committed
// axis-synome contract. A stale key (e.g. an address removed by a contract regeneration)
// contributes nothing and the position silently downgrades to the estimated observation
// block; and since the token upsert is LEAST(existing, new), a too-low value never
// self-corrects. So also sanity-check the block numbers are positive.
func TestKnownCreatedAtBlocks_KeysMatchContractEntries(t *testing.T) {
	entries := defaultEntries(t)

	valid := make(map[createdAtBlockKey]bool, len(entries))
	for _, e := range entries {
		valid[createdAtBlockKey{Chain: e.Chain, Contract: e.ContractAddress}] = true
	}

	for key, block := range knownCreatedAtBlocks {
		if !valid[key] {
			t.Errorf("knownCreatedAtBlocks has a stale key with no matching contract entry: chain=%s contract=%s (block %d) — remove it or correct the chain/address",
				key.Chain, key.Contract.Hex(), block)
		}
		if block <= 0 {
			t.Errorf("knownCreatedAtBlocks[chain=%s contract=%s] = %d, want a positive block number",
				key.Chain, key.Contract.Hex(), block)
		}
	}
}
