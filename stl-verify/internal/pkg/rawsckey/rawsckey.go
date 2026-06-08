// Package rawsckey builds S3 keys for archived raw smart contract calls.
//
// Key format: raw-sc-calls/block={partition}/bv={blockVersion}/{blockNumber}_{source}_{selector}_{inputHash}.jsonl.zst
//
// Example: raw-sc-calls/block=21500000-21500999/bv=0/21500042_oracle-price_0xfeaf968c_a3f2c1d4e5b6f7c8.jsonl.zst
//
// The key is fully derivable from (blockNumber, blockVersion, callData): given
// those three inputs a replay can compute the exact object and fetch the stored
// response without listing.
package rawsckey

import (
	"encoding/hex"
	"fmt"
	"hash/fnv"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
)

const prefix = "raw-sc-calls"

// Selector returns the 0x-prefixed hex of the 4-byte function selector
// (callData[:4]). Returns "0x" when callData is shorter than 4 bytes.
func Selector(callData []byte) string {
	if len(callData) < 4 {
		return "0x"
	}
	return "0x" + hex.EncodeToString(callData[:4])
}

// HashInput returns the FNV-1a 64-bit hash of callData as a fixed 16-char hex
// string. Deterministic across runs and platforms.
func HashInput(callData []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(callData) // fnv.Write never returns an error
	return fmt.Sprintf("%016x", h.Sum64())
}

// Build constructs the full S3 key for a single archived call.
func Build(blockNumber int64, blockVersion int, source string, callData []byte) string {
	return fmt.Sprintf(
		"%s/block=%s/bv=%d/%d_%s_%s_%s.jsonl.zst",
		prefix,
		partition.GetPartition(blockNumber),
		blockVersion,
		blockNumber,
		source,
		Selector(callData),
		HashInput(callData),
	)
}
