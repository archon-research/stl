// Package rawsckey builds S3 keys for archived raw smart contract call batches.
//
// Key format: raw-sc-calls/chain_id={chainID}/block={partition}/{blockNumber}_{blockVersion}_{source}_{batchHash}.jsonl.zst
//
// Example: raw-sc-calls/chain_id=1/block=21500000-21500999/21500042_0_oracle-price_a3f2c1d4e5b6f7c8.jsonl.zst
//
// One object per multicall batch. All chains share one bucket; chain_id is the
// top-level partition. The key is fully derivable from
// (chainID, blockNumber, blockVersion, source, batch composition): given those
// inputs a replay can compute the exact object and fetch the stored batch
// without listing.
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

// BatchHashInput is one call's contribution to the batch hash. Target carries
// the contract address bytes (any deterministic encoding the caller uses
// consistently); CallData is the raw ABI-encoded call input.
type BatchHashInput struct {
	Target   []byte
	CallData []byte
}

// BatchHash returns the FNV-1a 64-bit hash of the batch in issue order, as a
// fixed 16-char hex string. Identical batches in the same order produce the
// same hash; reordering changes the hash.
func BatchHash(inputs []BatchHashInput) string {
	h := fnv.New64a()
	for _, in := range inputs {
		_, _ = h.Write(in.Target)   // fnv.Write never returns an error
		_, _ = h.Write(in.CallData) // fnv.Write never returns an error
	}
	return fmt.Sprintf("%016x", h.Sum64())
}

// Build constructs the full S3 key for an archived batch. batchHash should be
// obtained from BatchHash over the calls in this batch.
func Build(chainID, blockNumber int64, blockVersion int, source, batchHash string) string {
	return fmt.Sprintf(
		"%s/chain_id=%d/block=%s/%d_%d_%s_%s.jsonl.zst",
		prefix,
		chainID,
		partition.GetPartition(blockNumber),
		blockNumber,
		blockVersion,
		source,
		batchHash,
	)
}
