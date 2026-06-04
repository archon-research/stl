// Package rawsckey builds S3 keys for raw smart-contract call archives
// (VEC-81).
//
// Key format:
//
//	raw-sc-calls/chain_id={chainID}/block={partition}/bv={blockVersion}/build_id={buildID}/{source}_{method}_{timestamp}.jsonl.zst
//
// Example:
//
//	raw-sc-calls/chain_id=1/block=21500000-21500999/bv=0/build_id=42/oracle-price_latestRoundData_20260604T103045Z.jsonl.zst
//
// NOTE: The VEC-81 ticket specifies a `pv=` (processing_version) segment.
// We use `build_id=` instead, because `processing_version` is assigned per-row
// by a Postgres BEFORE INSERT trigger (ADR-0002 / VEC-80) and is therefore not
// known at S3 write time without one DB query per call. `build_id` is the
// upstream cause of `processing_version` and is uniquely 1:1 with it for any
// given (natural_key, block, bv) tuple, so audit joins remain trivial.
// This divergence from the ticket is pending team confirmation.
package rawsckey

import (
	"fmt"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
)

// Prefix is the top-level prefix used by every raw SC-call archive object.
const Prefix = "raw-sc-calls"

// Extension is the suffix every archive object uses: JSONL payload compressed
// with zstd. The extension carries the format/compression contract since the
// AWS Content-Encoding header set on PUT is implementation-dependent.
const Extension = ".jsonl.zst"

// MixedMethod is used in the filename when a single batch contains calls to
// more than one distinct method. The exact per-call method is still recorded
// on every JSONL line.
const MixedMethod = "mixed"

// TimestampFormat is the compact RFC3339-derived UTC timestamp used in
// filenames. Compact form keeps keys S3-friendly (no colons).
const TimestampFormat = "20060102T150405Z"

// Build constructs the full S3 key for an archived batch of contract calls.
//
// `source` should be the lowercase indexer name with the `-indexer` suffix
// stripped (e.g. "oracle-price", "sparklend", "morpho"). `method` is the ABI
// method name when the batch is homogeneous, or MixedMethod when it is not.
// `ts` should be wall-clock UTC at archive time.
func Build(chainID int64, blockNumber int64, blockVersion int, buildID int, source, method string, ts time.Time) string {
	return fmt.Sprintf(
		"%s/chain_id=%d/block=%s/bv=%d/build_id=%d/%s_%s_%s%s",
		Prefix,
		chainID,
		partition.GetPartition(blockNumber),
		blockVersion,
		buildID,
		source,
		method,
		ts.UTC().Format(TimestampFormat),
		Extension,
	)
}
