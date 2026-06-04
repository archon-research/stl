package outbound

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// CallArchiver writes one batch of raw smart-contract call request/response
// pairs to an immutable audit store (VEC-81).
//
// Implementations MUST be safe for concurrent use. They MUST be fail-open: a
// failure to archive must NOT break the indexer's hot path — the indexer's
// call already succeeded by the time ArchiveBatch is invoked. Implementations
// are expected to log + bump a metric on failure and return nil from the live
// caller's perspective whenever possible. The error return is reserved for
// programmer mistakes (e.g. nil batch).
type CallArchiver interface {
	// ArchiveBatch records the raw inputs and outputs of every call in a
	// single Multicaller.Execute invocation. The implementation owns
	// serialisation, compression, naming, and durable storage.
	ArchiveBatch(ctx context.Context, batch CallBatch) error
}

// CallBatch is the unit of archival: the raw calls and responses from one
// Multicaller.Execute call against one block. All records share the meta
// fields below; per-call fields live on CallRecord.
type CallBatch struct {
	ChainID      int64
	BlockNumber  int64
	BlockVersion int

	// BuildID is the build_registry.id resolved at service startup
	// (see buildregistry.Registry). Stored as int here to avoid an
	// import cycle on the outbound ports package.
	BuildID int

	// Source identifies the indexer that made the calls — lowercase,
	// `-indexer` suffix stripped (e.g. "oracle-price", "sparklend").
	Source string

	// Multicaller is the address of the Multicall3 contract used, or the
	// zero address when calls were sent directly via JSON-RPC batching
	// (DirectCaller).
	Multicaller common.Address

	// Timestamp is the wall-clock UTC time the Multicaller.Execute call
	// returned. Used in S3 filenames to disambiguate retries / replays.
	Timestamp time.Time

	Records []CallRecord
}

// CallRecord is a single eth_call request/response pair captured during a
// batch. Bytes are stored raw; the storage adapter encodes them (hex / base64)
// when serialising.
type CallRecord struct {
	ContractAddress common.Address

	// Method is the human-readable ABI method name when known (e.g.
	// "latestRoundData"). Optional — empty when no decoder was supplied.
	Method string

	// CallData is the raw eth_call input bytes (selector + ABI-encoded args).
	CallData []byte

	// Response is the raw bytes the chain returned for this call. Empty
	// when Success is false.
	Response []byte

	// Success mirrors outbound.Result.Success — false means the sub-call
	// reverted (Multicall3 with AllowFailure) or otherwise produced no data.
	Success bool

	// Decoded is an optional structured decoding of Response. Populated only
	// when the indexer supplies a Decoder; otherwise nil. The storage adapter
	// is responsible for JSON-encoding whatever the indexer hands back.
	Decoded any
}
