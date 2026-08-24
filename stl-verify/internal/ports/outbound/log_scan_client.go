package outbound

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
)

// ErrLogRangeTooLarge marks the one GetLogs failure a caller can act on: the
// provider refused the query because the range yields too many logs (or too
// large a response), so a narrower range would succeed. Adapters wrap it around
// their provider's own wording; a caller decides by errors.Is, never by string
// matching. Every other failure — a transient 5xx, a bad address, a node
// missing the range — is an ordinary error and must not carry this.
var ErrLogRangeTooLarge = errors.New("log query range too large")

// LogFilter bounds one eth_getLogs query. FromBlock and ToBlock are inclusive
// block heights, not tags: a historical scan pins its own range, and "latest"
// would silently move under it between windows.
type LogFilter struct {
	// Address is the single emitting contract to filter on; the zero address
	// means unconstrained.
	Address   common.Address
	FromBlock int64
	ToBlock   int64
	// Topic0 is the event signature hash; the zero hash means unconstrained.
	Topic0 common.Hash
	// Topic1 is an OR-set over the first indexed argument — the shape a
	// singleton emitter needs to ask for several pools' logs in one query. An
	// empty slice leaves topic1 unconstrained.
	Topic1 []common.Hash
}

// FilteredLog is one eth_getLogs result, kept in the wire's hex-string form
// exactly as BlockHeader is. The strings are unvalidated provider output:
// consumers must reject a malformed topic or hash themselves rather than let
// common.HexToHash left-pad it into a plausible-looking wrong value.
type FilteredLog struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockHash        string   `json:"blockHash"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// LogScanClient is the execution-layer RPC slice a finality-pinned historical
// scan needs: filtered log queries plus the head and header reads that pin the
// range. It is deliberately separate from BlockchainClient, whose per-block
// payload methods a scanner never calls.
type LogScanClient interface {
	// GetLogs returns every log matching filter. Implementations retry only
	// transient failures; a range refusal comes back immediately, wrapping
	// ErrLogRangeTooLarge, because narrowing the range is the caller's decision
	// and retrying the same query would only burn the provider's budget.
	GetLogs(ctx context.Context, filter LogFilter) ([]FilteredLog, error)
	// GetCurrentBlockNumber fetches the latest block number.
	GetCurrentBlockNumber(ctx context.Context) (int64, error)
	// GetBlockHeaderByNumber fetches one block's header by height. A scan uses
	// it twice on the same height — once to pin, once to prove the pin still
	// names the same hash — so it must answer from the current canonical chain,
	// never from a cache.
	GetBlockHeaderByNumber(ctx context.Context, blockNumber int64) (*BlockHeader, error)
}
