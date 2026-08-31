package outbound

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"
)

// ErrLogRangeTooLarge marks the one GetLogs failure a caller can act on: the
// provider refused the range, so a narrower one would succeed. Adapters wrap it
// around their own wording; a caller decides by errors.Is, never by matching it.
var ErrLogRangeTooLarge = errors.New("log query range too large")

// LogFilter bounds one eth_getLogs query. FromBlock and ToBlock are inclusive;
// a zero Address or Topic0 and an empty Topic1 leave that position unconstrained.
type LogFilter struct {
	Address   common.Address
	FromBlock int64
	ToBlock   int64
	Topic0    common.Hash
	Topic1    []common.Hash
}

// FilteredLog reaches a consumer unvalidated, exactly as the wire had it.
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

type LogScanClient interface {
	// A range refusal must come back immediately wrapping ErrLogRangeTooLarge,
	// never retried: only the caller's windowing policy can fix it.
	GetLogs(ctx context.Context, filter LogFilter) ([]FilteredLog, error)
	GetCurrentBlockNumber(ctx context.Context) (int64, error)
	// Must answer from the current canonical chain, never a cache: a scan
	// re-reads one height to prove its pin still names the same hash.
	GetBlockHeaderByNumber(ctx context.Context, blockNumber int64) (*BlockHeader, error)
}
