package morpho_v2_bootstrap

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// blockRange is one inclusive [From,To] slice of the sweep.
type blockRange struct {
	From, To int64
}

// chunkBlockRange splits [from,to] into consecutive inclusive ranges of at most
// size blocks each, covering every block exactly once. size must be >= 1
// (Config.validate enforces it); an inverted range yields no chunks.
func chunkBlockRange(from, to, size int64) []blockRange {
	if from > to {
		return nil
	}
	var chunks []blockRange
	for start := from; start <= to; start += size {
		chunks = append(chunks, blockRange{From: start, To: min(start+size-1, to)})
	}
	return chunks
}

// batchAddresses splits addresses into groups of at most size, so one
// eth_getLogs address filter stays within what the provider accepts. size must
// be >= 1 (Config.validate enforces it).
func batchAddresses(addresses []common.Address, size int) [][]common.Address {
	var batches [][]common.Address
	for start := 0; start < len(addresses); start += size {
		end := min(start+size, len(addresses))
		batches = append(batches, addresses[start:end])
	}
	return batches
}

// sortLogs orders logs strictly by (block number, log index).
//
// Ordering is desirable, not correctness-critical: membership is an append-only
// log keyed on each observation's own position, so an out-of-order replay reaches
// the same answers. What the sort buys is a clean ops signal — an Allocate
// replayed ahead of its AddAdapter records a redundant allocation_event row and a
// WARN that otherwise means "discovery missed an adapter". Each chunk is fetched
// as several per-address-batch requests whose results interleave, so the node's
// per-response ordering is not enough.
func sortLogs(logs []ethtypes.Log) {
	slices.SortFunc(logs, func(a, b ethtypes.Log) int {
		if a.BlockNumber != b.BlockNumber {
			return int(a.BlockNumber) - int(b.BlockNumber)
		}
		return int(a.Index) - int(b.Index)
	})
}

// toSharedLog renders a decoded go-ethereum log back into the JSON-RPC wire
// shape the Morpho handlers decode. The hex encodings match what the live SQS
// path receives from the node, so a replayed event produces a byte-identical
// audit row.
func toSharedLog(l ethtypes.Log) shared.Log {
	topics := make([]string, len(l.Topics))
	for i, t := range l.Topics {
		topics[i] = t.Hex()
	}
	return shared.Log{
		Address:          l.Address.Hex(),
		Topics:           topics,
		Data:             hexutil.Encode(l.Data),
		BlockHash:        l.BlockHash.Hex(),
		BlockNumber:      hexutil.EncodeUint64(l.BlockNumber),
		TransactionHash:  l.TxHash.Hex(),
		TransactionIndex: hexutil.EncodeUint64(uint64(l.TxIndex)),
		LogIndex:         hexutil.EncodeUint64(uint64(l.Index)),
		Removed:          l.Removed,
	}
}

// rangeTooLargeSignals are the provider responses that mean "ask for fewer
// blocks", not "this failed". They ride HTTP 200 as JSON-RPC errors, so the
// rpchttp retry transport (429/5xx/network only) correctly does not retry them
// and the sweep must narrow its range instead. Everything else bubbles.
//
// Each phrase is specific to a cap. A bare "block range" would also match
// malformed-parameter errors like "invalid block range params", which would then
// be halved fourteen times before surfacing — burying the real cause under
// narrowing logs.
var rangeTooLargeSignals = []string{
	"more than 10000 results",
	"log response size exceeded",
	"block range is too large",
	"query returned more than",
	"reducing your block range",
	"up to a", // "…requests with up to a 500 block range"
}

// isRangeTooLargeError reports whether err is a provider result/range cap the
// sweep can work around by halving the requested block range.
func isRangeTooLargeError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, signal := range rangeTooLargeSignals {
		if strings.Contains(msg, signal) {
			return true
		}
	}
	return false
}

// fetchLogs returns every log emitted by addresses in [r.From,r.To] whose topic0
// is in topics, halving the requested range when the provider reports a result
// or range cap. A single-block request that still trips the cap is unrecoverable
// here and bubbles rather than silently returning a partial set.
func (s *Service) fetchLogs(ctx context.Context, addresses []common.Address, topics []common.Hash, r blockRange) ([]ethtypes.Log, error) {
	logs, err := s.chain.FilterLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(r.From),
		ToBlock:   big.NewInt(r.To),
		Addresses: addresses,
		Topics:    [][]common.Hash{topics},
	})
	if err == nil {
		return logs, nil
	}
	if !isRangeTooLargeError(err) || r.From == r.To {
		return nil, fmt.Errorf("eth_getLogs [%d,%d] over %d addresses: %w", r.From, r.To, len(addresses), err)
	}

	mid := r.From + (r.To-r.From)/2
	s.logger.Info("narrowing eth_getLogs range after a provider cap",
		"from", r.From, "to", r.To, "splitAt", mid, "error", err)

	lower, err := s.fetchLogs(ctx, addresses, topics, blockRange{From: r.From, To: mid})
	if err != nil {
		return nil, err
	}
	upper, err := s.fetchLogs(ctx, addresses, topics, blockRange{From: mid + 1, To: r.To})
	if err != nil {
		return nil, err
	}
	return append(lower, upper...), nil
}
