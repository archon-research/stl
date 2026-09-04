package archiveblock

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrEmptyList marks a receipt or trace list with no elements: what a block with
// no transactions is answered with, and the only answer that names no block a
// caller may accept.
var ErrEmptyList = errors.New("the list holds no elements")

const transactionsField = "transactions"

// HashFromPayload returns the hash a block payload carries, for a document
// already in hand rather than in the archive.
func HashFromPayload(payload []byte) (string, bool) {
	hash, outcome := scanStringField(payload, blockHashDepth, blockHashField)
	return hash, outcome == fieldFound
}

// ListBlockHash returns the block a receipt or trace list describes, from the
// blockHash its elements carry. A list that names no block is an error rather
// than an empty answer: a fetch has to be able to tell the block it asked for
// from the one it was served.
func ListBlockHash(payload []byte) (string, error) {
	hash, outcome := scanStringField(payload, listBlockHashDepth, listBlockHashField)
	if outcome == fieldFound {
		return hash, nil
	}
	if emptyList(payload) {
		return "", ErrEmptyList
	}
	return "", fmt.Errorf("no element of the list carries a %s", listBlockHashField)
}

// HasTransactions reports whether a block payload's transaction list holds an
// entry; a payload without the list cannot answer and is an error.
func HasTransactions(payload []byte) (bool, error) {
	var block struct {
		Transactions *[]json.RawMessage `json:"transactions"`
	}
	if err := json.Unmarshal(payload, &block); err != nil {
		return false, fmt.Errorf("decoding the block payload: %w", err)
	}
	if block.Transactions == nil {
		return false, fmt.Errorf("the payload carries no %s", transactionsField)
	}
	return len(*block.Transactions) > 0, nil
}

func emptyList(payload []byte) bool {
	var elements *[]json.RawMessage
	return json.Unmarshal(payload, &elements) == nil && elements != nil && len(*elements) == 0
}
