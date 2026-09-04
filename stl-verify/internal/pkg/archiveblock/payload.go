package archiveblock

import (
	"bytes"
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
// entry. It reads no further than that list, so a payload of megabytes costs the
// header fields ahead of it. A payload that cannot answer is an error: whether
// the block had transactions is the only thing that makes an empty receipt or
// trace list legitimate.
func HasTransactions(payload []byte) (bool, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	if open, err := dec.Token(); err != nil || open != json.Delim('{') {
		return false, errors.New("the payload is not a block object")
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return false, fmt.Errorf("reading the payload's fields: %w", err)
		}
		if key != transactionsField {
			var skipped json.RawMessage
			if err := dec.Decode(&skipped); err != nil {
				return false, fmt.Errorf("reading past %v: %w", key, err)
			}
			continue
		}
		if open, err := dec.Token(); err != nil || open != json.Delim('[') {
			return false, fmt.Errorf("the payload's %s are not a list", transactionsField)
		}
		return dec.More(), nil
	}
	return false, fmt.Errorf("the payload carries no %s", transactionsField)
}

func emptyList(payload []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(payload))
	if open, err := dec.Token(); err != nil || open != json.Delim('[') {
		return false
	}
	closed, err := dec.Token()
	return err == nil && closed == json.Delim(']')
}
