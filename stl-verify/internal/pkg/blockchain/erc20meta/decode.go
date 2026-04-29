// Package erc20meta decodes ERC20 metadata return values across the
// modern (`string`) and legacy (`bytes32`) ABIs.
//
// The ERC20 standard specifies `symbol()` and `name()` as returning
// `string`, but a handful of early ERC20 deployments — most notably MKR
// — return raw `bytes32` instead. Tools that strictly decode as `string`
// either error out or silently produce the wrong value for these tokens.
// This package's helper handles both cases transparently and guarantees
// the returned string is valid UTF-8 (since we persist these into pgx
// `text` columns, which reject invalid UTF-8).
package erc20meta

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

// DecodeStringOrBytes32 decodes an ERC20 method return value that should
// represent a human-readable identifier (typically `symbol()` or
// `name()`). It first tries the standard `string` decode, and on failure
// falls back to interpreting the raw 32-byte return as a left-aligned,
// null-padded ASCII string (the MKR-style convention).
//
// The result is guaranteed valid UTF-8: invalid byte sequences (which
// EVM bytes32 can legally contain — the ABI doesn't constrain payload
// shape) are dropped via strings.ToValidUTF8. This matters because the
// downstream `text` columns in Postgres reject non-UTF-8 input via pgx,
// turning any pathological symbol/name into a stuck-message DLQ flow.
//
// Returns a non-nil error only when both decode paths fail — the caller
// should treat that as a structural problem with the contract, not a
// VEC-188-style transport issue.
func DecodeStringOrBytes32(erc20ABI *abi.ABI, methodName string, data []byte) (string, error) {
	if _, ok := erc20ABI.Methods[methodName]; !ok {
		return "", fmt.Errorf("erc20meta: method %q not in ABI", methodName)
	}
	if len(data) == 0 {
		return "", fmt.Errorf("erc20meta: empty return data for %s", methodName)
	}

	// Modern path: try unpacking as `string`.
	unpacked, stringErr := erc20ABI.Unpack(methodName, data)
	if stringErr == nil && len(unpacked) > 0 {
		if s, ok := unpacked[0].(string); ok {
			return strings.ToValidUTF8(s, ""), nil
		}
	}

	// Legacy fallback: tokens like MKR return the value as a raw bytes32.
	// The convention is ASCII content left-aligned, with trailing null
	// padding to fill 32 bytes.
	if len(data) == 32 {
		trimmed := strings.TrimRight(string(data), "\x00")
		// Sanitize: a contract is free to put arbitrary bytes here, and
		// invalid UTF-8 would be rejected by Postgres on the downstream
		// text upsert. Drop invalid byte sequences rather than failing
		// the whole row — symbols/names are display-only and an empty
		// string is a safer outcome than a stuck message.
		return strings.ToValidUTF8(trimmed, ""), nil
	}

	if stringErr != nil {
		return "", fmt.Errorf("erc20meta: decode %s: not a string and not bytes32: %w", methodName, stringErr)
	}
	return "", fmt.Errorf("erc20meta: decode %s: unpack returned no values and data is not bytes32-shaped", methodName)
}
