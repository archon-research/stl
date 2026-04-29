// Package erc20meta decodes ERC20 metadata return values across the
// modern (`string`) and legacy (`bytes32`) ABIs.
//
// The ERC20 standard specifies `symbol()` and `name()` as returning
// `string`, but a handful of early ERC20 deployments — most notably MKR
// — return raw `bytes32` instead. Tools that strictly decode as `string`
// either error out or silently produce the wrong value for these tokens.
// This package's helper handles both cases transparently.
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
			return s, nil
		}
	}

	// Legacy fallback: tokens like MKR return the value as a raw bytes32.
	// The convention is ASCII content left-aligned, with trailing null
	// padding to fill 32 bytes.
	if len(data) == 32 {
		trimmed := strings.TrimRight(string(data), "\x00")
		// A 32-byte zero return ("") is also valid — the contract may
		// genuinely have no symbol/name. Fall through to the unpack-error
		// path only if the modern decode failed AND the bytes look
		// like garbage (non-zero, non-printable, etc.). For now we keep
		// the policy simple: any 32-byte return is acceptable as bytes32.
		return trimmed, nil
	}

	if stringErr != nil {
		return "", fmt.Errorf("erc20meta: decode %s: not a string and not bytes32: %w", methodName, stringErr)
	}
	return "", fmt.Errorf("erc20meta: decode %s: unpack returned no values and data is not bytes32-shaped", methodName)
}
