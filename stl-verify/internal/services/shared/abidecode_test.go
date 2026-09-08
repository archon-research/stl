package shared

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func uintTestABI(t *testing.T) *abi.ABI {
	t.Helper()
	a, err := abi.JSON(strings.NewReader(`[
		{"type":"function","name":"v","outputs":[{"type":"uint256"}],"stateMutability":"view"},
		{"type":"function","name":"b","outputs":[{"type":"bool"}],"stateMutability":"view"},
		{"type":"function","name":"none","outputs":[],"stateMutability":"view"}
	]`))
	if err != nil {
		t.Fatalf("abi.JSON: %v", err)
	}
	return &a
}

func TestUnpackUint(t *testing.T) {
	a := uintTestABI(t)
	okData, err := a.Methods["v"].Outputs.Pack(big.NewInt(42))
	if err != nil {
		t.Fatalf("pack uint: %v", err)
	}
	boolData, err := a.Methods["b"].Outputs.Pack(true)
	if err != nil {
		t.Fatalf("pack bool: %v", err)
	}

	t.Run("success returns value", func(t *testing.T) {
		got, err := UnpackUint(a, "v", outbound.Result{Success: true, ReturnData: okData})
		if err != nil {
			t.Fatalf("UnpackUint: %v", err)
		}
		if got.Cmp(big.NewInt(42)) != 0 {
			t.Errorf("got %s, want 42", got)
		}
	})

	t.Run("revert errors", func(t *testing.T) {
		if _, err := UnpackUint(a, "v", outbound.Result{Success: false}); err == nil {
			t.Fatal("expected error on reverted sub-call")
		}
	})

	t.Run("undecodable payload errors with method context", func(t *testing.T) {
		_, err := UnpackUint(a, "v", outbound.Result{Success: true, ReturnData: []byte{0x01, 0x02}})
		if err == nil {
			t.Fatal("expected decode error on truncated payload")
		}
		if !strings.Contains(err.Error(), "unpacking v") {
			t.Errorf("error %q should name the method so multicall decode failures are triageable", err)
		}
	})

	t.Run("wrong return type errors", func(t *testing.T) {
		if _, err := UnpackUint(a, "b", outbound.Result{Success: true, ReturnData: boolData}); err == nil {
			t.Fatal("expected error when first value is not *big.Int")
		}
	})

	t.Run("empty outputs errors", func(t *testing.T) {
		if _, err := UnpackUint(a, "none", outbound.Result{Success: true, ReturnData: nil}); err == nil {
			t.Fatal("expected error when method returns no values")
		}
	})

	t.Run("nil ABI errors without panicking", func(t *testing.T) {
		if _, err := UnpackUint(nil, "v", outbound.Result{Success: true, ReturnData: okData}); err == nil {
			t.Fatal("expected an error (not a panic) when the ABI is nil")
		}
	})
}
