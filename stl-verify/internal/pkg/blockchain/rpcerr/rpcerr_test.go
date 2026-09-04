package rpcerr

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// fakeRPCErr implements go-ethereum's rpc.Error interface
// (Error() string; ErrorCode() int).
type fakeRPCErr struct {
	code int
	msg  string
}

func (e *fakeRPCErr) Error() string  { return e.msg }
func (e *fakeRPCErr) ErrorCode() int { return e.code }

func TestIsEVMRevert(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantRevert bool
	}{
		{name: "nil error is not a revert", err: nil, wantRevert: false},
		{name: "geth code 3 execution reverted is a revert", err: &fakeRPCErr{code: 3, msg: "execution reverted"}, wantRevert: true},
		{name: "alchemy code -32000 with 'execution reverted' message is a revert", err: &fakeRPCErr{code: -32000, msg: "execution reverted: custom reason"}, wantRevert: true},
		{name: "erigon-style 'Execution reverted' (capital E) is a revert", err: &fakeRPCErr{code: -32000, msg: "Execution reverted"}, wantRevert: true},
		{name: "nethermind-style 'Execution Reverted' (mixed case) is a revert", err: &fakeRPCErr{code: -32000, msg: "Execution Reverted: panic code 0x11"}, wantRevert: true},
		{name: "alchemy 429 rate limit is NOT a revert", err: &fakeRPCErr{code: 429, msg: "Your app has exceeded its compute units per second capacity"}, wantRevert: false},
		{name: "alchemy -32005 limit exceeded is NOT a revert", err: &fakeRPCErr{code: -32005, msg: "Too many requests"}, wantRevert: false},
		{name: "internal server error is NOT a revert", err: &fakeRPCErr{code: -32603, msg: "internal error"}, wantRevert: false},
		{name: "wrapped revert is still a revert (errors.As climbs)", err: fmt.Errorf("batch step 2: %w", &fakeRPCErr{code: 3, msg: "execution reverted"}), wantRevert: true},
		{name: "plain stdlib error is NOT a revert", err: errors.New("connection reset by peer"), wantRevert: false},
		{name: "net.OpError (network) is NOT a revert", err: &net.OpError{Op: "dial", Err: errors.New("timeout")}, wantRevert: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsEVMRevert(tt.err)
			if got != tt.wantRevert {
				t.Errorf("IsEVMRevert(%v) = %v, want %v", tt.err, got, tt.wantRevert)
			}
		})
	}
}

func TestIsGasExhausted(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		wantGas bool
	}{
		{name: "nil error is not gas exhaustion", err: nil, wantGas: false},
		{name: "alchemy out-of-gas answer is gas exhaustion", err: &fakeRPCErr{code: -32000, msg: "out of gas: gas required exceeds: 550000000"}, wantGas: true},
		{name: "geth 'gas required exceeds allowance' is gas exhaustion", err: &fakeRPCErr{code: -32000, msg: "gas required exceeds allowance (50000000)"}, wantGas: true},
		{name: "capitalised 'Out of gas' is gas exhaustion", err: &fakeRPCErr{code: -32000, msg: "Out of gas"}, wantGas: true},
		{name: "wrapped out-of-gas is still gas exhaustion (errors.As climbs)", err: fmt.Errorf("multicall probe: %w", &fakeRPCErr{code: -32000, msg: "out of gas"}), wantGas: true},
		{name: "execution revert is NOT gas exhaustion", err: &fakeRPCErr{code: 3, msg: "execution reverted"}, wantGas: false},
		{name: "alchemy 429 rate limit is NOT gas exhaustion", err: &fakeRPCErr{code: 429, msg: "Your app has exceeded its compute units per second capacity"}, wantGas: false},
		{name: "plain stdlib error naming gas is NOT gas exhaustion", err: errors.New("out of gas"), wantGas: false},
		{name: "net.OpError (network) is NOT gas exhaustion", err: &net.OpError{Op: "dial", Err: errors.New("timeout")}, wantGas: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsGasExhausted(tt.err); got != tt.wantGas {
				t.Errorf("IsGasExhausted(%v) = %v, want %v", tt.err, got, tt.wantGas)
			}
		})
	}
}

func TestRequireAllSucceeded(t *testing.T) {
	t.Run("nil results is an error", func(t *testing.T) {
		if err := RequireAllSucceeded(nil, "test"); err == nil {
			t.Fatal("expected error for nil results")
		}
	})
	t.Run("empty results is an error", func(t *testing.T) {
		if err := RequireAllSucceeded([]outbound.Result{}, "test"); err == nil {
			t.Fatal("expected error for empty results")
		}
	})
	t.Run("all success returns nil", func(t *testing.T) {
		results := []outbound.Result{{Success: true, ReturnData: []byte{1}}, {Success: true, ReturnData: []byte{2}}}
		if err := RequireAllSucceeded(results, "test"); err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})
	t.Run("any failure returns error naming the context", func(t *testing.T) {
		results := []outbound.Result{{Success: true}, {Success: false}}
		err := RequireAllSucceeded(results, "getTokenMetadata")
		if err == nil {
			t.Fatal("expected error on sub-call failure")
		}
		if !strings.Contains(err.Error(), "getTokenMetadata") {
			t.Errorf("error should name context; got %q", err)
		}
		if !strings.Contains(err.Error(), "1") {
			t.Errorf("error should name failing index; got %q", err)
		}
	})
	t.Run("multiple failures all reported", func(t *testing.T) {
		results := []outbound.Result{{Success: false}, {Success: true}, {Success: false}}
		err := RequireAllSucceeded(results, "ctx")
		if err == nil {
			t.Fatal("expected error")
		}
		msg := err.Error()
		if !strings.Contains(msg, "0") || !strings.Contains(msg, "2") {
			t.Errorf("error should name both failing indices 0 and 2; got %q", err)
		}
	})
}
