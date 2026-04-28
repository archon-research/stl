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
