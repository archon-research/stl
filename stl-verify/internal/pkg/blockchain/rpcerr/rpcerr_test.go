package rpcerr

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestIsEVMRevert(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantRevert bool
	}{
		{name: "nil error is not a revert", err: nil, wantRevert: false},
		{name: "geth code 3 execution reverted is a revert", err: testutil.RPCError{Code: 3, Msg: "execution reverted"}, wantRevert: true},
		{name: "alchemy code -32000 with 'execution reverted' message is a revert", err: testutil.RPCError{Code: -32000, Msg: "execution reverted: custom reason"}, wantRevert: true},
		{name: "erigon-style 'Execution reverted' (capital E) is a revert", err: testutil.RPCError{Code: -32000, Msg: "Execution reverted"}, wantRevert: true},
		{name: "nethermind-style 'Execution Reverted' (mixed case) is a revert", err: testutil.RPCError{Code: -32000, Msg: "Execution Reverted: panic code 0x11"}, wantRevert: true},
		{name: "alchemy 429 rate limit is NOT a revert", err: testutil.RPCError{Code: 429, Msg: "Your app has exceeded its compute units per second capacity"}, wantRevert: false},
		{name: "alchemy -32005 limit exceeded is NOT a revert", err: testutil.RPCError{Code: -32005, Msg: "Too many requests"}, wantRevert: false},
		{name: "internal server error is NOT a revert", err: testutil.RPCError{Code: -32603, Msg: "internal error"}, wantRevert: false},
		{name: "wrapped revert is still a revert (errors.As climbs)", err: fmt.Errorf("batch step 2: %w", testutil.RPCError{Code: 3, Msg: "execution reverted"}), wantRevert: true},
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
		{name: "alchemy out-of-gas answer is gas exhaustion", err: testutil.RPCError{Code: -32000, Msg: "out of gas: gas required exceeds: 550000000"}, wantGas: true},
		{name: "geth 'gas required exceeds allowance' is gas exhaustion", err: testutil.RPCError{Code: -32000, Msg: "gas required exceeds allowance (50000000)"}, wantGas: true},
		{name: "capitalised 'Out of gas' is gas exhaustion", err: testutil.RPCError{Code: -32000, Msg: "Out of gas"}, wantGas: true},
		{name: "wrapped out-of-gas is still gas exhaustion (errors.As climbs)", err: fmt.Errorf("multicall probe: %w", testutil.RPCError{Code: -32000, Msg: "out of gas"}), wantGas: true},
		{name: "execution revert is NOT gas exhaustion", err: testutil.RPCError{Code: 3, Msg: "execution reverted"}, wantGas: false},
		{name: "alchemy 429 rate limit is NOT gas exhaustion", err: testutil.RPCError{Code: 429, Msg: "Your app has exceeded its compute units per second capacity"}, wantGas: false},
		{name: "nethermind-style VM error with the verdict in data is gas exhaustion", err: testutil.RPCError{Code: -32015, Msg: "VM execution error.", Data: "Out of gas"}, wantGas: true},
		{name: "VM error whose data says reverted is NOT gas exhaustion", err: testutil.RPCError{Code: -32015, Msg: "VM execution error.", Data: "Reverted 0x08c379a0"}, wantGas: false},
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

func TestIsRequestTooLarge(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil is not too large", err: nil, want: false},
		{name: "HTTP 413 is too large", err: rpc.HTTPError{StatusCode: http.StatusRequestEntityTooLarge, Status: "413 Request Entity Too Large"}, want: true},
		{name: "wrapped HTTP 413 is too large", err: fmt.Errorf("batch: %w", rpc.HTTPError{StatusCode: http.StatusRequestEntityTooLarge}), want: true},
		{name: "HTTP 429 is NOT too large", err: rpc.HTTPError{StatusCode: http.StatusTooManyRequests}, want: false},
		{name: "JSON-RPC error is NOT too large", err: testutil.RPCError{Code: -32000, Msg: "out of gas"}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsRequestTooLarge(tt.err); got != tt.want {
				t.Errorf("IsRequestTooLarge(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsBlockUnavailableAtHash(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil is not a missing block", err: nil, want: false},
		{name: "drpc -32001 block not found names the hash", err: testutil.RPCError{Code: -32001, Msg: "block not found: hash 0x70559c33e0f4f4d6e9f0d1c1a5f3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5"}, want: true},
		{name: "geth header-for-hash phrasing is a missing block", err: testutil.RPCError{Code: -32000, Msg: "header for hash not found"}, want: true},
		{name: "reth header not found is a missing block", err: testutil.RPCError{Code: -32000, Msg: "header not found"}, want: true},
		{name: "unknown block is a missing block", err: testutil.RPCError{Code: -32000, Msg: "unknown block"}, want: true},
		{name: "capitalised phrasing is a missing block", err: testutil.RPCError{Code: -32001, Msg: "Block not found"}, want: true},
		{name: "wrapped missing block is still a missing block", err: fmt.Errorf("multicall at hash: %w", testutil.RPCError{Code: -32001, Msg: "block not found: hash 0xabc"}), want: true},
		{name: "execution revert is NOT a missing block", err: testutil.RPCError{Code: 3, Msg: "execution reverted"}, want: false},
		{name: "rate limit is NOT a missing block", err: testutil.ThrottledRPCError(), want: false},
		{name: "out of gas is NOT a missing block", err: testutil.GasExhaustedRPCError(), want: false},
		{name: "plain stdlib error naming a missing block is NOT one", err: errors.New("block not found"), want: false},
		{name: "network failure is NOT a missing block", err: &net.OpError{Op: "dial", Err: errors.New("timeout")}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsBlockUnavailableAtHash(tt.err); got != tt.want {
				t.Errorf("IsBlockUnavailableAtHash(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestTagBlockUnavailableAtHashMarksAMissingBlockForErrorsIs(t *testing.T) {
	underlying := testutil.RPCError{Code: -32001, Msg: "block not found: hash 0xabc"}

	tagged := TagBlockUnavailableAtHash(underlying)

	if !errors.Is(tagged, ErrBlockUnavailableAtHash) {
		t.Errorf("errors.Is(%v, ErrBlockUnavailableAtHash) = false, want true", tagged)
	}
	if !errors.Is(tagged, underlying) {
		t.Errorf("tagging must keep the provider's own error matchable; got %v", tagged)
	}
}

func TestTagBlockUnavailableAtHashLeavesOtherErrorsAlone(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "nil", err: nil},
		{name: "revert", err: testutil.RPCError{Code: 3, Msg: "execution reverted"}},
		{name: "throttle", err: testutil.ThrottledRPCError()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := TagBlockUnavailableAtHash(tt.err)
			if got != tt.err {
				t.Errorf("TagBlockUnavailableAtHash(%v) = %v, want it returned unchanged", tt.err, got)
			}
			if errors.Is(got, ErrBlockUnavailableAtHash) {
				t.Errorf("%v must not match ErrBlockUnavailableAtHash", got)
			}
		})
	}
}
