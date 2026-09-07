package shared

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// testSnapshotPool is the generic type parameter used to exercise
// RunSnapshotReads without depending on any DEX-specific pool type.
type testSnapshotPool struct {
	Address common.Address
}

func returnData(tag byte) []byte {
	return []byte{tag}
}

func callFor(tag byte) outbound.Call {
	return outbound.Call{CallData: []byte{tag}}
}

// resultOf packs a single-byte payload into a Result so a Decode function can
// verify, by content, exactly which results it was handed (and in what order).
func resultOf(tag byte) outbound.Result {
	return outbound.Result{Success: true, ReturnData: returnData(tag)}
}

func TestRunSnapshotReads_RoutesEachReadItsOwnResults(t *testing.T) {
	pool := testSnapshotPool{Address: common.HexToAddress("0x1")}

	var gotA, gotB, gotC []outbound.Result

	reads := []SnapshotRead[testSnapshotPool]{
		{
			Name: "readA",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(1)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error {
				gotA = results
				return nil
			},
		},
		{
			Name: "readB",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(2), callFor(3)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error {
				gotB = results
				return nil
			},
		},
		{
			Name: "readC",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(4)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error {
				gotC = results
				return nil
			},
		},
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		if len(calls) != 4 {
			return nil, fmt.Errorf("test stub: got %d calls, want 4", len(calls))
		}
		return []outbound.Result{
			resultOf(1),
			resultOf(2),
			resultOf(3),
			resultOf(4),
		}, nil
	}

	blockHash := common.HexToHash("0xdeadbeef")
	if err := RunSnapshotReads(context.Background(), mc, pool, blockHash, reads); err != nil {
		t.Fatalf("RunSnapshotReads: %v", err)
	}

	if len(gotA) != 1 || gotA[0].ReturnData[0] != 1 {
		t.Errorf("readA results = %+v, want exactly [tag 1]", gotA)
	}
	if len(gotB) != 2 || gotB[0].ReturnData[0] != 2 || gotB[1].ReturnData[0] != 3 {
		t.Errorf("readB results = %+v, want exactly [tag 2, tag 3] in order", gotB)
	}
	if len(gotC) != 1 || gotC[0].ReturnData[0] != 4 {
		t.Errorf("readC results = %+v, want exactly [tag 4]", gotC)
	}
}

func TestRunSnapshotReads_PackErrorNamesRead(t *testing.T) {
	pool := testSnapshotPool{}
	wantErr := errors.New("boom")

	reads := []SnapshotRead[testSnapshotPool]{
		{
			Name: "readOK",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(1)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error { return nil },
		},
		{
			Name: "readFailsToPack",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return nil, wantErr
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error { return nil },
		},
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		t.Fatal("ExecuteAtHash must not be called when a Pack fails")
		return nil, nil
	}

	err := RunSnapshotReads(context.Background(), mc, pool, common.Hash{}, reads)
	if err == nil {
		t.Fatal("RunSnapshotReads: want error when a read's Pack fails, got nil")
	}
	if !strings.Contains(err.Error(), "readFailsToPack") {
		t.Errorf("error %q must name the failing read (readFailsToPack)", err)
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error %q must wrap the underlying Pack error", err)
	}
}

func TestRunSnapshotReads_DecodeErrorNamesRead(t *testing.T) {
	pool := testSnapshotPool{}
	wantErr := errors.New("bad decode")

	reads := []SnapshotRead[testSnapshotPool]{
		{
			Name: "readOK",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(1)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error { return nil },
		},
		{
			Name: "readFailsToDecode",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(2)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error {
				return wantErr
			},
		},
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		return []outbound.Result{resultOf(1), resultOf(2)}, nil
	}

	err := RunSnapshotReads(context.Background(), mc, pool, common.Hash{}, reads)
	if err == nil {
		t.Fatal("RunSnapshotReads: want error when a read's Decode fails, got nil")
	}
	if !strings.Contains(err.Error(), "readFailsToDecode") {
		t.Errorf("error %q must name the failing read (readFailsToDecode)", err)
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error %q must wrap the underlying Decode error", err)
	}
}

func TestRunSnapshotReads_ResultCountMismatchErrors(t *testing.T) {
	pool := testSnapshotPool{}

	reads := []SnapshotRead[testSnapshotPool]{
		{
			Name: "readA",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(1), callFor(2)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error {
				t.Fatal("Decode must not be called when the result count mismatches (contract violation)")
				return nil
			},
		},
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
		// Deliberately return fewer results than calls issued.
		return []outbound.Result{resultOf(1)}, nil
	}

	err := RunSnapshotReads(context.Background(), mc, pool, common.Hash{}, reads)
	if err == nil {
		t.Fatal("RunSnapshotReads: want error on result-count mismatch, got nil")
	}
}

func TestRunSnapshotReads_PinsToGivenBlockHash(t *testing.T) {
	pool := testSnapshotPool{}
	blockHash := common.HexToHash("0xcafebabe")

	reads := []SnapshotRead[testSnapshotPool]{
		{
			Name: "readA",
			Pack: func(p testSnapshotPool) ([]outbound.Call, error) {
				return []outbound.Call{callFor(1)}, nil
			},
			Decode: func(p testSnapshotPool, results []outbound.Result) error { return nil },
		},
	}

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("Execute must not be called; RunSnapshotReads must pin reads via ExecuteAtHash")
		return nil, nil
	}
	var gotHash common.Hash
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, hash common.Hash) ([]outbound.Result, error) {
		gotHash = hash
		return []outbound.Result{resultOf(1)}, nil
	}

	if err := RunSnapshotReads(context.Background(), mc, pool, blockHash, reads); err != nil {
		t.Fatalf("RunSnapshotReads: %v", err)
	}
	if gotHash != blockHash {
		t.Errorf("ExecuteAtHash blockHash = %s, want %s", gotHash, blockHash)
	}
	if mc.CallCount != 1 {
		t.Errorf("ExecuteAtHash invocation count = %d, want 1 (single batched multicall)", mc.CallCount)
	}
}
