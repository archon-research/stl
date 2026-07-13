package blockchain

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// fakeHeaders serves headers by block arg: the finalized tag (negative arg) maps
// to finalizedNum, and a concrete height maps to canonicalHashes[height].
type fakeHeaders struct {
	finalizedNum   int64
	canonicalHash  common.Hash
	finalizedErr   error
	canonicalErr   error
	nilFinalized   bool
	nilCanonical   bool
	canonicalCalls int
}

func (f *fakeHeaders) HeaderByNumber(_ context.Context, number *big.Int) (*types.Header, error) {
	if number != nil && number.Sign() < 0 { // the "finalized" tag
		if f.finalizedErr != nil {
			return nil, f.finalizedErr
		}
		if f.nilFinalized {
			return nil, nil
		}
		return &types.Header{Number: big.NewInt(f.finalizedNum)}, nil
	}
	f.canonicalCalls++
	if f.canonicalErr != nil {
		return nil, f.canonicalErr
	}
	if f.nilCanonical {
		return nil, nil
	}
	// Craft a header whose Hash() is deterministic; the test compares against the
	// hash of this very header, so it stands in for "the canonical block".
	return &types.Header{Number: new(big.Int).Set(number), Extra: f.canonicalHash.Bytes()}, nil
}

// canonicalHashOf mirrors what the fake will produce for a given height, so a
// test can assert "the event hash matches the canonical one".
func canonicalHashOf(height int64, marker common.Hash) common.Hash {
	return (&types.Header{Number: big.NewInt(height), Extra: marker.Bytes()}).Hash()
}

// A finalized block whose canonical hash differs from ours IS reorged out.
func TestReorgedOut_FinalizedAndDifferentHash_True(t *testing.T) {
	f := &fakeHeaders{finalizedNum: 200, canonicalHash: common.HexToHash("0xaa")}
	c := NewReorgChecker(f)

	out, err := c.ReorgedOut(context.Background(), 100, common.HexToHash("0xdead"))
	if err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	if !out {
		t.Error("finalized block with a different canonical hash must report reorged out")
	}
}

// A finalized block whose canonical hash matches ours is NOT reorged out.
func TestReorgedOut_FinalizedAndSameHash_False(t *testing.T) {
	marker := common.HexToHash("0xaa")
	f := &fakeHeaders{finalizedNum: 200, canonicalHash: marker}
	c := NewReorgChecker(f)

	out, err := c.ReorgedOut(context.Background(), 100, canonicalHashOf(100, marker))
	if err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	if out {
		t.Error("a still-canonical finalized block must not report reorged out")
	}
}

// THE CORE GUARD: a block above the finalized head is never judged, even if our
// node currently shows a different hash there (tip churn / minority fork). It
// must report false so the caller retries rather than dropping a block the
// watcher may consider canonical and never republish.
func TestReorgedOut_NotYetFinalized_NeverJudges(t *testing.T) {
	f := &fakeHeaders{finalizedNum: 100, canonicalHash: common.HexToHash("0xaa")}
	c := NewReorgChecker(f)

	out, err := c.ReorgedOut(context.Background(), 150, common.HexToHash("0xdead"))
	if err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	if out {
		t.Fatal("a block above the finalized head must never be judged reorged out")
	}
	if f.canonicalCalls != 0 {
		t.Errorf("canonical header fetched %d times for a non-final block, want 0 (must not even ask)", f.canonicalCalls)
	}
}

// Exactly at the finalized head is final, so it may be judged.
func TestReorgedOut_AtFinalizedHead_IsJudged(t *testing.T) {
	f := &fakeHeaders{finalizedNum: 100, canonicalHash: common.HexToHash("0xaa")}
	c := NewReorgChecker(f)

	out, err := c.ReorgedOut(context.Background(), 100, common.HexToHash("0xdead"))
	if err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	if !out {
		t.Error("a block AT the finalized head must be judged (and here is reorged out)")
	}
}

// Any failure to determine the answer must error (caller retries, never skips).
func TestReorgedOut_ErrorsPropagate(t *testing.T) {
	cases := []struct {
		name string
		f    *fakeHeaders
	}{
		{"finalized head lookup fails", &fakeHeaders{finalizedErr: errors.New("rpc down")}},
		{"nil finalized header", &fakeHeaders{nilFinalized: true}},
		{"canonical lookup fails", &fakeHeaders{finalizedNum: 200, canonicalErr: errors.New("rpc down")}},
		{"nil canonical header", &fakeHeaders{finalizedNum: 200, nilCanonical: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewReorgChecker(tc.f)
			out, err := c.ReorgedOut(context.Background(), 100, common.HexToHash("0xdead"))
			if err == nil {
				t.Fatal("an undeterminable reorg check must return an error so the caller retries")
			}
			if out {
				t.Error("must not report reorged out when the check failed")
			}
		})
	}
}

// The finalized tag is what we actually ask for (guards against silently
// pinning to "latest", which would defeat the finality gate entirely).
func TestReorgedOut_UsesFinalizedTag(t *testing.T) {
	var gotArg *big.Int
	f := &argRecorder{onCall: func(n *big.Int) {
		if gotArg == nil {
			gotArg = n
		}
	}, finalizedNum: 200}
	c := NewReorgChecker(f)

	if _, err := c.ReorgedOut(context.Background(), 100, common.HexToHash("0xdead")); err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	want := big.NewInt(int64(rpc.FinalizedBlockNumber))
	if gotArg == nil || gotArg.Cmp(want) != 0 {
		t.Errorf("first header arg = %v, want the finalized tag %v", gotArg, want)
	}
}

// argRecorder records the first block arg passed to HeaderByNumber.
type argRecorder struct {
	onCall       func(*big.Int)
	finalizedNum int64
}

func (a *argRecorder) HeaderByNumber(_ context.Context, number *big.Int) (*types.Header, error) {
	a.onCall(number)
	if number != nil && number.Sign() < 0 {
		return &types.Header{Number: big.NewInt(a.finalizedNum)}, nil
	}
	return &types.Header{Number: new(big.Int).Set(number)}, nil
}
