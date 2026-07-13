package dexconsumer

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// fakeBlockStates implements outbound.BlockStateRepository; only GetBlockByHash
// is exercised, so the embedded interface leaves the rest unimplemented.
type fakeBlockStates struct {
	outbound.BlockStateRepository
	state   *outbound.BlockState
	err     error
	gotHash string
}

func (f *fakeBlockStates) GetBlockByHash(_ context.Context, hash string) (*outbound.BlockState, error) {
	f.gotHash = hash
	return f.state, f.err
}

// The hash the watcher marked orphaned in staging (block 25512663 v0).
var testHash = common.HexToHash("0x5a94ae0445960dc11f7bf6048201a2ad600f2ad2fac663b4c45fd9ede414424f")

// The watcher marked THIS hash orphaned -> proven reorged out.
func TestBlockStateReorgChecker_OrphanedHash_True(t *testing.T) {
	repo := &fakeBlockStates{state: &outbound.BlockState{Number: 25512663, IsOrphaned: true}}
	c := NewBlockStateReorgChecker(repo)

	out, err := c.ReorgedOut(context.Background(), 25512663, testHash)
	if err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	if !out {
		t.Error("a hash the watcher marked orphaned must report reorged out")
	}
	if repo.gotHash != testHash.Hex() {
		t.Errorf("looked up hash %q, want %q (must query the exact published hash)", repo.gotHash, testHash.Hex())
	}
}

// Recorded but NOT orphaned -> still canonical for our pipeline -> retry.
func TestBlockStateReorgChecker_CanonicalHash_False(t *testing.T) {
	repo := &fakeBlockStates{state: &outbound.BlockState{Number: 25512663, IsOrphaned: false}}
	c := NewBlockStateReorgChecker(repo)

	out, err := c.ReorgedOut(context.Background(), 25512663, testHash)
	if err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	if out {
		t.Error("a block the watcher has not orphaned must not report reorged out")
	}
}

// Unknown to the watcher -> cannot prove it was replaced -> never skip.
func TestBlockStateReorgChecker_UnknownHash_False(t *testing.T) {
	repo := &fakeBlockStates{state: nil} // GetBlockByHash returns (nil, nil) when absent
	c := NewBlockStateReorgChecker(repo)

	out, err := c.ReorgedOut(context.Background(), 25512663, testHash)
	if err != nil {
		t.Fatalf("ReorgedOut: %v", err)
	}
	if out {
		t.Error("a hash the watcher never recorded must not report reorged out")
	}
}

// Lookup failure -> unknown -> error so the caller retries, never skips.
func TestBlockStateReorgChecker_LookupError_Errors(t *testing.T) {
	repo := &fakeBlockStates{err: errors.New("db down")}
	c := NewBlockStateReorgChecker(repo)

	out, err := c.ReorgedOut(context.Background(), 25512663, testHash)
	if err == nil {
		t.Fatal("a failed lookup must return an error so the caller retries")
	}
	if out {
		t.Error("must not report reorged out when the lookup failed")
	}
}

// A height mismatch between the event and our record is an anomaly: refuse to
// judge rather than discard on a disagreement.
func TestBlockStateReorgChecker_HeightMismatch_Errors(t *testing.T) {
	repo := &fakeBlockStates{state: &outbound.BlockState{Number: 999, IsOrphaned: true}}
	c := NewBlockStateReorgChecker(repo)

	out, err := c.ReorgedOut(context.Background(), 25512663, testHash)
	if err == nil {
		t.Fatal("a block-number mismatch must return an error, not a skip verdict")
	}
	if out {
		t.Error("must not report reorged out on a height mismatch")
	}
}
