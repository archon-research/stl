package abis

import (
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

// The literal lives HERE and nowhere else: every other site now reads
// TransferTopic0, so this is the only thing holding it to the real hash. Both
// assertions are needed — the keccak one proves the literal, the fragment one
// proves the ABI parses to the same event and has not had an indexed flag or an
// argument type edited out from under it.
func TestTransferTopic0_IsTheRealTransferSignature(t *testing.T) {
	want := crypto.Keccak256Hash([]byte("Transfer(address,address,uint256)"))
	if TransferTopic0() != want {
		t.Fatalf("TransferTopic0 = %s, want keccak256 of the Transfer signature %s", TransferTopic0(), want)
	}

	ev, err := ERC721TransferEvent()
	if err != nil {
		t.Fatalf("ERC721TransferEvent: %v", err)
	}
	if ev.ID != TransferTopic0() {
		t.Fatalf("ERC721TransferEvent().ID = %s, want TransferTopic0 %s", ev.ID, TransferTopic0())
	}
}

// tokenId indexed is the whole reason this fragment is separate from ERC-20's:
// it is what puts the id in topics[3] and leaves data empty, which is what the
// posm decoder's four-topic guard keys on.
func TestERC721TransferEvent_IndexesEveryArgument(t *testing.T) {
	ev, err := ERC721TransferEvent()
	if err != nil {
		t.Fatalf("ERC721TransferEvent: %v", err)
	}
	if got := len(ev.Inputs); got != 3 {
		t.Fatalf("Transfer has %d inputs, want 3", got)
	}
	for _, in := range ev.Inputs {
		if !in.Indexed {
			t.Errorf("input %q is not indexed: an ERC-721 Transfer indexes all three, so a non-indexed one would be decoded out of the data block", in.Name)
		}
	}
	if got := ev.Inputs[2].Type.String(); got != "uint256" {
		t.Errorf("tokenId type is %s, want uint256", got)
	}
}
