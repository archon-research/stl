package outbound_test

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const testHashHex = "0x00000000000000000000000000000000000000000000000000000000000000ab"

func TestPinForEvent_PinsByHash(t *testing.T) {
	ev := outbound.BlockEvent{ChainID: 1, BlockNumber: 23145001, Version: 2, BlockHash: testHashHex}

	pin, err := outbound.PinForEvent(ev)

	if err != nil {
		t.Fatalf("PinForEvent: %v", err)
	}
	if pin.Mode() != outbound.PinReorgSafe {
		t.Fatalf("mode = %d, want PinReorgSafe", pin.Mode())
	}
	h, ok := pin.Hash()
	if !ok || h != common.HexToHash(testHashHex) {
		t.Fatalf("hash = %s ok=%v, want %s", h.Hex(), ok, testHashHex)
	}
	if pin.Number() != 23145001 || pin.Version() != 2 {
		t.Fatalf("number/version = %d/%d, want 23145001/2", pin.Number(), pin.Version())
	}
}

func TestPinForEvent_RejectsInvalidEvents(t *testing.T) {
	cases := []struct {
		name string
		ev   outbound.BlockEvent
	}{
		{"missing hash", outbound.BlockEvent{BlockNumber: 100, Version: 1}},
		{"non-positive number", outbound.BlockEvent{BlockNumber: 0, Version: 0, BlockHash: testHashHex}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := outbound.PinForEvent(tc.ev); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestNumberPinnedConstructors_CarryNoHash(t *testing.T) {
	cases := []struct {
		name        string
		pin         outbound.BlockPin
		wantMode    outbound.PinMode
		wantVersion int
	}{
		{"settled", outbound.PinForSettledBlock(500, 3), outbound.PinSettled, 3},
		{"static", outbound.PinForStaticRead(500), outbound.PinStatic, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.pin.Mode() != tc.wantMode {
				t.Fatalf("mode = %d, want %d", tc.pin.Mode(), tc.wantMode)
			}
			if _, ok := tc.pin.Hash(); ok {
				t.Fatal("number-pinned pin must not expose a hash")
			}
			if tc.pin.Number() != 500 || tc.pin.Version() != tc.wantVersion {
				t.Fatalf("number/version = %d/%d, want 500/%d", tc.pin.Number(), tc.pin.Version(), tc.wantVersion)
			}
		})
	}
}

func TestStatic_PreservesBlockCoordinatePreservingVersion(t *testing.T) {
	ev := outbound.BlockEvent{ChainID: 1, BlockNumber: 23145001, Version: 2, BlockHash: testHashHex}
	pin, err := outbound.PinForEvent(ev)
	if err != nil {
		t.Fatalf("PinForEvent: %v", err)
	}

	static := pin.Static()

	if static.Mode() != outbound.PinStatic {
		t.Fatalf("mode = %d, want PinStatic", static.Mode())
	}
	if static.Number() != 23145001 || static.Version() != 2 {
		t.Fatalf("number/version = %d/%d, want 23145001/2 (archive keys must match the context-stamped behavior)", static.Number(), static.Version())
	}
	if _, ok := static.Hash(); ok {
		t.Fatal("static pin must not expose a hash")
	}
}

func TestBlockPin_ZeroValueIsInvalid(t *testing.T) {
	var pin outbound.BlockPin
	if !pin.IsZero() {
		t.Fatal("zero-value pin must report IsZero")
	}
}
