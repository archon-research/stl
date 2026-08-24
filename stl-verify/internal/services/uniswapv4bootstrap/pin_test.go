package uniswapv4bootstrap

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func TestPinBlock_PinsFinalityDepthBelowHead(t *testing.T) {
	client := newFakeLogScanClient(1000, map[int64]*outbound.BlockHeader{936: header(936, pinHash)})

	pin, err := pinBlock(context.Background(), client, 1000-936, 0)
	if err != nil {
		t.Fatalf("pinBlock: %v", err)
	}
	if pin.number != 936 {
		t.Errorf("number = %d, want 936", pin.number)
	}
	if pin.hash != common.HexToHash(pinHash) {
		t.Errorf("hash = %s, want %s", pin.hash, pinHash)
	}
	if pin.ts.Unix() != pinTimestampUnix {
		t.Errorf("timestamp = %s (unix %d), want unix %d", pin.ts, pin.ts.Unix(), pinTimestampUnix)
	}
	if pin.ts.Location() != nil && pin.ts.Location().String() != "UTC" {
		t.Errorf("timestamp location = %s, want UTC", pin.ts.Location())
	}
}

func TestPinBlock_ExplicitOverrideSkipsTheHeadRead(t *testing.T) {
	client := newFakeLogScanClient(0, map[int64]*outbound.BlockHeader{500: header(500, pinHash)})
	client.HeadErr = errors.New("head must not be read when the pin is given")

	pin, err := pinBlock(context.Background(), client, 64, 500)
	if err != nil {
		t.Fatalf("pinBlock: %v", err)
	}
	if pin.number != 500 {
		t.Errorf("number = %d, want 500", pin.number)
	}
}

func TestPinBlock_FailsWhenHeadIsBelowTheFinalityDepth(t *testing.T) {
	client := newFakeLogScanClient(10, nil)

	_, err := pinBlock(context.Background(), client, 64, 0)
	if err == nil {
		t.Fatal("expected an error: there is no finality-safe block yet")
	}
	if !strings.Contains(err.Error(), "finality") {
		t.Errorf("error = %v, want it to explain the finality depth", err)
	}
}

func TestPinBlock_PropagatesReadFailures(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fakeLogScanClient)
	}{
		{"head read fails", func(c *fakeLogScanClient) { c.HeadErr = errors.New("boom") }},
		{"header read fails", func(c *fakeLogScanClient) { c.HeaderErr = errors.New("boom") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeLogScanClient(1000, map[int64]*outbound.BlockHeader{936: header(936, pinHash)})
			tt.mutate(client)

			if _, err := pinBlock(context.Background(), client, 64, 0); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestParsePinnedHeader_RejectsAnUnusableHeader(t *testing.T) {
	tests := []struct {
		name   string
		header *outbound.BlockHeader
		want   string
	}{
		{"nil header", nil, "no header"},
		{"short hash", &outbound.BlockHeader{Number: "0x3a8", Hash: "0xdeadbeef", Timestamp: pinTimestampHex}, "hash"},
		{"empty hash", &outbound.BlockHeader{Number: "0x3a8", Hash: "", Timestamp: pinTimestampHex}, "hash"},
		{"unparseable number", &outbound.BlockHeader{Number: "zzz", Hash: pinHash, Timestamp: pinTimestampHex}, "number"},
		{"number disagrees", &outbound.BlockHeader{Number: "0x1", Hash: pinHash, Timestamp: pinTimestampHex}, "number"},
		{"unparseable timestamp", &outbound.BlockHeader{Number: "0x3a8", Hash: pinHash, Timestamp: "zzz"}, "timestamp"},
		{"zero timestamp", &outbound.BlockHeader{Number: "0x3a8", Hash: pinHash, Timestamp: "0x0"}, "timestamp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parsePinnedHeader(936, tt.header)
			if err == nil {
				t.Fatalf("expected an error for %s", tt.name)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %v, want it to name %q", err, tt.want)
			}
		})
	}
}

func TestAssertPinStable_PassesWhenTheHashIsUnchanged(t *testing.T) {
	client := newFakeLogScanClient(1000, map[int64]*outbound.BlockHeader{936: header(936, pinHash)})
	pin := pinnedBlock{number: 936, hash: common.HexToHash(pinHash)}

	if err := assertPinStable(context.Background(), client, pin); err != nil {
		t.Fatalf("assertPinStable: %v", err)
	}
	if client.HeaderCalls != 1 {
		t.Errorf("header reads = %d, want 1", client.HeaderCalls)
	}
}

func TestAssertPinStable_FailsWhenTheHeightNowNamesAnotherBlock(t *testing.T) {
	client := newFakeLogScanClient(1000, map[int64]*outbound.BlockHeader{936: header(936, forkHash)})
	pin := pinnedBlock{number: 936, hash: common.HexToHash(pinHash)}

	err := assertPinStable(context.Background(), client, pin)
	if err == nil {
		t.Fatal("expected an error: the pinned height was reorged")
	}
	if !strings.Contains(err.Error(), forkHash) || !strings.Contains(err.Error(), pinHash) {
		t.Errorf("error = %v, want it to name both hashes", err)
	}
}

func TestAssertPinStable_PropagatesTheReReadFailure(t *testing.T) {
	client := newFakeLogScanClient(1000, nil)
	client.HeaderErr = errors.New("boom")

	if err := assertPinStable(context.Background(), client, pinnedBlock{number: 936}); err == nil {
		t.Fatal("expected an error")
	}
}
