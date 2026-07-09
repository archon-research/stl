package statereader_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/statereader"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var (
	testHash  = common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ab")
	testEvent = outbound.BlockEvent{ChainID: 1, BlockNumber: 100, Version: 1, BlockHash: testHash.Hex()}
	oneCall   = []outbound.Call{{Target: common.HexToAddress("0x1"), CallData: []byte{0x01}}}
	oneResult = []outbound.Result{{Success: true, ReturnData: []byte{0x02}}}
)

func TestRead_ReorgSafePin_DispatchesToExecuteAtHash(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	var gotHash common.Hash
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, h common.Hash) ([]outbound.Result, error) {
		gotHash = h
		return oneResult, nil
	}
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		t.Fatal("reorg-safe pin must never reach Execute")
		return nil, nil
	}
	pin, err := outbound.PinForEvent(testEvent)
	if err != nil {
		t.Fatalf("PinForEvent: %v", err)
	}

	results, err := statereader.New(mc).Read(context.Background(), pin, oneCall)

	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if gotHash != testHash {
		t.Fatalf("hash = %s, want %s", gotHash.Hex(), testHash.Hex())
	}
	if len(results) != 1 || !results[0].Success {
		t.Fatalf("results = %+v, want the mocked result", results)
	}
}

func TestRead_NumberPinnedModes_DispatchToExecute(t *testing.T) {
	cases := []struct {
		name string
		pin  outbound.BlockPin
	}{
		{"settled", outbound.PinForSettledBlock(100, 1)},
		{"static", outbound.PinForStaticRead(100)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mc := testutil.NewMockMulticaller()
			var gotNumber *big.Int
			mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, bn *big.Int) ([]outbound.Result, error) {
				gotNumber = bn
				return oneResult, nil
			}
			mc.ExecuteAtHashFn = func(_ context.Context, _ []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
				t.Fatal("number-pinned pin must never reach ExecuteAtHash")
				return nil, nil
			}

			if _, err := statereader.New(mc).Read(context.Background(), tc.pin, oneCall); err != nil {
				t.Fatalf("Read: %v", err)
			}
			if gotNumber == nil || gotNumber.Int64() != 100 {
				t.Fatalf("blockNumber = %v, want 100", gotNumber)
			}
		})
	}
}

func TestRead_ZeroPin_FailsWithoutRPC(t *testing.T) {
	mc := testutil.NewMockMulticaller() // no Fns set: any dispatch would error differently

	_, err := statereader.New(mc).Read(context.Background(), outbound.BlockPin{}, oneCall)

	if err == nil {
		t.Fatal("expected error for zero pin")
	}
	if mc.CallCount != 0 {
		t.Fatalf("CallCount = %d, want 0 (no RPC on invalid pin)", mc.CallCount)
	}
}

func TestRead_ResultCountMismatch_IsAnError(t *testing.T) {
	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		return nil, nil // 0 results for 1 call
	}

	_, err := statereader.New(mc).Read(context.Background(), outbound.PinForStaticRead(100), oneCall)

	if err == nil {
		t.Fatal("expected count-mismatch error")
	}
}
