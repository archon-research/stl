package testutil_test

import (
	"context"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

func TestStateReaderStub_RecordsEveryPin(t *testing.T) {
	stub := &testutil.StateReaderStub{
		ReadFn: func(_ context.Context, _ outbound.BlockPin, _ []outbound.Call) ([]outbound.Result, error) {
			return nil, nil
		},
	}

	_, _ = stub.Read(context.Background(), outbound.PinForSettledBlock(10, 1), nil)
	_, _ = stub.Read(context.Background(), outbound.PinForStaticRead(11), nil)

	pins := stub.Pins()
	if len(pins) != 2 || stub.CallCount() != 2 {
		t.Fatalf("pins/calls = %d/%d, want 2/2", len(pins), stub.CallCount())
	}
	if pins[0].Mode() != outbound.PinSettled || pins[1].Mode() != outbound.PinStatic {
		t.Fatalf("recorded modes = %d,%d, want PinSettled,PinStatic", pins[0].Mode(), pins[1].Mode())
	}
}

func TestStateReaderStub_UnstubbedReadErrors(t *testing.T) {
	stub := &testutil.StateReaderStub{}
	if _, err := stub.Read(context.Background(), outbound.PinForStaticRead(1), nil); err == nil {
		t.Fatal("expected error when ReadFn is unset")
	}
}
