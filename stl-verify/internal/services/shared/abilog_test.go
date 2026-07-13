package shared

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ============================================================================
// LogBelongsTo
// ============================================================================

func TestLogBelongsTo(t *testing.T) {
	pool := common.HexToAddress("0x1111111111111111111111111111111111111111")
	lpToken := common.HexToAddress("0x2222222222222222222222222222222222222222")
	other := common.HexToAddress("0x3333333333333333333333333333333333333333")

	tests := []struct {
		name  string
		addr  common.Address
		watch []common.Address
		want  bool
	}{
		{
			name:  "matches sole watched address",
			addr:  pool,
			watch: []common.Address{pool},
			want:  true,
		},
		{
			name:  "matches second watched address",
			addr:  lpToken,
			watch: []common.Address{pool, lpToken},
			want:  true,
		},
		{
			name:  "no match",
			addr:  other,
			watch: []common.Address{pool, lpToken},
			want:  false,
		},
		{
			name:  "no watched addresses",
			addr:  pool,
			watch: nil,
			want:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LogBelongsTo(tc.addr, tc.watch...); got != tc.want {
				t.Errorf("LogBelongsTo(%s, %v) = %v, want %v", tc.addr, tc.watch, got, tc.want)
			}
		})
	}
}

// ============================================================================
// DecodeLog
// ============================================================================

func transferEvent(t *testing.T) abi.Event {
	t.Helper()
	a, err := abi.JSON(strings.NewReader(`[
		{"type":"event","name":"Transfer","inputs":[
			{"name":"from","type":"address","indexed":true},
			{"name":"to","type":"address","indexed":true},
			{"name":"value","type":"uint256","indexed":false}
		]}
	]`))
	if err != nil {
		t.Fatalf("abi.JSON: %v", err)
	}
	return a.Events["Transfer"]
}

func TestDecodeLog(t *testing.T) {
	ev := transferEvent(t)
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")

	t.Run("extracts indexed and non-indexed fields", func(t *testing.T) {
		nonIndexed := abi.Arguments{ev.Inputs[2]}
		data, err := nonIndexed.Pack(big.NewInt(123456))
		if err != nil {
			t.Fatalf("packing data: %v", err)
		}
		log := Log{
			Topics: []string{
				ev.ID.Hex(),
				common.BytesToHash(from.Bytes()).Hex(),
				common.BytesToHash(to.Bytes()).Hex(),
			},
			Data: "0x" + common.Bytes2Hex(data),
		}

		got, err := DecodeLog(ev, log)
		if err != nil {
			t.Fatalf("DecodeLog: %v", err)
		}
		if got["from"].(common.Address) != from {
			t.Errorf("from = %v, want %v", got["from"], from)
		}
		if got["to"].(common.Address) != to {
			t.Errorf("to = %v, want %v", got["to"], to)
		}
		if got["value"].(*big.Int).Cmp(big.NewInt(123456)) != 0 {
			t.Errorf("value = %v, want 123456", got["value"])
		}
	})

	t.Run("malformed indexed topic errors", func(t *testing.T) {
		log := Log{
			Topics: []string{ev.ID.Hex(), common.BytesToHash(from.Bytes()).Hex()}, // missing 'to' topic
			Data:   "0x",
		}
		if _, err := DecodeLog(ev, log); err == nil {
			t.Fatal("expected error when an indexed topic is missing")
		}
	})

	t.Run("no data for non-indexed args is not decoded", func(t *testing.T) {
		log := Log{
			Topics: []string{
				ev.ID.Hex(),
				common.BytesToHash(from.Bytes()).Hex(),
				common.BytesToHash(to.Bytes()).Hex(),
			},
			Data: "0x",
		}
		got, err := DecodeLog(ev, log)
		if err != nil {
			t.Fatalf("DecodeLog: %v", err)
		}
		if _, ok := got["value"]; ok {
			t.Errorf("value should be absent when Data is empty, got %v", got["value"])
		}
	})
}

// ============================================================================
// GetAddrField / GetBigIntField / GetBigIntSliceField
// ============================================================================

func TestGetAddrField(t *testing.T) {
	addr := common.HexToAddress("0x1111111111111111111111111111111111111111")

	t.Run("present and correct type", func(t *testing.T) {
		got, err := GetAddrField(map[string]any{"a": addr}, "a")
		if err != nil {
			t.Fatalf("GetAddrField: %v", err)
		}
		if got != addr {
			t.Errorf("got %s, want %s", got, addr)
		}
	})

	t.Run("missing field errors", func(t *testing.T) {
		if _, err := GetAddrField(map[string]any{}, "a"); err == nil {
			t.Fatal("expected error for missing field")
		}
	})

	t.Run("wrong type errors", func(t *testing.T) {
		if _, err := GetAddrField(map[string]any{"a": "not-an-address"}, "a"); err == nil {
			t.Fatal("expected error for wrong type")
		}
	})
}

func TestGetBigIntField(t *testing.T) {
	v := big.NewInt(42)

	t.Run("present and correct type", func(t *testing.T) {
		got, err := GetBigIntField(map[string]any{"v": v}, "v")
		if err != nil {
			t.Fatalf("GetBigIntField: %v", err)
		}
		if got.Cmp(v) != 0 {
			t.Errorf("got %s, want %s", got, v)
		}
	})

	t.Run("missing field errors", func(t *testing.T) {
		if _, err := GetBigIntField(map[string]any{}, "v"); err == nil {
			t.Fatal("expected error for missing field")
		}
	})

	t.Run("wrong type errors", func(t *testing.T) {
		if _, err := GetBigIntField(map[string]any{"v": "42"}, "v"); err == nil {
			t.Fatal("expected error for wrong type")
		}
	})
}

func TestGetBigIntSliceField(t *testing.T) {
	vs := []*big.Int{big.NewInt(1), big.NewInt(2)}

	t.Run("present and correct type", func(t *testing.T) {
		got, err := GetBigIntSliceField(map[string]any{"v": vs}, "v")
		if err != nil {
			t.Fatalf("GetBigIntSliceField: %v", err)
		}
		if len(got) != 2 || got[0].Cmp(big.NewInt(1)) != 0 || got[1].Cmp(big.NewInt(2)) != 0 {
			t.Errorf("got %v, want %v", got, vs)
		}
	})

	t.Run("missing field errors", func(t *testing.T) {
		if _, err := GetBigIntSliceField(map[string]any{}, "v"); err == nil {
			t.Fatal("expected error for missing field")
		}
	})

	t.Run("wrong type errors", func(t *testing.T) {
		if _, err := GetBigIntSliceField(map[string]any{"v": 42}, "v"); err == nil {
			t.Fatal("expected error for wrong type")
		}
	})
}

// ============================================================================
// ParseHexUint
// ============================================================================

func TestParseHexUint(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    uint
		wantErr bool
	}{
		{name: "valid hex", in: "0x5", want: 5},
		{name: "valid hex multi-digit", in: "0x1a", want: 26},
		{name: "missing 0x prefix errors", in: "5", wantErr: true},
		{name: "empty after prefix errors", in: "0x", wantErr: true},
		{name: "invalid hex digits errors", in: "0xzz", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseHexUint(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// ============================================================================
// UnpackSingleUint / UnpackUintArray / OptionalUintResult
// ============================================================================

func TestUnpackSingleUint(t *testing.T) {
	u256T, err := abi.NewType("uint256", "", nil)
	if err != nil {
		t.Fatalf("uint256 type: %v", err)
	}
	args := abi.Arguments{{Type: u256T}}
	okData, err := args.Pack(big.NewInt(99))
	if err != nil {
		t.Fatalf("packing uint256: %v", err)
	}

	t.Run("success returns value", func(t *testing.T) {
		got, err := UnpackSingleUint(outbound.Result{Success: true, ReturnData: okData})
		if err != nil {
			t.Fatalf("UnpackSingleUint: %v", err)
		}
		if got.Cmp(big.NewInt(99)) != 0 {
			t.Errorf("got %s, want 99", got)
		}
	})

	t.Run("revert errors", func(t *testing.T) {
		if _, err := UnpackSingleUint(outbound.Result{Success: false}); err == nil {
			t.Fatal("expected error on reverted call")
		}
	})

	t.Run("undecodable payload errors", func(t *testing.T) {
		if _, err := UnpackSingleUint(outbound.Result{Success: true, ReturnData: []byte{0x01}}); err == nil {
			t.Fatal("expected error on undecodable payload")
		}
	})
}

func TestUnpackUintArray(t *testing.T) {
	arrT, err := abi.NewType("uint256[3]", "", nil)
	if err != nil {
		t.Fatalf("uint256[3] type: %v", err)
	}
	args := abi.Arguments{{Type: arrT}}
	okData, err := args.Pack([3]*big.Int{big.NewInt(1), big.NewInt(2), big.NewInt(3)})
	if err != nil {
		t.Fatalf("packing uint256[3]: %v", err)
	}

	t.Run("success returns values", func(t *testing.T) {
		got, err := UnpackUintArray(outbound.Result{Success: true, ReturnData: okData}, 3)
		if err != nil {
			t.Fatalf("UnpackUintArray: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("len = %d, want 3", len(got))
		}
		for i, want := range []int64{1, 2, 3} {
			if got[i].Cmp(big.NewInt(want)) != 0 {
				t.Errorf("got[%d] = %s, want %d", i, got[i], want)
			}
		}
	})

	t.Run("revert errors", func(t *testing.T) {
		if _, err := UnpackUintArray(outbound.Result{Success: false}, 3); err == nil {
			t.Fatal("expected error on reverted call")
		}
	})

	t.Run("undecodable payload errors", func(t *testing.T) {
		if _, err := UnpackUintArray(outbound.Result{Success: true, ReturnData: []byte{0x01}}, 3); err == nil {
			t.Fatal("expected error on undecodable payload")
		}
	})
}

func TestOptionalUintResult(t *testing.T) {
	a, err := abi.JSON(strings.NewReader(`[
		{"type":"function","name":"v","outputs":[{"type":"uint256"}],"stateMutability":"view"}
	]`))
	if err != nil {
		t.Fatalf("abi.JSON: %v", err)
	}
	okData, err := a.Methods["v"].Outputs.Pack(big.NewInt(7))
	if err != nil {
		t.Fatalf("pack uint: %v", err)
	}
	target := common.HexToAddress("0x1111111111111111111111111111111111111111")

	t.Run("success returns value", func(t *testing.T) {
		got, err := OptionalUintResult(&a, "v", outbound.Result{Success: true, ReturnData: okData}, target, 100)
		if err != nil {
			t.Fatalf("OptionalUintResult: %v", err)
		}
		if got.Cmp(big.NewInt(7)) != 0 {
			t.Errorf("got %s, want 7", got)
		}
	})

	t.Run("revert is an error, never a silent nil", func(t *testing.T) {
		got, err := OptionalUintResult(&a, "v", outbound.Result{Success: false}, target, 100)
		if err == nil {
			t.Fatal("expected error on reverted AllowFailure result, got nil error (would silently swallow the revert)")
		}
		if got != nil {
			t.Errorf("expected nil value alongside error, got %v", got)
		}
	})
}
