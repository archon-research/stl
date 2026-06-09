package archivingwire

import (
	"context"
	"math/big"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/common"
)

type stubMulticaller struct{ addr common.Address }

func (s stubMulticaller) Execute(context.Context, []outbound.Call, *big.Int) ([]outbound.Result, error) {
	return nil, nil
}
func (s stubMulticaller) Address() common.Address { return s.addr }

func TestEnabled(t *testing.T) {
	tests := []struct {
		name   string
		envVal string
		want   bool
	}{
		{name: "true enables", envVal: "true", want: true},
		{name: "false disables", envVal: "false", want: false},
		{name: "unset disables", envVal: "", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ARCHIVE_SC_CALLS", tt.envVal)
			if got := Enabled(); got != tt.want {
				t.Fatalf("Enabled() = %v, want %v for env=%q", got, tt.want, tt.envVal)
			}
		})
	}
}

func TestNewS3WrapFromEnvRequiresBucket(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	t.Setenv("RAW_SC_BUCKET", "")
	_, _, err := NewS3WrapFromEnv(t.Context(), nil, 1, 47, "oracle-price")
	if err == nil {
		t.Fatal("expected error when RAW_SC_BUCKET is empty")
	}
}

// TestBootstrapDisabled verifies the identity wrap and no-op drain returned when
// archiving is off, so callers can wire them unconditionally.
func TestBootstrapDisabled(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "false")

	wrap, drain, err := Bootstrap(context.Background(), nil, 1, 47, "oracle-price")
	if err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if wrap == nil || drain == nil {
		t.Fatal("disabled Bootstrap must return non-nil wrap and drain")
	}

	mc := stubMulticaller{addr: common.HexToAddress("0xabc")}
	if got := wrap(mc); got != mc {
		t.Fatal("disabled wrap must return the multicaller unchanged")
	}
	drain() // must not panic
}

// TestBootstrapErrorWhenMisconfigured verifies that enabling archiving without a
// bucket surfaces a wrapped error rather than silently disabling.
func TestBootstrapErrorWhenMisconfigured(t *testing.T) {
	t.Setenv("ARCHIVE_SC_CALLS", "true")
	t.Setenv("RAW_SC_BUCKET", "")

	wrap, drain, err := Bootstrap(context.Background(), nil, 1, 47, "oracle-price")
	if err == nil {
		t.Fatal("expected error when RAW_SC_BUCKET is empty")
	}
	if wrap != nil || drain != nil {
		t.Fatal("error path must return nil wrap and drain")
	}
}
