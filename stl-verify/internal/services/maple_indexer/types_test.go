package maple_indexer

import (
	"testing"
)

func TestMapleSyrupDeployBlock(t *testing.T) {
	tests := []struct {
		name      string
		chainID   int64
		wantBlock int64
		wantErr   bool
	}{
		{"ethereum mainnet", 1, 20231245, false},
		{"unsupported chain", 137, 0, true},
		{"zero chain id", 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MapleSyrupDeployBlock(tt.chainID)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err=%v, wantErr=%v", err, tt.wantErr)
			}
			if got != tt.wantBlock {
				t.Fatalf("got %d, want %d", got, tt.wantBlock)
			}
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	c := ConfigDefaults()
	if c.MaxMessages == 0 {
		t.Fatal("expected default MaxMessages > 0")
	}
	if c.PollInterval == 0 {
		t.Fatal("expected default PollInterval > 0")
	}
	if c.Logger == nil {
		t.Fatal("expected default Logger to be non-nil")
	}
}

func TestNewTelemetry_NilSafeOperations(t *testing.T) {
	var tel *Telemetry
	// All recorder methods must be no-ops on a nil receiver — the service
	// passes the telemetry struct around as a value that may not be wired
	// up in unit tests.
	tel.RecordBlockProcessed(nil, 0, nil)
	tel.RecordVaultStateWrite(nil)
	tel.RecordPositionWrites(nil, 5)
	tel.RecordRPCCall(nil, "x", 0, nil)
	tel.RecordError(nil, "op", nil)
}

func TestNewTelemetry_CreatesAllInstruments(t *testing.T) {
	tel, err := NewTelemetry()
	if err != nil {
		t.Fatalf("NewTelemetry: %v", err)
	}
	if tel.tracer == nil || tel.meter == nil {
		t.Fatal("tracer/meter not initialised")
	}
	if tel.blocksProcessed == nil || tel.vaultStateWrites == nil ||
		tel.positionWrites == nil || tel.rpcCallsTotal == nil || tel.errorsTotal == nil {
		t.Fatal("counters not initialised")
	}
	if tel.blockDuration == nil || tel.receiptDuration == nil || tel.rpcDuration == nil {
		t.Fatal("histograms not initialised")
	}
}
