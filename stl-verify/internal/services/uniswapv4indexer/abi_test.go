package uniswapv4indexer

import (
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

func poolManagerABIForTest(t *testing.T) *abi.ABI {
	t.Helper()
	a, err := PoolManagerABI()
	if err != nil {
		t.Fatalf("loading PoolManager ABI: %v", err)
	}
	return a
}

// TestPoolManagerABI_EventTopicHashes pins every event signature to the topic0
// captured verbatim from real mainnet PoolManager logs. topic0 hashes the
// signature only, so no assertion here can catch a wrong indexed flag; the
// verbatim-log decode fixtures in event_decode_test.go pin that half, because a
// log decoded against the wrong flags yields the wrong fields.
func TestPoolManagerABI_EventTopicHashes(t *testing.T) {
	a := poolManagerABIForTest(t)

	tests := []struct {
		event string
		want  string
	}{
		{event: "Initialize", want: "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"},
		{event: "ModifyLiquidity", want: "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"},
		{event: "Swap", want: "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"},
		{event: "Donate", want: "0x29ef05caaff9404b7cb6d1c0e9bbae9eaa7ab2541feba1a9c4248594c08156cb"},
		{event: "ProtocolFeeUpdated", want: "0xe9c42593e71f84403b84352cd168d693e2c9fcd1fdbcc3feb21d92b43e6696f9"},
		{event: "ProtocolFeeControllerUpdated", want: "0xb4bd8ef53df690b9943d3318996006dbb82a25f54719d8c8035b516a2a5b8acc"},
		{event: "Transfer", want: "0x1b3d7edb2e9c0b0e7c525b20aaaef0f5940d2ed71663c7d39266ecafac728859"},
		{event: "Approval", want: "0xb3fd5071835887567a0671151121894ddccc2842f1d10bedad13e0d17cace9a7"},
		{event: "OperatorSet", want: "0xceb576d9f15e4e200fdb5096d64d5dfd667e16def20c1eefd14256d8e3faa267"},
		{event: "OwnershipTransferred", want: "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0"},
	}

	for _, tt := range tests {
		t.Run(tt.event, func(t *testing.T) {
			ev, ok := a.Events[tt.event]
			if !ok {
				t.Fatalf("event %s missing from the PoolManager ABI", tt.event)
			}
			if got := ev.ID; got != common.HexToHash(tt.want) {
				t.Errorf("%s topic0 = %s, want %s (signature %q)", tt.event, got, tt.want, ev.Sig)
			}
		})
	}

	if len(a.Events) != len(tests) {
		t.Errorf("PoolManager ABI has %d events, want %d", len(a.Events), len(tests))
	}
}
