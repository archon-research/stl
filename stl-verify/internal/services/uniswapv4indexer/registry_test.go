package uniswapv4indexer

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Mainnet ETH/wstETH 0.01% pool: currency0 is native ETH (address(0)),
// tickSpacing 1, no hooks. Its PoolId is the keccak of that exact key, so the
// pair doubles as the positive fixture for ValidatePoolKeys.
const (
	ethWstethPoolID = "0x1d5b2949ece8754c2d736991c62c5162bd144f497b2212182401b9bae77e2d76"
	wstethAddress   = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
	// Same pair and tickSpacing at 0.005% behind the Bunni hook.
	hookedPoolID = "0x904e8ad11c6f8abb44ea77c507355900e7f9d2907ab0a29353cb1ef0f06b0852"
	hookAddress  = "0x4440854B2d02C57A0Dc5c58b7A884562D875c0c4"
)

// registeredPool builds a valid RegisteredPool whose PoolIDHash really is the
// keccak of its key, with the varying fields as arguments.
func registeredPool(id int64, poolIDHash string, fee int, hooks common.Address) RegisteredPool {
	return RegisteredPool{
		ID:                id,
		PoolManager:       common.HexToAddress("0x000000000004444c5dc75cB358380D2e3dE08A90"),
		StateView:         common.HexToAddress("0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"),
		PoolIDHash:        common.HexToHash(poolIDHash),
		Currency0:         common.Address{},
		Currency1:         common.HexToAddress(wstethAddress),
		Currency0Decimals: 18,
		Currency1Decimals: 18,
		Fee:               fee,
		TickSpacing:       1,
		Hooks:             hooks,
		DeployBlock:       21743144,
		SnapshotSupported: true,
	}
}

func TestRegisteredPoolsFromRows(t *testing.T) {
	rows := []outbound.UniswapV4PoolRow{
		{
			ID:                1,
			ProtocolID:        7,
			PoolManager:       common.HexToAddress("0x000000000004444c5dc75cB358380D2e3dE08A90"),
			StateView:         common.HexToAddress("0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"),
			PoolIDHash:        common.HexToHash(ethWstethPoolID),
			Currency0:         common.Address{},
			Currency1:         common.HexToAddress(wstethAddress),
			Currency0Decimals: 18,
			Currency1Decimals: 18,
			Fee:               100,
			TickSpacing:       1,
			DeployBlock:       21743144,
		},
		{
			ID:                2,
			ProtocolID:        7,
			PoolManager:       common.HexToAddress("0x000000000004444c5dc75cB358380D2e3dE08A90"),
			StateView:         common.HexToAddress("0x7fFE42C4a5DEeA5b0feC41C94C136Cf115597227"),
			PoolIDHash:        common.HexToHash(hookedPoolID),
			Currency0:         common.Address{},
			Currency1:         common.HexToAddress(wstethAddress),
			Currency0Decimals: 18,
			Currency1Decimals: 18,
			Fee:               50,
			TickSpacing:       1,
			Hooks:             common.HexToAddress(hookAddress),
			DeployBlock:       23326185,
		},
	}

	got := RegisteredPoolsFromRows(rows)

	if len(got) != len(rows) {
		t.Fatalf("got %d pools, want %d", len(got), len(rows))
	}
	for i, row := range rows {
		want := RegisteredPool{
			ID:                row.ID,
			PoolManager:       row.PoolManager,
			StateView:         row.StateView,
			PoolIDHash:        row.PoolIDHash,
			Currency0:         row.Currency0,
			Currency1:         row.Currency1,
			Currency0Decimals: row.Currency0Decimals,
			Currency1Decimals: row.Currency1Decimals,
			Fee:               row.Fee,
			TickSpacing:       row.TickSpacing,
			Hooks:             row.Hooks,
			DeployBlock:       row.DeployBlock,
			SnapshotSupported: row.SnapshotSupported,
		}
		if got[i] != want {
			t.Errorf("pool %d = %+v, want %+v", i, got[i], want)
		}
	}
}

func TestRegisteredPoolsFromRows_Empty(t *testing.T) {
	if got := RegisteredPoolsFromRows(nil); len(got) != 0 {
		t.Errorf("got %d pools, want 0 for nil input", len(got))
	}
}

func TestRegisteredPool_SnapshotPoolAccessors(t *testing.T) {
	pool := registeredPool(42, ethWstethPoolID, 100, common.Address{})
	if got := pool.PoolID(); got != 42 {
		t.Errorf("PoolID() = %d, want 42 (the surrogate registry key, not the on-chain hash)", got)
	}
	if got := pool.DeployBlockNum(); got != 21743144 {
		t.Errorf("DeployBlockNum() = %d, want 21743144", got)
	}
}

func TestValidatePoolKeys_AcceptsRealPools(t *testing.T) {
	pools := []RegisteredPool{
		registeredPool(1, ethWstethPoolID, 100, common.Address{}),
		registeredPool(2, hookedPoolID, 50, common.HexToAddress(hookAddress)),
	}
	if err := ValidatePoolKeys(pools); err != nil {
		t.Fatalf("ValidatePoolKeys: %v", err)
	}
}

func TestValidatePoolKeys_EmptyIsValid(t *testing.T) {
	if err := ValidatePoolKeys(nil); err != nil {
		t.Errorf("ValidatePoolKeys(nil) = %v, want nil", err)
	}
}

// TestValidatePoolKeys_RejectsKeyMismatch covers each key component: any
// registry field that disagrees with the stored PoolId makes every log lookup
// for that pool miss silently, so boot must fail instead.
func TestValidatePoolKeys_RejectsKeyMismatch(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(p RegisteredPool) RegisteredPool
	}{
		{name: "wrong fee", mutate: func(p RegisteredPool) RegisteredPool { p.Fee = 3000; return p }},
		{name: "wrong tick spacing", mutate: func(p RegisteredPool) RegisteredPool { p.TickSpacing = 60; return p }},
		{name: "wrong currency0", mutate: func(p RegisteredPool) RegisteredPool {
			p.Currency0 = common.HexToAddress("0x1111111111111111111111111111111111111111")
			return p
		}},
		{name: "wrong currency1", mutate: func(p RegisteredPool) RegisteredPool {
			p.Currency1 = common.HexToAddress("0x2222222222222222222222222222222222222222")
			return p
		}},
		{name: "wrong hooks", mutate: func(p RegisteredPool) RegisteredPool {
			p.Hooks = common.HexToAddress(hookAddress)
			return p
		}},
		{name: "wrong pool id hash", mutate: func(p RegisteredPool) RegisteredPool {
			p.PoolIDHash = common.HexToHash("0xdead")
			return p
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := tt.mutate(registeredPool(9, ethWstethPoolID, 100, common.Address{}))
			err := ValidatePoolKeys([]RegisteredPool{pool})
			if err == nil {
				t.Fatalf("ValidatePoolKeys: want error for %s, got nil", tt.name)
			}
			if !strings.Contains(err.Error(), "9") {
				t.Errorf("error %q does not name pool 9", err)
			}
		})
	}
}

func TestValidatePoolKeys_RejectsDuplicatePoolIDHash(t *testing.T) {
	first := registeredPool(1, ethWstethPoolID, 100, common.Address{})
	second := first
	second.ID = 2

	err := ValidatePoolKeys([]RegisteredPool{first, second})
	if err == nil {
		t.Fatal("ValidatePoolKeys: want error for two pools sharing a PoolId, got nil")
	}
	if !strings.Contains(err.Error(), ethWstethPoolID) {
		t.Errorf("error %q does not name the duplicated PoolId", err)
	}
}

// TestSnapshottablePools_KeepsOnlySupportedPools pins the curated gate that
// replaced the boot refusal: an excluded pool stays in the registry (its events
// still index) and only drops out of the snapshot schedule.
func TestSnapshottablePools_KeepsOnlySupportedPools(t *testing.T) {
	supported := registeredPool(1, ethWstethPoolID, 100, common.Address{})
	excluded := registeredPool(2, hookedPoolID, 50, common.HexToAddress(hookAddress))
	excluded.SnapshotSupported = false

	got := SnapshottablePools([]RegisteredPool{supported, excluded})

	if len(got) != 1 || got[0].ID != supported.ID {
		t.Errorf("SnapshottablePools = %+v, want only pool %d", got, supported.ID)
	}
}

// TestValidatePoolKeys_AcceptsDynamicFeePool pins that the fee sentinel is no
// longer a boot refusal: the fee is hashed into the PoolId, so a registered
// dynamic-fee pool has no sanctioned repair and must not crash the worker.
func TestValidatePoolKeys_AcceptsDynamicFeePool(t *testing.T) {
	pool := registeredPool(4, ethWstethPoolID, DynamicFeeFlag, common.Address{})
	hash, err := computePoolID(pool)
	if err != nil {
		t.Fatalf("computePoolID: %v", err)
	}
	pool.PoolIDHash = hash
	pool.SnapshotSupported = false

	if err := ValidatePoolKeys([]RegisteredPool{pool}); err != nil {
		t.Fatalf("ValidatePoolKeys with a dynamic-fee pool: %v", err)
	}
}

// TestValidatePoolKeys_RejectsFeeOutsideUint24 keeps the recompute from
// silently truncating: abi.Pack would reject an out-of-range uint24, and that
// failure must surface as a registry error rather than a panic.
func TestValidatePoolKeys_RejectsFeeOutsideUint24(t *testing.T) {
	pool := registeredPool(3, ethWstethPoolID, 1<<24, common.Address{})
	if err := ValidatePoolKeys([]RegisteredPool{pool}); err == nil {
		t.Fatal("ValidatePoolKeys: want error for a fee outside uint24, got nil")
	}
}
