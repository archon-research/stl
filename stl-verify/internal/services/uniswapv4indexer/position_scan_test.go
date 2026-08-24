package uniswapv4indexer

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const (
	scanOwnerA = "0x1111111111111111111111111111111111111111"
	scanOwnerB = "0x2222222222222222222222222222222222222222"
	scanSaltA  = "0x00000000000000000000000000000000000000000000000000000000000000aa"
	scanSaltB  = "0x00000000000000000000000000000000000000000000000000000000000000bb"
)

// modifyLiquidityLog builds one ModifyLiquidity log for pool, owner and tick
// range. liquidityDelta is irrelevant to key discovery, so it stays fixed.
func modifyLiquidityLog(t *testing.T, poolIDHash, owner string, tickLower, tickUpper int64, salt string) shared.Log {
	t.Helper()
	return buildLog(t, "ModifyLiquidity",
		[]common.Hash{common.HexToHash(poolIDHash), addrTopic(common.HexToAddress(owner))},
		big.NewInt(tickLower), big.NewInt(tickUpper), big.NewInt(1000), common.HexToHash(salt))
}

func TestModifyLiquidityTopic0_MatchesTheABIEventID(t *testing.T) {
	got, err := ModifyLiquidityTopic0()
	if err != nil {
		t.Fatalf("ModifyLiquidityTopic0: %v", err)
	}
	a := poolManagerABIForTest(t)
	if want := a.Events["ModifyLiquidity"].ID; got != want {
		t.Errorf("topic0 = %s, want %s", got, want)
	}
}

func TestPositionKeysFromLogs_GroupsDeduplicatedKeysByPool(t *testing.T) {
	poolA := decodeTestPool(7, modifyFixturePoolID)
	poolB := decodeTestPool(9, swapFixturePoolID)
	pools := poolsByIDOf(poolA, poolB)

	logs := []shared.Log{
		modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltA),
		// Same key touched again in a later block: one row, not two.
		modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltA),
		modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerB, -100, 200, scanSaltA),
		modifyLiquidityLog(t, swapFixturePoolID, scanOwnerA, -60, 60, scanSaltB),
	}

	got, err := PositionKeysFromLogs(logs, pools, poolManagerAddress())
	if err != nil {
		t.Fatalf("PositionKeysFromLogs: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("pools with keys = %d, want 2", len(got))
	}
	if len(got[poolA.ID]) != 2 {
		t.Errorf("pool %d keys = %d, want 2", poolA.ID, len(got[poolA.ID]))
	}
	if len(got[poolB.ID]) != 1 {
		t.Errorf("pool %d keys = %d, want 1", poolB.ID, len(got[poolB.ID]))
	}

	want := entity.UniswapV4PositionKey{
		Owner:     common.HexToAddress(scanOwnerA),
		TickLower: -60,
		TickUpper: 60,
		Salt:      common.HexToHash(scanSaltB),
	}
	if got[poolB.ID][0] != want {
		t.Errorf("pool %d key = %+v, want %+v", poolB.ID, got[poolB.ID][0], want)
	}
}

func TestPositionKeysFromLogs_ReturnsKeysInCompareOrder(t *testing.T) {
	pool := decodeTestPool(7, modifyFixturePoolID)
	logs := []shared.Log{
		modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerB, -100, 200, scanSaltA),
		modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltB),
		modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltA),
	}

	got, err := PositionKeysFromLogs(logs, poolsByIDOf(pool), poolManagerAddress())
	if err != nil {
		t.Fatalf("PositionKeysFromLogs: %v", err)
	}

	keys := got[pool.ID]
	if len(keys) != 3 {
		t.Fatalf("keys = %d, want 3", len(keys))
	}
	for i := 1; i < len(keys); i++ {
		if keys[i-1].Compare(keys[i]) >= 0 {
			t.Errorf("keys[%d] %+v is not before keys[%d] %+v", i-1, keys[i-1], i, keys[i])
		}
	}
}

func TestPositionKeysFromLogs_DropsUnregisteredPool(t *testing.T) {
	pool := decodeTestPool(7, modifyFixturePoolID)
	logs := []shared.Log{modifyLiquidityLog(t, donateFixturePoolID, scanOwnerA, -100, 200, scanSaltA)}

	got, err := PositionKeysFromLogs(logs, poolsByIDOf(pool), poolManagerAddress())
	if err != nil {
		t.Fatalf("PositionKeysFromLogs: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("keys = %v, want none: the log names a pool outside the registry", got)
	}
}

func TestPositionKeysFromLogs_RejectsLogFromAnotherEmitter(t *testing.T) {
	pool := decodeTestPool(7, modifyFixturePoolID)
	log := modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltA)
	log.Address = "0x000000000000000000000000000000000000dead"

	_, err := PositionKeysFromLogs([]shared.Log{log}, poolsByIDOf(pool), poolManagerAddress())
	if err == nil {
		t.Fatal("expected an error: a filtered scan must never see another contract's log")
	}
	if !strings.Contains(err.Error(), "0x000000000000000000000000000000000000dEaD") {
		t.Errorf("error = %v, want it to name the foreign emitter", err)
	}
}

func TestPositionKeysFromLogs_RejectsNonModifyLiquidityTopic0(t *testing.T) {
	pool := decodeTestPool(7, modifyFixturePoolID)
	log := modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltA)
	a := poolManagerABIForTest(t)
	log.Topics[0] = a.Events["Swap"].ID.Hex()

	if _, err := PositionKeysFromLogs([]shared.Log{log}, poolsByIDOf(pool), poolManagerAddress()); err == nil {
		t.Fatal("expected an error: the filter asked for ModifyLiquidity only")
	}
}

func TestPositionKeysFromLogs_RejectsMalformedHexWord(t *testing.T) {
	cases := []struct {
		name  string
		mutil func(*shared.Log)
	}{
		{"short topic", func(l *shared.Log) { l.Topics[1] = "0xdeadbeef" }},
		{"short transaction hash", func(l *shared.Log) { l.TransactionHash = "0xdeadbeef" }},
		{"non-hex address", func(l *shared.Log) { l.Address = "not-an-address" }},
		{"non-hex log index", func(l *shared.Log) { l.LogIndex = "zzz" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := decodeTestPool(7, modifyFixturePoolID)
			log := modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltA)
			tc.mutil(&log)

			if _, err := PositionKeysFromLogs([]shared.Log{log}, poolsByIDOf(pool), poolManagerAddress()); err == nil {
				t.Fatalf("expected an error for a %s", tc.name)
			}
		})
	}
}

func TestPositionKeysFromLogs_RejectsMissingPoolIDTopic(t *testing.T) {
	pool := decodeTestPool(7, modifyFixturePoolID)
	log := modifyLiquidityLog(t, modifyFixturePoolID, scanOwnerA, -100, 200, scanSaltA)
	log.Topics = log.Topics[:1]

	if _, err := PositionKeysFromLogs([]shared.Log{log}, poolsByIDOf(pool), poolManagerAddress()); err == nil {
		t.Fatal("expected an error for a log carrying no indexed pool id")
	}
}

func TestPositionKeysFromLogs_RejectsOutOfRangeTick(t *testing.T) {
	pool := decodeTestPool(7, modifyFixturePoolID)
	log := buildLog(t, "ModifyLiquidity",
		[]common.Hash{common.HexToHash(modifyFixturePoolID), addrTopic(common.HexToAddress(scanOwnerA))},
		big.NewInt(-900000), big.NewInt(900000), big.NewInt(1000), common.HexToHash(scanSaltA))

	if _, err := PositionKeysFromLogs([]shared.Log{log}, poolsByIDOf(pool), poolManagerAddress()); err == nil {
		t.Fatal("expected an error: a tick outside the V4 range cannot be packed into an int24 read")
	}
}

func TestPositionKeysFromLogs_EmptyInputYieldsNoPools(t *testing.T) {
	got, err := PositionKeysFromLogs(nil, poolsByIDOf(decodeTestPool(7, modifyFixturePoolID)), poolManagerAddress())
	if err != nil {
		t.Fatalf("PositionKeysFromLogs: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("keys = %v, want none", got)
	}
}
