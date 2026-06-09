package maple_indexer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

func newMapleVault(t *testing.T, addr common.Address) *entity.MapleVault {
	t.Helper()
	return newMapleVaultWithDecimals(t, addr, 6)
}

func newMapleVaultWithDecimals(t *testing.T, addr common.Address, decimals uint8) *entity.MapleVault {
	t.Helper()
	v, err := entity.NewMapleVault(
		1, 7, 9,
		addr.Bytes(),
		"Syrup USDC", "SyrupUSDC",
		1, 20231245,
		decimals,
	)
	if err != nil {
		t.Fatalf("NewMapleVault: %v", err)
	}
	return v
}

func TestVaultRegistry_LoadFromDB_PopulatesMap(t *testing.T) {
	addr := common.HexToAddress(syrupUSDCAddr)
	v := newMapleVault(t, addr)
	repo := newRepoStub(map[common.Address]*entity.MapleVault{addr: v}, nil)

	r := NewVaultRegistry(quietLogger())
	if err := r.LoadFromDB(context.Background(), repo, 1); err != nil {
		t.Fatalf("LoadFromDB: %v", err)
	}
	if r.Count() != 1 {
		t.Fatalf("Count=%d, want 1", r.Count())
	}
	if got := r.GetVault(addr); got == nil || got.ID != v.ID {
		t.Fatalf("GetVault returned wrong vault: %+v", got)
	}
}

func TestVaultRegistry_GetVault_NilForUnknown(t *testing.T) {
	repo := newRepoStub(map[common.Address]*entity.MapleVault{}, nil)
	r := NewVaultRegistry(quietLogger())
	if err := r.LoadFromDB(context.Background(), repo, 1); err != nil {
		t.Fatal(err)
	}
	if got := r.GetVault(common.HexToAddress(otherAddr)); got != nil {
		t.Fatalf("GetVault for unknown address should be nil, got %+v", got)
	}
}

func TestVaultRegistry_LoadFromDB_PropagatesError(t *testing.T) {
	wantErr := errors.New("db boom")
	repo := newRepoStub(nil, wantErr)
	r := NewVaultRegistry(quietLogger())
	err := r.LoadFromDB(context.Background(), repo, 1)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped wantErr, got %v", err)
	}
}

func TestVaultRegistry_LoadFromDB_NilMapHandled(t *testing.T) {
	repo := newRepoStub(nil, nil) // repo returns nil map without error
	r := NewVaultRegistry(quietLogger())
	if err := r.LoadFromDB(context.Background(), repo, 1); err != nil {
		t.Fatal(err)
	}
	if r.Count() != 0 {
		t.Fatalf("Count=%d, want 0", r.Count())
	}
}

func TestVaultRegistry_All_ReturnsCopy(t *testing.T) {
	addr := common.HexToAddress(syrupUSDCAddr)
	v := newMapleVault(t, addr)
	repo := newRepoStub(map[common.Address]*entity.MapleVault{addr: v}, nil)
	r := NewVaultRegistry(quietLogger())
	if err := r.LoadFromDB(context.Background(), repo, 1); err != nil {
		t.Fatal(err)
	}
	all := r.All()
	if len(all) != 1 {
		t.Fatalf("All() len=%d, want 1", len(all))
	}
	// Mutating the snapshot must not affect future calls.
	all[0] = nil
	all2 := r.All()
	if all2[0] == nil {
		t.Fatal("All() returned shared slice — must return a snapshot copy")
	}
}

func TestVaultRegistry_LoadFromDB_IsIdempotent(t *testing.T) {
	addr := common.HexToAddress(syrupUSDCAddr)
	v := newMapleVault(t, addr)
	repo := newRepoStub(map[common.Address]*entity.MapleVault{addr: v}, nil)
	r := NewVaultRegistry(quietLogger())
	for i := range 3 {
		if err := r.LoadFromDB(context.Background(), repo, 1); err != nil {
			t.Fatal(err)
		}
		if r.Count() != 1 {
			t.Fatalf("iter %d: Count=%d, want 1", i, r.Count())
		}
	}
}
